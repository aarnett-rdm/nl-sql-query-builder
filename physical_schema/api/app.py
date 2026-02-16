from __future__ import annotations

import asyncio
import concurrent.futures
import datetime
import json
import logging
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, field_validator
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from tools.nl_to_spec import nl_to_spec
from tools.spec_executor import execute_spec
from tools.config import AppConfig
from tools.exceptions import (
    NLSQLError,
    AmbiguousDimensionError,
    ConfigError,
    DateFilterError,
    LLMBackendError,
    MetricResolutionError,
    OllamaError,
    SpecValidationError,
)
from tools.feedback_store import CorrectionRecord, FeedbackStore, VALID_TYPES
from tools.llm_adapter import LLMAdapter, build_llm_adapter


# ----------------------------
# Configuration (centralized)
# ----------------------------

config = AppConfig.from_env()


# ----------------------------
# Structured JSON logging
# ----------------------------

class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


_handler = logging.StreamHandler()
_handler.setFormatter(JSONFormatter())
logging.root.handlers = [_handler]
logging.root.setLevel(config.log_level)

logger = logging.getLogger("nl_sql_service")


# ----------------------------
# FastAPI app
# ----------------------------

app = FastAPI(title="NL-SQL Service", version="0.5")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy-initialized LLM adapter (set at startup)
_llm_adapter: Optional[LLMAdapter] = None

# Feedback store (set at startup)
_feedback_store: Optional[FeedbackStore] = None

# Thread pool for running synchronous LLM calls with timeout
_llm_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)


# ----------------------------
# Request logging middleware
# ----------------------------

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Assigns correlation IDs, logs request/response, skips /healthz noise."""

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id

        if request.url.path in ("/healthz", "/ready"):
            return await call_next(request)

        t0 = time.time()
        _log_json(
            "request_start",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        response = await call_next(request)

        elapsed_ms = int((time.time() - t0) * 1000)
        _log_json(
            "request_end",
            request_id=request_id,
            status_code=response.status_code,
            elapsed_ms=elapsed_ms,
        )
        response.headers["X-Request-ID"] = request_id
        return response


app.add_middleware(RequestLoggingMiddleware)


# ----------------------------
# Models
# ----------------------------

class QueryRequest(BaseModel):
    question: str = Field(
        min_length=1,
        max_length=4000,
        examples=[
            "show clicks by campaign last 7 days",
            "compare google vs microsoft clicks for the last 30 days",
            "show conversions and cost by account yesterday",
        ],
    )


class QueryResponse(BaseModel):
    request_id: str = Field(examples=["8b2c9e6f-8f1f-4b5a-9a7a-61d1f4cda3c7"])
    spec: Dict[str, Any]
    sql: Optional[str] = None
    clarifications: list = Field(default_factory=list)


_SPEC_REQUIRED_KEYS = {"grain", "platform", "metrics", "dimensions", "filters"}


class ContinueRequest(BaseModel):
    spec: Dict[str, Any] = Field(
        description="Prior spec returned from /query (client-owned, unmodified except for applying answers)."
    )
    answers: Dict[str, Any] = Field(
        default_factory=dict,
        description="Explicit answers keyed by dotted-path field name (no guessing).",
        examples=[
            {"platform": "google", "grain": "campaign_calendar"},
            {"filters.campaign_id": [123, 456, 789]},
            {"date_range.preset": "last_7_days"},
        ],
    )

    @field_validator("spec")
    @classmethod
    def spec_has_required_keys(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        missing = _SPEC_REQUIRED_KEYS - v.keys()
        if missing:
            raise ValueError(f"spec missing required keys: {sorted(missing)}")
        return v


class ErrorResponse(BaseModel):
    request_id: str
    error_type: str
    message: str
    details: Optional[Dict[str, Any]] = None


class FeedbackRequest(BaseModel):
    request_id: str = Field(description="Correlation ID from the original /query response")
    original_question: str = Field(min_length=1, max_length=4000)
    original_spec: Dict[str, Any]
    corrected_spec: Dict[str, Any]
    correction_type: str = Field(
        description="Category: metric_mismatch, dimension_wrong, platform_wrong, date_filter_wrong, filter_wrong, other"
    )
    notes: str = Field(default="", max_length=2000)

    @field_validator("correction_type")
    @classmethod
    def correction_type_valid(cls, v: str) -> str:
        if v not in VALID_TYPES:
            raise ValueError(f"correction_type must be one of {sorted(VALID_TYPES)}")
        return v

    @field_validator("corrected_spec")
    @classmethod
    def corrected_spec_has_required_keys(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        missing = _SPEC_REQUIRED_KEYS - v.keys()
        if missing:
            raise ValueError(f"corrected_spec missing required keys: {sorted(missing)}")
        return v


class FeedbackResponse(BaseModel):
    feedback_id: str
    status: str = "recorded"


# ----------------------------
# API Key stub (no enforcement)
# ----------------------------

api_key_header = APIKeyHeader(name=config.api_key_header, auto_error=False)


def get_api_key(api_key: Optional[str] = Depends(api_key_header)) -> Optional[str]:
    """Stub: accepts optional API key but does not enforce authentication."""
    return api_key


# ----------------------------
# OpenAPI examples
# ----------------------------

QUERY_REQUEST_EXAMPLE = {
    "question": "show clicks by campaign last 7 days"
}

QUERY_RESPONSE_SQL_EXAMPLE = {
    "request_id": "8b2c9e6f-8f1f-4b5a-9a7a-61d1f4cda3c7",
    "spec": {
        "metrics": ["clicks"],
        "dimensions": ["campaignname"],
        "date_range": {"preset": "last_7_days"},
        "grain": "campaign_calendar",
        "platform": "google",
    },
    "sql": "SELECT ...",
    "clarifications": [],
}

QUERY_RESPONSE_CLARIFICATIONS_EXAMPLE = {
    "request_id": "8b2c9e6f-8f1f-4b5a-9a7a-61d1f4cda3c7",
    "spec": {
        "metrics": ["clicks"],
        "dimensions": ["campaignname"],
        "clarifications": [
            {
                "field": "platform",
                "prompt": "Which platform?",
                "options": ["google", "microsoft"],
            }
        ],
    },
    "sql": None,
    "clarifications": [
        {
            "field": "platform",
            "prompt": "Which platform?",
            "options": ["google", "microsoft"],
        }
    ],
}

CONTINUE_REQUEST_EXAMPLE = {
    "spec": {
        "metrics": ["clicks"],
        "dimensions": ["campaignname"],
        "clarifications": [
            {"field": "platform", "prompt": "Which platform?", "options": ["google", "microsoft"]}
        ],
    },
    "answers": {"platform": "google"},
}

ERROR_400_EXAMPLE = {
    "request_id": "8b2c9e6f-8f1f-4b5a-9a7a-61d1f4cda3c7",
    "error_type": "MetricResolutionError",
    "message": "Unknown metric: 'cliks'",
    "details": {"metric_name": "cliks", "suggestions": ["clicks"]},
}


# ----------------------------
# Utilities
# ----------------------------

def _new_request_id() -> str:
    return str(uuid.uuid4())


def _get_request_id(request: Optional[Request] = None) -> str:
    """Extract correlation ID from request state, or generate one."""
    if request is not None:
        return getattr(request.state, "request_id", None) or _new_request_id()
    return _new_request_id()


def _log_json(event: str, request_id: str, **fields: Any) -> None:
    payload = {"event": event, "request_id": request_id, **fields}
    logger.info(json.dumps(payload, default=str))


def _set_by_path(obj: Dict[str, Any], dotted_path: str, value: Any) -> None:
    """Minimal spec patcher: assigns obj[a][b][c] = value for "a.b.c"."""
    parts = [p for p in dotted_path.split(".") if p]
    if not parts:
        raise SpecValidationError(
            "Empty answer field path", field=dotted_path
        )

    cur: Any = obj
    for p in parts[:-1]:
        if not isinstance(cur, dict):
            raise SpecValidationError(
                f"Path '{dotted_path}' traverses a non-dict at '{p}'",
                field=dotted_path,
            )
        if p not in cur or cur[p] is None:
            cur[p] = {}
        cur = cur[p]

    last = parts[-1]
    if not isinstance(cur, dict):
        raise SpecValidationError(
            f"Path '{dotted_path}' cannot assign into non-dict at '{last}'",
            field=dotted_path,
        )
    cur[last] = value


def _error_response(
    status_code: int,
    request_id: str,
    exc: Exception,
    details: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    """Build a consistent JSON error response."""
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(
            request_id=request_id,
            error_type=type(exc).__name__,
            message=str(exc),
            details=details,
        ).model_dump(),
    )


# ----------------------------
# Startup
# ----------------------------

@app.on_event("startup")
def _startup() -> None:
    global _llm_adapter, _feedback_store

    config.validate()

    if config.llm_enabled:
        try:
            _llm_adapter = build_llm_adapter(
                registry_path=config.metric_registry,
                physical_schema_path=config.physical_schema,
                retriever_chunks_dir=str(config.chunks_dir) if config.chunks_dir else None,
                ollama_url=config.ollama_url,
                ollama_model=config.ollama_model,
            )
            backend_ok = _llm_adapter.backend.is_available()
            _log_json(
                "startup_llm_init",
                request_id="startup",
                llm_enabled=True,
                backend_available=backend_ok,
                model=_llm_adapter.backend.model_name,
                provider=config.llm_provider,
            )
        except Exception as e:
            _llm_adapter = None
            logger.warning("LLM adapter init failed: %s", e, exc_info=True)
            _log_json("startup_llm_failed", request_id="startup", error=str(e))
    else:
        _log_json("startup_llm_disabled", request_id="startup")

    # Feedback store
    project_root = Path(__file__).resolve().parents[1]
    _feedback_store = FeedbackStore(project_root / "feedback" / "corrections.jsonl")

    _log_json(
        "startup_ok",
        request_id="startup",
        metric_registry=str(config.metric_registry),
        version="0.5",
    )


# ----------------------------
# Exception handlers
# ----------------------------

@app.exception_handler(HTTPException)
def _http_exception_handler(request: Request, exc: HTTPException):
    request_id = _get_request_id(request)
    _log_json("http_error", request_id=request_id, status_code=exc.status_code, detail=str(exc.detail))
    return _error_response(exc.status_code, request_id, exc)


@app.exception_handler(MetricResolutionError)
def _metric_error_handler(request: Request, exc: MetricResolutionError):
    request_id = _get_request_id(request)
    _log_json("metric_resolution_error", request_id=request_id, error=str(exc))
    return _error_response(400, request_id, exc, details=exc.to_dict())


@app.exception_handler(DateFilterError)
def _date_filter_error_handler(request: Request, exc: DateFilterError):
    request_id = _get_request_id(request)
    _log_json("date_filter_error", request_id=request_id, error=str(exc))
    return _error_response(400, request_id, exc, details=exc.to_dict())


@app.exception_handler(SpecValidationError)
def _spec_validation_error_handler(request: Request, exc: SpecValidationError):
    request_id = _get_request_id(request)
    _log_json("spec_validation_error", request_id=request_id, error=str(exc))
    return _error_response(400, request_id, exc, details=exc.to_dict())


@app.exception_handler(ConfigError)
def _config_error_handler(request: Request, exc: ConfigError):
    request_id = _get_request_id(request)
    logger.error("Config error: %s", exc, exc_info=True)
    return _error_response(503, request_id, exc, details=exc.to_dict())


@app.exception_handler(LLMBackendError)
def _llm_backend_error_handler(request: Request, exc: LLMBackendError):
    request_id = _get_request_id(request)
    _log_json("llm_backend_error", request_id=request_id, error=str(exc))
    return _error_response(502, request_id, exc)


@app.exception_handler(Exception)
def _unhandled_exception_handler(request: Request, exc: Exception):
    request_id = _get_request_id(request)
    logger.error(
        "Unhandled exception: %s", exc,
        exc_info=True,
    )
    _log_json(
        "unhandled_error",
        request_id=request_id,
        error=str(exc),
        error_type=type(exc).__name__,
        traceback=traceback.format_exc(),
    )
    return _error_response(500, request_id, exc)


# ----------------------------
# NL parsing (LLM or rule-based)
# ----------------------------

def _run_with_timeout(fn, *args, timeout_sec: int = 0):
    """Run a synchronous function in the thread pool with a timeout.

    If timeout_sec <= 0 the call runs without a timeout.
    Raises OllamaError on timeout so the 502 handler fires.
    """
    if timeout_sec <= 0:
        return fn(*args)
    future = _llm_pool.submit(fn, *args)
    try:
        return future.result(timeout=timeout_sec)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise LLMBackendError(f"LLM request timed out after {timeout_sec}s")


def _parse_question(question: str) -> Dict[str, Any]:
    """Parse a natural language question into a Spec dict."""
    if _llm_adapter is not None:
        return _run_with_timeout(
            _llm_adapter.parse_nl_to_spec,
            question,
            timeout_sec=config.ollama_timeout,
        )
    return nl_to_spec(question, config.metric_registry)


def _try_llm_disambiguate(
    question: str, column: str, candidates: list,
) -> Optional[str]:
    """Attempt LLM-based column disambiguation. Returns chosen table or None."""
    if _llm_adapter is not None:
        return _run_with_timeout(
            _llm_adapter.disambiguate,
            question, column, candidates,
            timeout_sec=config.ollama_timeout,
        )
    return None


# ----------------------------
# Routes
# ----------------------------

@app.post(
    "/query/sql",
    response_class=PlainTextResponse,
    responses={
        200: {"description": "SQL as plain text (copy/paste friendly)."},
        409: {"description": "Clarifications required; use /query for JSON contract."},
    },
)
def query_sql(req: QueryRequest):
    spec = _parse_question(req.question)

    clarifications = spec.get("clarifications", []) or []
    if clarifications:
        raise HTTPException(status_code=409, detail={"clarifications": clarifications, "spec": spec})

    try:
        sql = execute_spec(spec)
    except AmbiguousDimensionError as e:
        chosen = _try_llm_disambiguate(req.question, e.column, e.candidates)
        if chosen:
            for i, d in enumerate(spec.get("dimensions", [])):
                if d == e.column:
                    spec["dimensions"][i] = f"{chosen}.{e.column}"
            try:
                sql = execute_spec(spec)
                return PlainTextResponse(content=sql.strip() + "\n")
            except Exception as retry_exc:
                logger.warning(
                    "LLM disambiguation retry failed for column '%s': %s",
                    e.column, retry_exc,
                )
        raise HTTPException(status_code=409, detail={
            "clarifications": [{
                "field": f"dimensions.{e.column}",
                "reason": "ambiguous_column",
                "question": e.question,
                "choices": e.candidates,
            }],
            "spec": spec,
        })
    return PlainTextResponse(content=sql.strip() + "\n")


@app.post(
    "/query",
    response_model=QueryResponse,
    responses={
        200: {
            "description": "Returns either SQL or clarifications (mutually exclusive).",
            "content": {
                "application/json": {
                    "examples": {
                        "sql_returned": {"summary": "SQL returned", "value": QUERY_RESPONSE_SQL_EXAMPLE},
                        "clarifications_required": {
                            "summary": "Clarifications required",
                            "value": QUERY_RESPONSE_CLARIFICATIONS_EXAMPLE,
                        },
                    }
                }
            },
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request (invalid metric, date filter, etc.).",
            "content": {"application/json": {"example": ERROR_400_EXAMPLE}},
        },
        422: {"description": "Validation error (FastAPI/Pydantic)."},
        500: {
            "model": ErrorResponse,
            "description": "Unhandled error (service layer).",
        },
    },
)
def query(
    req: QueryRequest = Body(..., example=QUERY_REQUEST_EXAMPLE),
    api_key: Optional[str] = Depends(get_api_key),
):
    request_id = _new_request_id()
    t0 = time.time()

    parser_used = "unknown"
    _log_json("query_received", request_id=request_id, question_len=len(req.question), api_key_present=bool(api_key))

    spec = _parse_question(req.question)
    parser_used = spec.get("notes", {}).get("parser", "rule_based")

    clarifications = spec.get("clarifications", []) or []
    if clarifications:
        _log_json(
            "query_clarifications",
            request_id=request_id,
            parser=parser_used,
            clarifications_count=len(clarifications),
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        return QueryResponse(
            request_id=request_id,
            spec=spec,
            sql=None,
            clarifications=clarifications,
        )

    try:
        sql = execute_spec(spec)
    except AmbiguousDimensionError as e:
        chosen = _try_llm_disambiguate(req.question, e.column, e.candidates)
        if chosen:
            for i, d in enumerate(spec.get("dimensions", [])):
                if d == e.column:
                    spec["dimensions"][i] = f"{chosen}.{e.column}"
            try:
                sql = execute_spec(spec)
                _log_json(
                    "query_llm_disambiguated",
                    request_id=request_id,
                    column=e.column,
                    chosen_table=chosen,
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
                return QueryResponse(
                    request_id=request_id,
                    spec=spec,
                    sql=sql,
                    clarifications=[],
                )
            except Exception as retry_exc:
                logger.warning(
                    "LLM disambiguation retry failed for column '%s': %s",
                    e.column, retry_exc,
                )

        clarification = {
            "field": f"dimensions.{e.column}",
            "reason": "ambiguous_column",
            "question": e.question,
            "choices": e.candidates,
        }
        _log_json(
            "query_disambiguation_needed",
            request_id=request_id,
            column=e.column,
            candidates=e.candidates,
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        return QueryResponse(
            request_id=request_id,
            spec=spec,
            sql=None,
            clarifications=[clarification],
        )

    _log_json(
        "query_sql_built",
        request_id=request_id,
        parser=parser_used,
        sql_len=len(sql),
        elapsed_ms=int((time.time() - t0) * 1000),
    )
    return QueryResponse(
        request_id=request_id,
        spec=spec,
        sql=sql,
        clarifications=[],
    )


@app.post(
    "/query/continue",
    response_model=QueryResponse,
    responses={
        200: {
            "description": "Applies explicit answers to a prior spec and returns SQL or remaining clarifications.",
            "content": {
                "application/json": {
                    "examples": {
                        "sql_returned": {"summary": "SQL returned", "value": QUERY_RESPONSE_SQL_EXAMPLE},
                        "remaining_clarifications": {
                            "summary": "Still needs clarifications",
                            "value": QUERY_RESPONSE_CLARIFICATIONS_EXAMPLE,
                        },
                    }
                }
            },
        },
        422: {"description": "Validation error (FastAPI/Pydantic)."},
        500: {
            "model": ErrorResponse,
            "description": "Unhandled error (service layer).",
        },
    },
)
def continue_query(
    req: ContinueRequest = Body(..., example=CONTINUE_REQUEST_EXAMPLE),
    api_key: Optional[str] = Depends(get_api_key),
):
    request_id = _new_request_id()
    t0 = time.time()

    _log_json(
        "continue_received",
        request_id=request_id,
        api_key_present=bool(api_key),
    )

    spec = dict(req.spec)  # shallow copy

    # Apply explicit answers (no guessing)
    for field_path, value in (req.answers or {}).items():
        _set_by_path(spec, field_path, value)

    clarifications = spec.get("clarifications", []) or []
    if clarifications:
        _log_json(
            "continue_remaining_clarifications",
            request_id=request_id,
            clarifications_count=len(clarifications),
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        return QueryResponse(
            request_id=request_id,
            spec=spec,
            sql=None,
            clarifications=clarifications,
        )

    try:
        sql = execute_spec(spec)
    except AmbiguousDimensionError as e:
        clarification = {
            "field": f"dimensions.{e.column}",
            "reason": "ambiguous_column",
            "question": e.question,
            "choices": e.candidates,
        }
        _log_json(
            "continue_disambiguation_needed",
            request_id=request_id,
            column=e.column,
            candidates=e.candidates,
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        return QueryResponse(
            request_id=request_id,
            spec=spec,
            sql=None,
            clarifications=[clarification],
        )

    _log_json(
        "continue_sql_built",
        request_id=request_id,
        sql_len=len(sql),
        elapsed_ms=int((time.time() - t0) * 1000),
    )
    return QueryResponse(
        request_id=request_id,
        spec=spec,
        sql=sql,
        clarifications=[],
    )


@app.post("/feedback", response_model=FeedbackResponse)
def submit_feedback(req: FeedbackRequest):
    """Record a user correction for pattern analysis."""
    record = CorrectionRecord(
        feedback_id=str(uuid.uuid4()),
        timestamp=datetime.datetime.now().isoformat(),
        request_id=req.request_id,
        original_question=req.original_question,
        original_spec=req.original_spec,
        corrected_spec=req.corrected_spec,
        correction_type=req.correction_type,
        notes=req.notes,
    )
    _feedback_store.append(record)
    _log_json(
        "feedback_recorded",
        request_id=req.request_id,
        feedback_id=record.feedback_id,
        correction_type=req.correction_type,
    )
    return FeedbackResponse(feedback_id=record.feedback_id)


@app.get("/healthz")
def healthz():
    """Liveness probe — confirms the process is running."""
    return {"ok": True, "version": "0.5"}


@app.get("/ready")
def ready():
    """Readiness probe — validates config and checks LLM connectivity."""
    try:
        config.validate()
    except ConfigError as e:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "reason": str(e)},
        )

    llm_status: Dict[str, Any] = {"enabled": config.llm_enabled, "provider": config.llm_provider}
    if _llm_adapter is not None:
        llm_status["backend_available"] = _llm_adapter.backend.is_available()
        llm_status["model"] = _llm_adapter.backend.model_name
        # Backward compat key for monitoring scripts
        llm_status["ollama_available"] = llm_status["backend_available"]
    else:
        llm_status["backend_available"] = False
        llm_status["ollama_available"] = False

    return {"ok": True, "llm": llm_status}

from __future__ import annotations

import asyncio
import concurrent.futures
import datetime
import hashlib
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

from pathlib import Path as _Path
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(_Path(__file__).resolve().parents[1] / ".env", override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on OS environment variables

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
from tools.feedback_store import (
    CorrectionRecord,
    FeedbackLockedError,
    FeedbackStore,
    VALID_TYPES,
    get_feedback_path,
)
from tools.feedback_analyzer import generate_recommendations, generate_feedback_log
from tools.llm_adapter import LLMAdapter, FailoverBackend, build_llm_adapter


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

# Tracks the active provider name so /providers can report it
_current_provider: str = config.llm_provider

# Feedback store (set at startup)
_feedback_store: Optional[FeedbackStore] = None

# Thread pool for running synchronous LLM calls with timeout
_llm_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# In-memory cache for /suggest (keyed by SHA-256 of question+spec JSON)
_suggest_cache: Dict[str, List[str]] = {}
_SUGGEST_CACHE_MAX = 500  # evict oldest entry when limit reached


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
    previous_context: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional context from the previous successful query. "
            "Keys: 'question' (str) and 'spec' (dict). "
            "Injected into the LLM prompt so follow-up questions inherit "
            "platform, date, and metric context."
        ),
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


class ProviderInfo(BaseModel):
    name: str
    label: str
    model: str
    available: bool
    configured: bool


class ProvidersResponse(BaseModel):
    current_provider: str
    providers: List[ProviderInfo]
    using_fallback: bool = False
    fallback_provider: str = ""


class ProviderSwitchRequest(BaseModel):
    provider: str = Field(description="Provider to activate: 'groq' or 'ollama'")


class ProviderSwitchResponse(BaseModel):
    provider: str
    model: str
    available: bool


class SummarizeRequest(BaseModel):
    question: str = Field(min_length=1, max_length=4000)
    sql: str = Field(min_length=1, max_length=50000)
    results_json: List[Dict[str, Any]] = Field(default_factory=list)


class SummarizeResponse(BaseModel):
    summary: str


class SuggestRequest(BaseModel):
    question: str = Field(min_length=1, max_length=4000)
    spec: Dict[str, Any]


class SuggestResponse(BaseModel):
    suggestions: List[str]


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
                fallback_provider=config.llm_fallback or None,
            )
        except Exception as e:
            _llm_adapter = None
            logger.warning("LLM adapter init failed: %s", e, exc_info=True)
            _log_json("startup_llm_failed", request_id="startup", error=str(e))
    else:
        _log_json("startup_llm_disabled", request_id="startup")

    # Feedback store — path from NL_SQL_FEEDBACK_PATH env var or default local path
    _feedback_store = FeedbackStore(get_feedback_path())

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


def _parse_question(
    question: str, previous_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Parse a natural language question into a Spec dict.

    Args:
        question: Natural language question.
        previous_context: Optional dict with keys 'question' and 'spec' from
                          the previous successful query, forwarded to the LLM
                          so follow-up questions can inherit context.
    """
    if _llm_adapter is not None:
        return _run_with_timeout(
            _llm_adapter.parse_nl_to_spec,
            question,
            previous_context,
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

    spec = _parse_question(req.question, req.previous_context)
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


def _regenerate_feedback_markdown():
    """Regenerate RECOMMENDATIONS.md and FEEDBACK_LOG.md from feedback store."""
    try:
        records = _feedback_store.load_all()
        if not records:
            return

        project_root = Path(__file__).resolve().parents[1]
        feedback_dir = project_root / "feedback"
        feedback_dir.mkdir(parents=True, exist_ok=True)

        # Generate RECOMMENDATIONS.md
        recommendations_md = generate_recommendations(records, min_count=1)
        recommendations_path = feedback_dir / "RECOMMENDATIONS.md"
        recommendations_path.write_text(recommendations_md, encoding="utf-8")

        # Generate FEEDBACK_LOG.md
        feedback_log_md = generate_feedback_log(records, max_recent=50)
        feedback_log_path = feedback_dir / "FEEDBACK_LOG.md"
        feedback_log_path.write_text(feedback_log_md, encoding="utf-8")

        _log_json("feedback_markdown_regenerated", record_count=len(records))
    except Exception as e:
        _log_json("feedback_markdown_regeneration_failed", error=str(e))


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
    try:
        _feedback_store.append(record)
    except FeedbackLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    _log_json(
        "feedback_recorded",
        request_id=req.request_id,
        feedback_id=record.feedback_id,
        correction_type=req.correction_type,
    )

    # Auto-regenerate markdown files every 5 feedback submissions
    all_records = _feedback_store.load_all()
    if len(all_records) % 5 == 0:
        _regenerate_feedback_markdown()

    return FeedbackResponse(feedback_id=record.feedback_id)


def _probe_ollama() -> bool:
    """Check Ollama availability without relying on the current adapter."""
    try:
        from tools.llm_adapter import OllamaClient
        client = OllamaClient(base_url=config.ollama_url, model=config.ollama_model, timeout=5)
        return client.is_available()
    except Exception:
        return False


def _probe_groq() -> bool:
    """Check Groq availability (requires GROQ_API_KEY to be configured)."""
    if not config.groq_api_key:
        return False
    try:
        from tools.groq_backend import GroqBackend
        backend = GroqBackend(api_key=config.groq_api_key, model=config.groq_model)
        return backend.is_available()
    except Exception:
        return False


@app.get("/providers", response_model=ProvidersResponse)
def get_providers():
    """List all configured LLM providers with live availability status.

    Both providers are probed in parallel (8 s timeout each) so the response
    is never slower than the slowest reachable backend.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_ollama = pool.submit(_probe_ollama)
        f_groq = pool.submit(_probe_groq)
        try:
            ollama_ok = f_ollama.result(timeout=8)
        except Exception:
            ollama_ok = False
        try:
            groq_ok = f_groq.result(timeout=8)
        except Exception:
            groq_ok = False

    providers = [
        ProviderInfo(
            name="groq",
            label="Groq (Cloud)",
            model=config.groq_model,
            available=groq_ok,
            configured=bool(config.groq_api_key),
        ),
        ProviderInfo(
            name="ollama",
            label="Ollama (Local/IT)",
            model=config.ollama_model,
            available=ollama_ok,
            configured=True,
        ),
    ]

    # Check if the active adapter is running in failover mode
    using_fallback = False
    fallback_provider = config.llm_fallback
    if _llm_adapter is not None and isinstance(_llm_adapter.backend, FailoverBackend):
        using_fallback = _llm_adapter.backend.using_fallback

    return ProvidersResponse(
        current_provider=_current_provider,
        providers=providers,
        using_fallback=using_fallback,
        fallback_provider=fallback_provider,
    )


@app.post("/provider", response_model=ProviderSwitchResponse)
def switch_provider(req: ProviderSwitchRequest):
    """Hot-swap the active LLM backend without restarting the API.

    Rebuilds the LLM adapter with the requested backend.  The switch is
    reflected immediately on subsequent /query calls.
    """
    global _llm_adapter, _current_provider

    if req.provider not in ("groq", "ollama"):
        raise HTTPException(
            status_code=400,
            detail=f"Unknown provider '{req.provider}'. Must be 'groq' or 'ollama'.",
        )

    try:
        if req.provider == "groq":
            if not config.groq_api_key:
                raise HTTPException(
                    status_code=400,
                    detail="GROQ_API_KEY is not configured. Add it to your .env file.",
                )
            from tools.groq_backend import GroqBackend
            new_backend = GroqBackend(api_key=config.groq_api_key, model=config.groq_model)
        else:
            from tools.llm_adapter import OllamaClient
            new_backend = OllamaClient(
                base_url=config.ollama_url,
                model=config.ollama_model,
                timeout=config.ollama_timeout,
            )

        _llm_adapter = build_llm_adapter(
            registry_path=config.metric_registry,
            physical_schema_path=config.physical_schema,
            retriever_chunks_dir=str(config.chunks_dir) if config.chunks_dir else None,
            backend=new_backend,
        )
        _current_provider = req.provider
        available = new_backend.is_available()

        _log_json(
            "provider_switched",
            request_id="switch",
            provider=req.provider,
            model=new_backend.model_name,
            available=available,
        )
        return ProviderSwitchResponse(
            provider=_current_provider,
            model=new_backend.model_name,
            available=available,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to switch to '{req.provider}': {exc}",
        )


@app.post("/summarize", response_model=SummarizeResponse)
def summarize_results(req: SummarizeRequest):
    """Generate a 2-3 sentence plain-English summary of query results.

    Accepts the original question, the generated SQL, and the result rows.
    Uses the active LLM backend (prefers Groq for low latency).
    Returns 503 if no LLM is configured, 502 on LLM failure.
    """
    if _llm_adapter is None:
        raise HTTPException(status_code=503, detail="LLM not available — cannot summarize results")

    # Truncate results to keep tokens manageable (first 50 rows, hard cap 8k chars)
    rows = req.results_json[:50]
    results_str = json.dumps(rows, default=str, indent=2)
    if len(results_str) > 8000:
        results_str = results_str[:8000] + "\n... (truncated)"

    system = (
        "You are a concise marketing data analyst. "
        "Given a user's question and the query results, write a 2-3 sentence plain-English summary "
        "of the key insight. Focus on the most important numbers and trends. "
        "Do not explain the SQL. Be specific with numbers where possible."
    )
    user = (
        f"Question: {req.question}\n\n"
        f"Results ({len(req.results_json)} row(s)):\n{results_str}"
    )

    request_id = _new_request_id()
    _log_json("summarize_request", request_id=request_id, question_len=len(req.question), row_count=len(req.results_json))
    t0 = time.time()

    try:
        result = _run_with_timeout(
            _llm_adapter.backend.chat,
            system,
            user,
            False,   # json_mode=False — we want free-form text
            0.3,     # temperature: slightly warmer for natural prose
            timeout_sec=config.ollama_timeout,
        )
        summary = result.content.strip()
        _log_json(
            "summarize_ok",
            request_id=request_id,
            elapsed_ms=int((time.time() - t0) * 1000),
            output_tokens=result.output_tokens,
        )
        return SummarizeResponse(summary=summary)
    except LLMBackendError as e:
        _log_json("summarize_llm_error", request_id=request_id, error=str(e))
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")


@app.post("/suggest", response_model=SuggestResponse)
def suggest_followups(req: SuggestRequest):
    """Generate 2-3 follow-up question suggestions based on the current query.

    Accepts the original question and the resolved spec.
    Results are cached by content hash to avoid redundant LLM calls.
    Returns 503 if no LLM is configured, 502 on LLM failure.
    """
    if _llm_adapter is None:
        raise HTTPException(status_code=503, detail="LLM not available — cannot suggest follow-ups")

    # Deduplicate via content hash (question + spec, keys sorted for stability)
    raw = json.dumps({"q": req.question, "spec": req.spec}, sort_keys=True)
    cache_key = hashlib.sha256(raw.encode()).hexdigest()
    if cache_key in _suggest_cache:
        return SuggestResponse(suggestions=_suggest_cache[cache_key])

    system = (
        "You are a marketing data analyst assistant. "
        "Given a user's question and the query spec (JSON), suggest exactly 3 concise follow-up questions "
        "that naturally build on or deepen the analysis. Each should be a complete, standalone question. "
        "Good follow-up types: break out by a new dimension, compare to a different time period, "
        "switch platform, add or swap a metric, or filter to a top/bottom segment. "
        'Return ONLY valid JSON in this exact format: {{"suggestions": ["question 1", "question 2", "question 3"]}}'
    )
    user = (
        f"Question: {req.question}\n\n"
        f"Spec: {json.dumps(req.spec, indent=2)}"
    )

    request_id = _new_request_id()
    _log_json("suggest_request", request_id=request_id, question_len=len(req.question))
    t0 = time.time()

    try:
        result = _run_with_timeout(
            _llm_adapter.backend.chat,
            system,
            user,
            True,   # json_mode=True — structured output
            0.7,    # temperature: warmer for varied suggestions
            timeout_sec=config.ollama_timeout,
        )

        # Parse JSON; fall back to empty list on parse failure
        try:
            parsed = json.loads(result.content)
            suggestions = [s for s in parsed.get("suggestions", []) if isinstance(s, str)][:3]
        except (json.JSONDecodeError, AttributeError):
            suggestions = []

        # Evict oldest entry when cache is full (FIFO via dict insertion order)
        if len(_suggest_cache) >= _SUGGEST_CACHE_MAX:
            oldest = next(iter(_suggest_cache))
            del _suggest_cache[oldest]
        _suggest_cache[cache_key] = suggestions

        _log_json(
            "suggest_ok",
            request_id=request_id,
            elapsed_ms=int((time.time() - t0) * 1000),
            suggestion_count=len(suggestions),
        )
        return SuggestResponse(suggestions=suggestions)
    except LLMBackendError as e:
        _log_json("suggest_llm_error", request_id=request_id, error=str(e))
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")


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

# TODO.md - NL SQL Query Builder

## Decision: Consolidate on `physical_schema/` Subsystem

The `physical_schema/` codebase is the foundation going forward. It has:
- Clean data contract (`spec.md`)
- Working metric resolution, join planning, derived formulas
- FastAPI service with tests
- Deterministic SQL generation (no hallucination risk)

The `rag_sql/` subsystem has critical bugs (WHERE/GROUP BY/ORDER BY never render) and would require extensive rework. Archive it.

---

## Priority 1: Critical Fixes - **DONE** (Feb 8, 2026)
- ~~Generic WHERE filters~~ - WhereFilterExtractor class with =, !=, >, <, >=, <=, contains operators
- ~~Column disambiguation~~ - prefer_fact, platform affinity, non-fact heuristic, AmbiguousDimensionError
- ~~Campaign filter consolidation~~ - Consistent filter_config.json
- ~~Date filter edge cases~~ - this week/month/quarter, last week/month/quarter, Q1-Q4 YYYY, YTD

---

## Priority 2: Architecture Cleanup - **DONE** (Feb 8, 2026)
- ~~Folder reorganization~~ - rag_sql/ archived, physical_schema/ promoted
- ~~Dedup~~ - Extracted to tools/common.py (bracket_ident, make_aliases, PLATFORM_TOKEN)
- ~~Imports~~ - try/except dual import pattern, __init__.py added
- ~~Type checking~~ - mypy passes clean on all tools/

---

## Priority 3: LLM Integration - **DONE** (Feb 9, 2026)
- ~~Ollama integration~~ - tools/llm_adapter.py with OllamaClient, PromptBuilder, SchemaContext
- ~~Schema context window~~ - tools/schema_retriever.py (BM25 hybrid retriever)
- ~~Disambiguation via LLM~~ - LLM resolves ambiguous columns before asking user
- ~~Prompt engineering~~ - prompts/ directory with system_prompt.txt, few_shot_examples.json, disambiguation_prompt.txt
- ~~Date awareness~~ - Current date injected into system prompt (fixes "this week"/"last month" hallucination)
- **Validated:** qwen3:14b on 192.168.12.51:11434, 71/71 tests pass, 11/11 real questions correct
- **Config:** `_DEFAULT_OLLAMA_URL = "http://192.168.12.51:11434"`, `_DEFAULT_MODEL = "qwen3:14b"`

---

## Priority 4: Testing Strategy - **DONE** (Feb 9, 2026)

### Summary
- **Total tests: 178** across 13 files (was 71 across 10 files)
- **New dependency:** sqlglot (T-SQL syntax validation)
- **147 offline tests pass** (no LLM/network required)
- **31 LLM tests pass** (require Ollama at 192.168.12.51:11434)

### 4.1 Golden Query Integration Tests - DONE
`test_golden_queries.py` - 31 end-to-end NL -> Spec -> SQL tests:
- ~~Basic metrics:~~ spend yesterday, profit last 7 days (portfolio UNION ALL)
- ~~By campaign:~~ multi-metric campaign filter, impressions by campaign this month
- ~~Platform-specific:~~ Google impressions by campaign, Microsoft cost by campaign
- ~~Derived metrics:~~ CTR, conversion rate, CPC (NULLIF safe divide validated)
- ~~WHERE filters:~~ state = Texas, status = active
- ~~Campaign name:~~ containing 'super bowl', 'spring training'
- ~~Comparisons:~~ cross-platform FULL OUTER JOIN, period-over-period delta
- ~~Date ranges:~~ Q1 2025, YTD, last month, last quarter
- ~~Exchange metrics:~~ exchange revenue via ClosePeerExchangeMetric
- ~~Campaign ID filters:~~ IN (101, 102, 103)
- ~~SQL structure:~~ LEFT JOINs, UNION ALL reaggregate, derived base columns
- ~~Known gap fixed:~~ AccountName dim auto-joins Account table via `_infer_missing_dimension_targets()`

### 4.2 SQL Syntax Validation - DONE
- ~~sqlglot T-SQL parsing~~ - every golden query validated via `sqlglot.parse(sql, read="tsql")`
- ~~Table reference validation~~ - assert_sql_references_table checks expected tables
- ~~JOIN structure validation~~ - LEFT JOIN only (no INNER), correct FULL OUTER for comparisons
- ~~Structural assertions~~ - SELECT/FROM/WHERE/GROUP BY presence verified per query shape

### 4.3 LLM Output Validation - DONE
- ~~JSON schema validation~~ - _ensure_spec_structure in llm_adapter.py
- ~~Metric names must exist in registry~~ - validate_spec in llm_adapter.py
- ~~Date formats~~ - handled by spec_executor
- ~~Fuzz testing~~ - `test_adversarial.py`: 46 tests (edge inputs, SQL injection, conflicting inputs, 30 randomized)
- ~~LLM parity~~ - `test_llm_parity.py`: 32 tests (metric/platform/date parity, LLM spec → SQL validation)
- SQL injection patterns neutralized: DROP, UNION SELECT, comment injection, quote/bracket injection all blocked

---

## Priority 5: Production Readiness (Week 3-4)

### 5.1 Error Handling - **DONE** (Feb 10, 2026)
- ~~Replace bare `raise ValueError` with custom exception hierarchy~~ → `tools/exceptions.py`
  - `NLSQLError` (base) → `MetricResolutionError`, `AmbiguousDimensionError`, `OllamaError`, `SpecValidationError`, `ConfigError`, `DateFilterError`
  - All exceptions carry structured context (`.to_dict()`) for API responses
- ~~API should never return 500 for known errors~~ → per-exception handlers with proper status codes:
  - `MetricResolutionError` → 400, `DateFilterError` → 400, `SpecValidationError` → 400
  - `ConfigError` → 503, `OllamaError` → 502, unhandled → 500 (with traceback logged)
- ~~Fix silent exception swallowing~~ → `except Exception: pass` blocks now log warnings
- LLM timeout retry deferred to Priority 6 (LLM abstraction layer)

### 5.2 Logging & Observability - **DONE** (Feb 10, 2026)
- ~~Structured JSON logging~~ → `JSONFormatter` class, every log line is a JSON object with timestamp/level/logger/message
- ~~Request tracing (correlation IDs)~~ → `RequestLoggingMiddleware`:
  - Reads `X-Request-ID` header or generates UUID
  - Logs `request_start`/`request_end` with method, path, status, elapsed_ms
  - Returns `X-Request-ID` in response headers
  - Skips `/healthz` noise
- ~~Exception tracebacks~~ → catch-all handler uses `logger.error(exc_info=True)` + traceback in JSON

### 5.3 Configuration Management - **DONE** (Feb 10, 2026)
- ~~Centralized config~~ → `tools/config.py` with `AppConfig` dataclass
  - `AppConfig.from_env()` loads all env vars in one place
  - `AppConfig.validate()` checks files exist and are valid JSON (raises `ConfigError`)
  - Replaces ~10 scattered `os.getenv()` calls in app.py
- ~~Environment-based overrides~~ → all config via env vars with documented defaults
- API version bumped to 0.5

### 5.4 API Hardening — **DONE** (Feb 10, 2026)
- ~~Readiness probe~~ → `/ready` endpoint (config validation + LLM connectivity check), `/healthz` is liveness-only
- ~~LLM timeout guard~~ → `_run_with_timeout()` wraps LLM calls via thread pool, raises `OllamaError` on timeout → 502
- ~~ContinueRequest validation~~ → Pydantic `field_validator` ensures spec has required keys before processing
- ~~Dockerfile improvements~~ → non-root user (`nlsql`), HEALTHCHECK interval 30s, `UVICORN_WORKERS` env var
- Auth enforcement, rate limiting, CORS: deferred (not needed yet)

### 5.5 Deployment — **DONE** (Feb 10, 2026)
- ~~docker-compose.yml~~ → API-only service, env vars from `.env`, config files as read-only volume mount
- ~~.env.example~~ → documents all supported environment variables with defaults
- ~~.dockerignore~~ → excludes docker-compose.yml, .env, .env.example from image
- ~~Dockerfile validated~~ → non-root user, 30s healthcheck, configurable workers

---

## Priority 6: LLM Migration Path (Week 5+)

### 6.1 Abstraction Layer — **DONE** (Feb 10, 2026)
- ~~`LLMBackend` Protocol~~ → `tools/llm_backend.py` with `ChatResult` dataclass + `LLMBackend` runtime-checkable protocol
- ~~`LLMBackendError` exception~~ → added to `tools/exceptions.py` as parent of `OllamaError`
- ~~OllamaClient refactored~~ → implements `LLMBackend` protocol, returns `ChatResult` instead of raw dict
- ~~LLMAdapter backend injection~~ → accepts `backend: LLMBackend` param, `ollama` property alias for backward compat
- ~~`build_llm_adapter()` factory~~ → accepts optional `backend` param to override Ollama default
- ~~API updated~~ → exception handler catches `LLMBackendError`, `/ready` uses `backend.*`
- ~~Config surface~~ → `NL_SQL_LLM_PROVIDER` env var (defaults to `ollama`, not branched on yet)
- ~~Tests~~ → 11 new tests: `test_llm_backend.py` (protocol, ChatResult, hierarchy) + `test_llm_adapter.py` (backend injection, alias)
- **Total tests: 158** (was 147 offline + 31 LLM parity)

### 6.2 Evaluation Framework — **DONE** (Feb 10, 2026)
- ~~Accuracy scoring~~ → `tools/eval_harness.py` with weighted scoring (metrics 0.40, platform 0.20, dims 0.15, date 0.15, grain 0.10)
- ~~A/B dual-parser comparison~~ → runs each question through both rule-based (`nl_to_spec`) and LLM (`LLMAdapter`) parsers
- ~~Cost/token tracking~~ → `ChatResult` extended with `input_tokens`/`output_tokens`, Ollama extracts `prompt_eval_count`/`eval_count`
- ~~Evaluation dataset~~ → `evals/eval_dataset.json` (25 golden entries with expected specs, tagged)
- ~~CLI harness~~ → `python tools/eval_harness.py [--tags parity] [--cost-input 0.001]`, writes `evals/run_*.json`
- ~~Config~~ → `cost_per_input_token`/`cost_per_output_token` in AppConfig (default 0.0 for Ollama)
- ~~Tests~~ → 24 new tests in `test_eval_harness.py` (scoring logic, date classification, set/exact matching)
- **Total tests: 182** (was 158 offline + 31 LLM parity)

### 6.3 Feedback Loop — **DONE** (Feb 11, 2026)
- ~~Feedback storage~~ → `tools/feedback_store.py` with CorrectionRecord dataclass, JSONL append-only store, thread-safe writes
- ~~Feedback API~~ → POST `/feedback` endpoint in `api/app.py` with FeedbackRequest/FeedbackResponse models, correction_type validation
- ~~Pattern analysis~~ → `tools/feedback_analyzer.py` CLI: detects metric synonym gaps, dimension disambiguation patterns, date filter misinterpretations, platform detection gaps, few-shot candidates
- ~~Recommendations~~ → Generates `feedback/RECOMMENDATIONS.md` markdown file for Claude Code review
- ~~Correction types~~ → `metric_mismatch`, `dimension_wrong`, `platform_wrong`, `date_filter_wrong`, `filter_wrong`, `other`
- ~~Tests~~ → 27 new tests: `test_feedback_store.py` (11), `test_feedback_analyzer.py` (16), `test_api_query.py` (+3)
- **Total tests: 209** (was 182 offline + 31 LLM parity)

### 6.4 Multi-Fact-Table CTE+JOIN — **DONE** (Feb 12, 2026)
- ~~Metric partitioning~~ → `MetricResolver.partition_metrics()` groups metrics by resolvable fact table; single-table fast path unchanged
- ~~CTE+JOIN builder~~ → `spec_executor._build_multi_fact_cte()` builds independent CTEs per partition, FULL OUTER JOINs on dimension columns
- ~~Bridge table resolution~~ → Dijkstra join planner automatically finds Event→CampaignEventMap→Campaign path for exchange metrics
- ~~COALESCE dimensions~~ → Outer SELECT uses COALESCE for dimensions, per-CTE metric references
- ~~No-dimension support~~ → CROSS JOIN for total-only aggregates (no GROUP BY)
- ~~Zero regression~~ → Existing single-fact-table queries bypass CTE entirely (identical output)
- ~~Tests~~ → 20 new tests in `test_multi_fact.py`: partition logic (8), CTE+JOIN output (8), end-to-end integration (4)
- **Total tests: 229** offline + 20 multi-fact = **249** (was 209 offline + 31 LLM parity)

### 6.5 Streamlit Chat UI — **DONE** (Feb 13, 2026)
- ~~Chat interface~~ → `ui/Query Builder.py` Streamlit app with chat bubble history, `st.chat_input` for NL questions
- ~~SQL display~~ → Generated SQL shown in `st.expander("View Generated SQL")` with `st.code(sql, language="sql")`; sidebar toggle for auto-expand
- ~~Clarification loop~~ → When API returns clarifications, renders `st.radio()` per clarification with Submit button; calls POST `/query/continue`
- ~~Sidebar status~~ → Calls GET `/ready` to show API connection + LLM availability + model name; configurable API URL
- ~~CORS middleware~~ → Added `CORSMiddleware(allow_origins=["*"])` to `api/app.py` for future browser-based clients
- ~~Error handling~~ → HTTP 400/409/422/500 mapped to user-friendly messages with expandable error details
- ~~Dependencies~~ → `ui/requirements.txt`: `streamlit>=1.30`, `requests>=2.31`
- ~~Launch~~ → `cd physical_schema && python -m streamlit run "ui/Query Builder.py"` (requires API on port 8000)

### 6.6 Direct Query Execution — **DONE** (Feb 13, 2026)
- ~~Fabric connection module~~ → `tools/fabric_conn.py` with `FabricConnection` class (pyodbc + azure-identity)
  - `InteractiveBrowserCredential` for Azure AD auth (browser popup login)
  - Token scope: `https://database.windows.net/.default`, struct-packed for `SQL_COPT_SS_ACCESS_TOKEN = 1256`
  - Server: `*.datawarehouse.fabric.microsoft.com`, database: `RDMWarehouse`
  - Row limit default 10,000 (safety cap, configurable via `FABRIC_ROW_LIMIT` env var)
- ~~Sidebar connection UI~~ → "Connect to Fabric" button triggers browser login, shows connected/disconnected status
- ~~Run Query button~~ → Per-message button below SQL expander, executes SQL via `FabricConnection.execute()`
- ~~Results display~~ → `st.dataframe(df)` with row count caption, persists in chat history
- ~~Results formatting~~ → pandas Styler with currency ($), percentages (%), comma separators for integers
- ~~Editable SQL~~ → Changed from `st.code()` to `st.text_area()` for interactive SQL editing before execution
- ~~Error handling~~ → Connection lost, permission denied, query timeout mapped to user-friendly messages
- ~~Dependencies~~ → `pyodbc>=4.0.39`, `azure-identity>=1.13.0`, `pandas>=1.5.0` added to `ui/requirements.txt`
- ~~Config~~ → `FABRIC_SERVER`, `FABRIC_DATABASE`, `FABRIC_DRIVER`, `FABRIC_ROW_LIMIT` in `.env.example`

### 6.7 Multi-Date Comparison Matrix — **DONE** (Feb 13, 2026)
- ~~Shared UI utilities~~ → `ui/shared.py` extracted from `Query Builder.py`: `format_results()`, `init_fabric_state()`, `render_fabric_sidebar()`
- ~~Multi-page architecture~~ → `ui/pages/Multi Date Reporting.py` auto-discovered by Streamlit (sidebar navigation)
- ~~Client-side SQL generation~~ → Imports `spec_executor` directly, no API roundtrip (NL parsing not needed for form input)
- ~~Programmatic spec building~~ → Correct format: `filters: {date: {date_from, date_to}, where: [{field, op, value}]}`
- ~~Platform filter~~ → Dropdown with google_ads/microsoft_ads (internal keys), display labels: Google/Microsoft
- ~~Account filter~~ → Single account text input (AccountName WHERE filter)
- ~~Campaign filter~~ → "Campaign Contains" text input (case-insensitive LIKE %value%)
- ~~Date range inputs~~ → 2-5 configurable ranges with labels and date pickers (default: 7-day periods going backward)
- ~~Summary matrix~~ → Rows = date range labels, columns = aggregate metrics (SUM across all rows per query)
- ~~SQL display~~ → Collapsible expander showing all generated queries with date range labels as comments
- ~~Formatted results~~ → Currency ($), percentages (%), commas reused via `shared.format_results()`
- **Spec format gotcha:** Must use `filters.date` and `filters.where` (not `date_filter` or flat `filters.AccountName`) for WHERE clauses to generate correctly

### 6.8 Revenue Per Conversion Metric — **DONE** (Feb 13, 2026)
- ~~Added "revenue per conversion"~~ → Derived metric in `metric_registry.json`: `revenue / conversions`
- ~~Updated semantic names~~ → "revenue per click" semantic_name changed from "rpc" to "rpcl" (avoid conflict)
- ~~Aliases updated~~ → "rpc" → "revenue per conversion", "rpcl" → "revenue per click"
- ~~Grain support~~ → Both metrics support google_ads/microsoft_ads platforms at campaign_calendar/adgroup_calendar grains
- ~~Build summary~~ → 19 metrics total (was 18), 6 derived (was 5)

---

## Priority 7: Enhanced User Experience

### 7.1 Conversational Context & Query Chaining
- [ ] Session state management — Store previous_spec, previous_dimensions, previous_filters in Streamlit session
- [ ] Context-aware NL parsing — LLMAdapter accepts optional previous_context parameter
- [ ] Follow-up question detection — Recognize patterns like "now break that down by X", "add Y to that", "same thing but for Z"
- [ ] Incremental spec building — Merge follow-up requests with previous spec instead of starting from scratch
- [ ] UI affordances — Show "Previous query" context in chat, "Clear context" button
- [ ] Context timeout — Auto-clear context after N minutes of inactivity

**Value:** Makes chat feel like natural conversation, enables iterative data exploration

### 7.2 Schema Explorer & Metric Catalog UI — **PHASE 2 COMPLETE** (Feb 13, 2026)
- ~~Metrics browser~~ — Tabular view with name, type, class, formula, platforms, grains; expandable formula details for derived metrics
- ~~Dimensions browser~~ — Dynamic extraction via `tools/dimension_extractor.py`, shows source tables and occurrence count
- ~~Platform/grain matrix~~ — Numeric grid showing metric count per platform+grain combination
- ~~Derived metric formulas~~ — Expandable sections showing formula, base metrics, supported platforms/grains
- ~~Search & filter~~ — Global search box, platform filter, domain filter (affects Metrics Browser tab)
- ~~Click-to-add integration~~ — Metric selection with session state, pre-populates chat page
- ~~Sample data preview~~ — Load top 10 dimension values from Fabric on demand with 24h caching
- ~~Dynamic dimension extraction~~ — `DimensionExtractor` parses physical_schema.json with heuristics (min 2 table occurrences)
- [ ] Business definitions — Human-readable descriptions (deferred to Phase 3: add `description` field to registry)
- [ ] Table relationship diagram — Visual graph (deferred to Phase 3: requires graphviz/plotly)

**Value:** Solves discoverability problem, helps users learn what's queryable without guessing
**Page location:** `ui/pages/Schema Explorer.py` (auto-discovered as "Schema Explorer" in Streamlit sidebar)
**Phase 2 additions:**
- `tools/dimension_extractor.py` — Automatic dimension discovery from physical schema
- Fabric integration for sample data preview (requires connection)
- Session state sharing with chat page for selected metrics

### 7.3 Query History, Favorites & Sharing
- [ ] Query history storage — SQLite or JSONL storage of all queries with NL question, spec, SQL, timestamp, row count
- [ ] History UI page — Chronological list with search/filter, re-run and edit buttons
- [ ] Favorites/bookmarks — Star queries, add names/descriptions/tags
- [ ] Query versioning — Track iterations of the same logical query, show diffs
- [ ] Shareable URLs — Generate persistent URLs that encode query specs (base64 in URL params)
- [ ] Export query definitions — Download spec JSON or SQL for external use
- [ ] Team sharing — Optional: Multi-user query library (requires auth)
- [ ] Query collections — Group related queries into folders/projects

**Value:** Makes queries reusable and shareable, builds institutional knowledge over time

### 7.4 Feedback & Continuous Improvement System — **DONE** (Feb 16, 2026)
- ~~Feedback UI in Query Builder~~ → 👍👎 buttons after each SQL query, expandable correction form with 6 correction types
- ~~Feedback storage~~ → JSONL append-only store at `feedback/corrections.jsonl` (thread-safe via `FeedbackStore`)
- ~~Pattern analysis~~ → `tools/feedback_analyzer.py` detects: metric synonyms, dimension preferences, date filter gaps, platform aliases, few-shot candidates
- ~~Markdown export~~ → Generates `FEEDBACK_LOG.md` (human-readable log) + `RECOMMENDATIONS.md` (actionable fixes)
- ~~Admin dashboard~~ → `ui/pages/Feedback_Dashboard.py` with stats, top issues, recent feedback viewer, download buttons
- ~~Auto-regeneration~~ → API triggers markdown regeneration every 5 feedback submissions
- ~~Improvement loop~~ → Admin downloads MD files → uploads to Claude Code → Claude implements fixes → pushes to GitHub → users get updates via git pull
- **Correction types:** metric_mismatch, dimension_wrong, platform_wrong, date_filter_wrong, filter_wrong, other
- **Files created:** `feedback/corrections.jsonl`, `feedback/FEEDBACK_LOG.md`, `feedback/RECOMMENDATIONS.md`
- **CLI:** `python tools/feedback_analyzer.py [--min-count N] [--max-recent N]`

**Value:** Zero-friction feedback mechanism, automated pattern detection, direct feedback-to-fix loop with AI, continuous improvement tracked in Git

---

## Out of Scope (Documented for Future)
- [ ] Result visualization / charting
- [ ] Multi-tenant support
- [ ] Query optimization suggestions
- [ ] Window function support (RANK, LAG/LEAD)
- [ ] Natural language result summarization (implement after core UX features)

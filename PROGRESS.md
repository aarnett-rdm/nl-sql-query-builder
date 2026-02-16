# PROGRESS.md - NL SQL Query Builder

## Project Overview
Natural language to T-SQL query builder for Microsoft Fabric, targeting business users in the tickets/marketing department.

---

## Folder Structure (Reorganized Feb 6, 2026)

```
rag/
├── physical_schema/            # ACTIVE CODEBASE - all development happens here
│   ├── tools/                  # Core Python package (10 files)
│   │   ├── __init__.py        # Package marker
│   │   ├── common.py          # Shared constants + utilities
│   │   ├── nl_to_spec.py      # NL parser -> Spec dict (rule-based fallback)
│   │   ├── llm_adapter.py     # LLM integration: Ollama -> Spec JSON (NEW)
│   │   ├── schema_retriever.py # BM25 schema chunk retriever for LLM context (NEW)
│   │   ├── spec_executor.py   # Spec -> SQL orchestrator
│   │   ├── query_builder.py   # Deterministic T-SQL builder
│   │   ├── metric_resolver.py # Registry-driven metric resolution
│   │   ├── join_planner.py    # Dijkstra join planning
│   │   └── qualify_ambiguous_tables.py  # Table name disambiguation utility
│   ├── prompts/               # Versioned LLM prompt templates (NEW)
│   │   ├── system_prompt.txt  # Main system prompt with schema/metric context
│   │   ├── few_shot_examples.json  # 10 canonical question -> spec pairs
│   │   └── disambiguation_prompt.txt  # Column disambiguation prompt
│   ├── api/app.py             # FastAPI service (v0.4, LLM-enabled)
│   ├── tests/                 # Test suite (10 files, 71 tests)
│   ├── current/               # Active configs (physical_schema.json, metric_registry.json, filter_config.json)
│   ├── inputs/                # Source CSVs for schema rebuilds
│   ├── join_plans/            # Reference join plan examples
│   ├── Dockerfile
│   ├── requirements.api.txt
│   └── spec.md                # Locked NL->Spec contract
├── archive/                   # Old code - reference only, not maintained
│   ├── rag_sql_old/           # Original subsystem (critical bugs, superseded)
│   ├── hybrid_retriever.py    # BM25+embedding retriever (reuse for LLM integration)
│   ├── notebooks/             # Jupyter notebooks
│   ├── old_config/            # Old semantic_schema configs
│   ├── old_outputs/           # Old outputs + semantic chunks
│   ├── physical_schema_iterations/  # Earlier schema/registry iterations
│   └── docs/                  # Word docs (Database Breakdown)
├── TODO.md                    # Prioritized fix list + LLM integration plan
├── PROGRESS.md                # This file
└── sql-query-builder-project-brief.md  # Original project brief
```

**Decision (Feb 6, 2026):** Consolidated on `physical_schema/` subsystem. The `rag_sql/` subsystem is archived - it had critical bugs (WHERE/GROUP BY/ORDER BY never rendered) and would require a near-complete rewrite.

---

## Active Codebase: `physical_schema/`

**Built:** Dec 23-31, 2025 | **Last Updated:** Feb 9, 2026 | **Status:** LLM integration complete (Priority 3)

Pipeline: `nl_to_spec.py` -> `spec_executor.py` -> `query_builder.py` (uses `metric_resolver.py` + `join_planner.py`)

| Module | Purpose | Status |
|--------|---------|--------|
| [tools/llm_adapter.py](physical_schema/tools/llm_adapter.py) | LLM integration: Ollama -> Spec JSON, validation, fallback | **New** (Feb 9) |
| [tools/schema_retriever.py](physical_schema/tools/schema_retriever.py) | BM25 schema chunk retriever for LLM context window | **New** (Feb 9) |
| [tools/nl_to_spec.py](physical_schema/tools/nl_to_spec.py) | Rule-based NL parser (fallback when LLM unavailable) | Working |
| [tools/spec_executor.py](physical_schema/tools/spec_executor.py) | Orchestrates spec -> SQL, handles portfolio/comparisons | Working |
| [tools/query_builder.py](physical_schema/tools/query_builder.py) | Deterministic SQL builder with disambiguation flow | Working |
| [tools/metric_resolver.py](physical_schema/tools/metric_resolver.py) | Registry-driven metric resolution, derived formulas | Working |
| [tools/join_planner.py](physical_schema/tools/join_planner.py) | Dijkstra-based join planning with platform awareness | Working |
| [tools/common.py](physical_schema/tools/common.py) | Shared constants + utilities (PLATFORM_TOKEN, bracket_ident, etc.) | Working |
| [tools/qualify_ambiguous_tables.py](physical_schema/tools/qualify_ambiguous_tables.py) | Resolves ambiguous unqualified table names | Working (utility) |
| [api/app.py](physical_schema/api/app.py) | FastAPI service v0.4 (/query, /continue, /sql, /healthz + LLM) | **Updated** (Feb 9) |

---

## What's Working

1. **Metric resolution from registry** - preferred_fact_table routing by (grain, platform)
2. **JOIN planning** - Dijkstra with platform-aware dimension preferences, confidence weighting
3. **NL parsing** - Metric extraction, platform detection, time phrases, generic WHERE filters, extended dates
4. **Derived metric expansion** - CTR, CPC, conversion rate etc. expand from registry formulas
5. **Campaign name filtering** - Free-text "campaign name contains 'X'" with multi-value AND/OR
6. **Campaign ID filtering** - Numeric ID lists
7. **Cross-platform queries** - UNION Google + Microsoft with re-aggregation
8. **Period-over-period comparison** - "last 7 days vs prior 7 days"
9. **Column disambiguation** - Auto-resolution via prefer_fact, platform affinity, non-fact heuristic; structured AmbiguousDimensionError for clarification
10. **Extended date filters** - this week/month/quarter, last week/month/quarter, Q1-Q4 YYYY, YTD
11. **Generic WHERE filters** - "where state = Texas", "cost > 100", "account contains venue" etc.
12. **FastAPI service** - /query, /query/continue, /query/sql, /healthz with clarification flow
13. **Test suite** - 71 tests across 10 files (NL parsing, WHERE filters, dates, disambiguation, campaigns, comparisons, smoke, LLM adapter)
14. **Type checking** - mypy passes clean on all tools/
15. **LLM integration (Ollama)** - "LLM Plans, Python Generates" architecture fully implemented
16. **Schema context retrieval** - BM25 hybrid retriever connected for LLM context window management
17. **LLM disambiguation** - Automatic column disambiguation via LLM before falling back to user
18. **Prompt engineering** - Versioned system prompt, 10 few-shot examples, anti-hallucination guardrails
19. **Graceful fallback** - LLM unavailable? Rule-based parser handles the request seamlessly

---

## What Needs Work

### Critical (blocks production use)
1. ~~Generic WHERE filters~~ - **DONE** (Feb 8, 2026)
2. ~~Column disambiguation~~ - **DONE** (Feb 8, 2026)
3. ~~LLM integration~~ - **DONE** (Feb 9, 2026) - Ollama adapter, schema retrieval, disambiguation, prompt engineering

### High Priority
4. ~~Date filter gaps~~ - **DONE** (Feb 8, 2026)
5. ~~`hybrid_retriever.py` unused~~ - **DONE** (Feb 9, 2026) - Connected as schema_retriever.py in tools/

### Medium Priority
6. ~~Code duplication~~ - **DONE** (Feb 8, 2026) - Extracted to tools/common.py
7. ~~Bare imports~~ - **DONE** (Feb 8, 2026) - try/except import pattern, __init__.py added

---

## Technical Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| ~Oct 2025 | Built `rag_sql/` with enriched semantic schema | Needed column-level metadata for NL grounding |
| ~Dec 2025 | Built `physical_schema/` as cleaner rewrite | Simpler contract, better metric resolution, testable |
| Dec 2025 | Registry-driven metric binding (not schema-index grounding) | Eliminates ambiguity for known metrics |
| Dec 2025 | Dijkstra over BFS for join planning | Weighted edges (confidence levels) produce better paths |
| Dec 2025 | "LLM plans, Python generates" architecture | Prevents SQL hallucination |
| Dec 2025 | FastAPI for service layer | Lightweight, async-ready, good OpenAPI docs |
| **Feb 6, 2026** | **Consolidated on `physical_schema/`, archived `rag_sql/`** | `rag_sql/` had critical bugs and incompatible data contracts |
| **Feb 8, 2026** | **Priority 1 complete**: generic WHERE filters, disambiguation, extended dates | Core NL->SQL pipeline robust for supported patterns |
| **Feb 8, 2026** | **Priority 2 complete**: dedup to common.py, dual imports, mypy clean | Architecture cleanup; `tools/` is now a proper package |
| **Feb 9, 2026** | **Priority 3 complete**: LLM integration via Ollama | llm_adapter.py, schema_retriever.py, prompts/, API wiring, 25 new tests |
| **Feb 9, 2026** | Ollama + qwen3:14b as default LLM (was codellama:34b) | Best JSON output among available models; validated 11/11 real questions |
| **Feb 9, 2026** | BM25 retriever connected for schema context | Keeps LLM context focused on relevant tables (not all 200+) |
| **Feb 9, 2026** | Graceful LLM fallback to rule-based parser | Service stays operational even if Ollama is down |
| **Feb 9, 2026** | Current date injection into LLM system prompt | Fixes "this week"/"last month" date hallucination; LLM now computes correct ranges |
| **Feb 9, 2026** | Ollama server: 192.168.12.51:11434 | Work-hosted server with qwen3:14b, phi4, llama3:8b, mistral, etc. |

---

## Implemented Architecture: LLM Integration (Feb 9, 2026)

```
User NL Question
       |
       v
[llm_adapter.py] -----> Ollama available? -----> [schema_retriever.py]
       |                       |                    BM25 retrieval of relevant
       |                       |                    schema chunks (top-K tables)
       |                       v                         |
       |            [Ollama qwen3:14b]              <---+
       |            System prompt: metrics, dims,
       |            synonyms, few-shot examples,
       |            retrieved schema context
       |                       |
       |                       v
       |            Spec JSON (validated against registry)
       |                       |
       +---- Ollama down? ---->+ (fallback)
       |                       |
       v                       v
[nl_to_spec.py]          Validated Spec
  (rule-based)                 |
       |                       v
       +-----> [spec_executor.py] -- Deterministic SQL generation
                      |
                      v
               T-SQL Output
               (or AmbiguousDimensionError -> LLM disambiguation -> retry)
```

**Key principle:** The LLM NEVER generates SQL. It produces a structured Spec (metrics, dimensions, filters, date range). Python code generates all SQL deterministically. This prevents hallucination while leveraging the LLM for intent understanding and disambiguation.

**Fallback:** If the LLM is unavailable, `nl_to_spec.py` (rule-based parser) handles the request seamlessly. The `notes.parser` field in the Spec indicates which path was used ("llm" or "rule_based").

**Configuration (env vars):**
| Variable | Default | Description |
|----------|---------|-------------|
| `NL_SQL_USE_LLM` | `true` | Enable/disable LLM integration |
| `OLLAMA_URL` | `http://192.168.12.51:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `qwen3:14b` | Model to use for NL parsing |
| `NL_SQL_CHUNKS_DIR` | (empty) | Path to semantic_chunks JSONL files for retriever |

---

## Future Optimization Ideas

- [ ] Embed-based semantic search for metric/column matching (hybrid_retriever.py foundation exists)
- [ ] Query result caching with TTL for repeated questions
- [ ] Schema change detection and auto-reindex
- [ ] Query history tracking for pattern learning
- [ ] User feedback loop (thumbs up/down on generated SQL)
- [ ] Window function support (RANK, ROW_NUMBER, LAG/LEAD)
- [ ] CTE generation for complex multi-step queries
- [ ] Query cost estimation before execution
- [ ] Auto-suggested indexes based on generated queries
- [ ] Streaming response for long-running queries

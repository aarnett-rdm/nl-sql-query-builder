# NL SQL Query Builder

Natural language to SQL query builder with LLM integration, built for querying Microsoft Fabric Data Warehouse.

## 🚀 Quick Start

**For Users:** See [GETTING_STARTED.md](GETTING_STARTED.md) for complete setup instructions.

**TL;DR:**
```bash
git clone https://github.com/aarnett-rdm/nl-sql-query-builder.git
cd nl-sql-query-builder
pip install -r physical_schema/requirements.txt -r physical_schema/ui/requirements.txt
# Then run: start_app.bat (Windows)
```

## 🎯 What This Does

Ask questions in plain English, get SQL queries and results from your data warehouse:

- **"Show me total revenue for last week"**
- **"What were clicks and conversions by campaign last month?"**
- **"Compare cost per click for Google Ads vs Microsoft Ads this year"**

## ✨ Features

- 🤖 **LLM-Powered**: Uses Ollama (qwen3:14b) to understand natural language
- 📊 **Interactive Visualizations**: Auto-generated Plotly charts (line, bar, area, etc.) with export to PNG/HTML
- 📈 **Visual Reports Builder**: Custom chart builder with templates, save/load configs, and multi-metric support
- 📊 **Multi-Date Reporting**: Compare metrics across multiple date ranges with visualization
- 🔍 **Smart Disambiguation**: Asks clarifying questions when needed
- 🎨 **Streamlit UI**: Clean chat interface with direct query execution
- 🔐 **Fabric Authentication**: Seamless integration with Microsoft Fabric DW
- 🧪 **Comprehensive Testing**: 260+ tests covering all major functionality
- 📝 **Feedback System**: Thumbs up/down, correction forms, pattern analysis, auto-improvement loop
- 📊 **Admin Dashboard**: Track feedback patterns, download analysis, monitor improvements

## 🏗️ Architecture

```
User Question (NL)
    ↓
LLM Adapter (llm_adapter.py) → Spec (structured query intent)
    ↓
Spec Executor (spec_executor.py) → Query Plan
    ↓
Query Builder (query_builder.py) → SQL
    ↓
Fabric Connection → Results
```

**Key Components:**
- **API**: FastAPI backend (`physical_schema/api/app.py`)
- **UI**: Streamlit chat app (`physical_schema/ui/Query Builder.py`)
- **LLM**: Ollama integration with fallback to rule-based parser
- **Tests**: 260 tests across 18 test files
- **Docs**: Complete user guides and cheat sheets

## 📚 Documentation

- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Complete setup guide for users
- **[GIT_CHEAT_SHEET.md](GIT_CHEAT_SHEET.md)** - Git basics (clone, pull, status)
- **[physical_schema/spec.md](physical_schema/spec.md)** - Spec format reference
- **[TODO.md](TODO.md)** - Project roadmap and priorities
- **[PROGRESS.md](PROGRESS.md)** - Development history

## 🛠️ For Developers

### Project Structure

```
physical_schema/
├── api/                    # FastAPI backend
│   └── app.py             # Main API with /query and /feedback endpoints
├── ui/                     # Streamlit frontend
│   ├── Query Builder.py   # Chat interface with auto-visualization
│   ├── pages/
│   │   ├── Multi Date Reporting.py      # Multi-date comparison with charts
│   │   ├── Visual_Reports.py            # Custom chart builder
│   │   └── Feedback_Dashboard.py        # Admin feedback dashboard
│   ├── shared.py          # Shared UI utilities (format_results, Fabric sidebar)
│   └── viz_utils.py       # Visualization utilities (Plotly chart generation)
├── tools/                  # Core logic
│   ├── llm_adapter.py     # LLM integration (Ollama)
│   ├── spec_executor.py   # Spec → Query plan
│   ├── query_builder.py   # Query plan → SQL
│   ├── metric_resolver.py # Metric registry & multi-fact CTE
│   ├── join_planner.py    # Dijkstra join path finding
│   ├── fabric_conn.py     # Fabric DW connection (pyodbc + azure-identity)
│   ├── feedback_store.py  # Feedback storage (JSONL)
│   ├── feedback_analyzer.py # Pattern detection & recommendations
│   └── ...
├── tests/                  # 260+ tests (pytest)
│   └── test_viz_utils.py  # 21 visualization tests
├── current/                # Config files
│   ├── physical_schema.json
│   ├── metric_registry.json
│   └── filter_config.json
├── prompts/                # LLM prompts
│   ├── system_prompt.txt
│   ├── few_shot_examples.json
│   └── disambiguation_prompt.txt
├── evals/                  # Evaluation framework
│   └── eval_dataset.json
└── feedback/               # User feedback storage
    ├── corrections.jsonl
    ├── FEEDBACK_LOG.md
    └── RECOMMENDATIONS.md
```

### Running Tests

```bash
cd physical_schema
pytest tests/                          # All tests (260)
pytest tests/test_golden_queries.py    # Golden queries only
pytest tests/test_llm_parity.py        # LLM parity tests (needs Ollama)
```

### Running the API

```bash
cd physical_schema
python -m uvicorn api.app:app --reload --port 8000
```

### Running the UI

```bash
cd physical_schema
streamlit run ui/Query\ Builder.py --server.port 8501
```

Or just use: **`start_app.bat`** (auto-updates and launches everything)

## 🧪 Evaluation & Feedback

### Evaluation Harness

```bash
cd physical_schema
python tools/eval_harness.py --tags parity --cost-input 0.001
```

Outputs: `evals/run_YYYY-MM-DDTHH-MM-SS.json` with accuracy scores

### Feedback Analysis

```bash
cd physical_schema
python tools/feedback_analyzer.py --min-count 2
```

Generates: `feedback/RECOMMENDATIONS.md` with improvement suggestions

## 🔧 Configuration

Environment variables (create `.env` in `physical_schema/`):

```bash
# LLM
NL_SQL_LLM_PROVIDER=ollama
NL_SQL_LLM_BASE_URL=http://192.168.12.51:11434
NL_SQL_LLM_MODEL=qwen3:14b
NL_SQL_LLM_TIMEOUT=60

# Fabric DW
FABRIC_SERVER=your-workspace.datawarehouse.fabric.microsoft.com
FABRIC_DATABASE=RDMWarehouse
FABRIC_DRIVER=ODBC Driver 18 for SQL Server
FABRIC_ROW_LIMIT=10000

# Eval costs (optional)
NL_SQL_COST_PER_INPUT_TOKEN=0.0
NL_SQL_COST_PER_OUTPUT_TOKEN=0.0
```

See [physical_schema/.env.example](physical_schema/.env.example) for full list.

## 🐳 Docker Deployment

```bash
cd physical_schema
docker-compose up -d
```

Access API at: `http://localhost:8000`

## 📊 Supported Metrics

- **Base**: impressions, clicks, cost, revenue, conversions, profit
- **Derived**: CTR, conversion rate, CPC, CPM, ROI, revenue per click/conversion

**Platforms**: Google Ads, Microsoft Ads
**Grains**: campaign_calendar, adgroup_calendar

See [physical_schema/current/metric_registry.json](physical_schema/current/metric_registry.json) for full list.

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and test: `pytest tests/`
3. Commit: `git commit -m "Add your feature"`
4. Push: `git push origin feature/your-feature`
5. Open a Pull Request

## 📝 License

Internal tool for Red Dog Media Inc.

## 💬 Support

- **Issues**: https://github.com/aarnett-rdm/nl-sql-query-builder/issues
- **Contact**: Andrew Arnett (aarnett@reddogmediainc.com)
- **Teams**: @aarnett

## 🎓 Learning Resources

New to Git? Check out [GIT_CHEAT_SHEET.md](GIT_CHEAT_SHEET.md) - it covers just the 3 commands you need!

## 🔄 Feedback & Continuous Improvement

### User Feedback
After each query, users can:
- Click 👍 if the query is correct
- Click 👎 to report issues with correction details
- System stores feedback for pattern analysis

### Admin Dashboard
Access via Streamlit sidebar → **Feedback Dashboard**:
- Summary statistics and trends
- Top issues by category
- Recent feedback viewer
- Download analysis reports

### Automated Improvement Loop
1. Users submit feedback via UI
2. System detects patterns (metric synonyms, dimension preferences, etc.)
3. Admin downloads `RECOMMENDATIONS.md` or `FEEDBACK_LOG.md`
4. Upload to Claude Code for automated fixes
5. Claude implements improvements and pushes to GitHub
6. Users get updates automatically via `git pull`

**Files:**
- `feedback/corrections.jsonl` - Raw feedback data
- `feedback/FEEDBACK_LOG.md` - Human-readable log for review
- `feedback/RECOMMENDATIONS.md` - Analyzed patterns with actionable fixes

## 🚦 Status

- ✅ **Core**: LLM integration, multi-fact queries, disambiguation
- ✅ **UI**: Chat interface, multi-date reporting, direct execution, feedback system
- ✅ **Visualizations**: Auto-charts in Query Builder, chart builder page, 7 chart types, PNG/HTML export
- ✅ **Testing**: 260+ tests, evaluation harness, feedback loop
- ✅ **Docs**: User guides, API docs, developer docs
- ✅ **Feedback**: Complete loop with UI, dashboard, and markdown export
- 🚧 **Next**: LLM-driven visualization requests, department rollout

---

**Built with**: Python, FastAPI, Streamlit, Ollama, Microsoft Fabric

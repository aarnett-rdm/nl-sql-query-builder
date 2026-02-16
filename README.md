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
- 📊 **Multi-Date Reporting**: Compare metrics across multiple date ranges
- 🔍 **Smart Disambiguation**: Asks clarifying questions when needed
- 🎨 **Streamlit UI**: Clean chat interface with direct query execution
- 🔐 **Fabric Authentication**: Seamless integration with Microsoft Fabric DW
- 🧪 **Comprehensive Testing**: 260 tests covering all major functionality
- 📝 **Feedback Loop**: Learn from corrections to improve accuracy

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
│   ├── Query Builder.py   # Chat interface
│   ├── pages/
│   │   └── Multi Date Reporting.py
│   └── shared.py          # Shared UI utilities
├── tools/                  # Core logic
│   ├── llm_adapter.py     # LLM integration (Ollama)
│   ├── spec_executor.py   # Spec → Query plan
│   ├── query_builder.py   # Query plan → SQL
│   ├── metric_resolver.py # Metric registry & multi-fact CTE
│   ├── join_planner.py    # Dijkstra join path finding
│   └── ...
├── tests/                  # 260 tests (pytest)
├── current/                # Config files
│   ├── physical_schema.json
│   ├── metric_registry.json
│   └── filter_config.json
├── prompts/                # LLM prompts
│   ├── system_prompt.txt
│   ├── few_shot_examples.json
│   └── disambiguation_prompt.txt
└── evals/                  # Evaluation framework
    └── eval_dataset.json
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

## 🚦 Status

- ✅ **Core**: LLM integration, multi-fact queries, disambiguation
- ✅ **UI**: Chat interface, multi-date reporting, direct execution
- ✅ **Testing**: 260 tests, evaluation harness, feedback loop
- ✅ **Docs**: User guides, API docs, developer docs
- 🚧 **Next**: Department rollout and stress testing

---

**Built with**: Python, FastAPI, Streamlit, Ollama, Microsoft Fabric

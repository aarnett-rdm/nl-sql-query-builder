# Natural Language SQL Query Builder - Project Brief

## Project Overview
A natural language to SQL query builder for business users in the tickets department to query marketing and conversion data from Microsoft Fabric (T-SQL) without needing to know SQL. The system parses natural language requests, understands the schema, and generates accurate T-SQL queries for complex data analysis.

---

## Current State Assessment

### What's Working ✅
- **Schema Parsing:** Successfully parses physical_schema.json with detailed table/column information
- **JOIN Logic:** Table relationship detection and JOIN generation works well
- **Basic Query Generation:** Can generate simple SELECT queries
- **Module Structure:** Has organized Python modules for different responsibilities:
  - `orchestrator.py` - Coordinates the query building process
  - `planner.py` - Plans query structure
  - `builder.py` / `sql_builder.py` - Constructs SQL queries
  - `validator.py` - Validates queries
  - `schema_index.py` - Indexes and searches schema
  - `binding.py` - Binds natural language to schema elements
  - `grain.py` - Determines query granularity
  - `normalize_term.py` - Normalizes terminology
  - `sanitize.py` - Input sanitization
  - `spec_builder.py` - Builds query specifications
  - `build_physical_plan_for_question.py` - Creates execution plan
  - `derived.py` - Handles derived/calculated fields
  - `io.py` - Input/output handling
  - `registry.py` - Component registry

### Critical Issues 🔴
1. **WHERE Clause Filtering Breakdown:**
   - System cannot correctly identify which columns to filter on
   - Fails to extract proper filter conditions from natural language
   - Example: "Show me orders from last week" - can't properly build the WHERE clause

2. **Column Ambiguity Problems:**
   - Similar column names cause incorrect selection
   - **Critical Example:** Confuses "revenue" with "exchange_revenue" (very different metrics)
   - No disambiguation logic when multiple similar columns exist

3. **Edge Cases with Complex Queries:**
   - As query complexity increases (multiple filters, aggregations, conditions), accuracy degrades
   - System likely built for happy path scenarios, fails on real-world complexity

4. **Missing LLM Integration:**
   - Currently uses rule-based logic only
   - No natural language understanding from an actual language model
   - High risk of pattern-matching failures

### Project Abandoned Point
- Last worked on: ~2 months ago (December 2025)
- Stopped due to: Edge cases in WHERE clause logic + inability to integrate LLM
- Warning from ChatGPT: Template-based LLM approach would cause hallucinations

---

## Technical Architecture

### Current Tech Stack
- **Language:** Python
- **Database:** Microsoft Fabric (T-SQL)
- **Schema:** JSON-based (physical_schema.json)
- **Interface:** Web-based (framework TBD from code review)
- **LLM (Planned):** Start with Ollama (open source), migrate to paid API based on value

### Schema Details
- **Size:** ~200 tables
- **Domain:** Marketing analytics data
  - Google Ads platform data
  - Microsoft Ads platform data  
  - In-house conversion tracking
  - Order values and revenue metrics
- **Schema Structure (from screenshot):**
  - Version tracking
  - Source CSV references (tables.csv, columns.csv, constraints.csv)
  - Summary statistics (table counts, column counts, primary keys)
  - Detailed table definitions with:
    - Schema and table names
    - Column metadata (data_type, nullable, precision, scale, max_length)
    - Data type information (datetime2, varchar, bigint, etc.)

### Current Query Flow (Approximate)
1. User inputs natural language query via web interface
2. System breaks question apart using multiple modules:
   - Identifies required columns
   - Determines aggregations needed
   - Extracts filter criteria (WHERE clauses)
   - Plans JOIN strategy
3. Set logic builds SQL query
4. Returns query or asks clarifying questions

---

## Target User Experience (Not Yet Achieved)

### Ideal Workflow
1. **User fills out structured form** with:
   - Columns/metrics they want to see
   - Filter criteria (e.g., date ranges, campaign names, status)
   - Grouping/aggregation preferences
   - Any special conditions

2. **LLM processes the request:**
   - Understands intent from form + any free-text
   - Maps user's language to actual schema columns
   - Handles ambiguity (asks: "Did you mean 'exchange_revenue' or 'gross_revenue'?")
   - Understands relationships between tables

3. **System generates T-SQL query:**
   - Accurate column selection
   - Proper JOINs based on relationships
   - Correct WHERE clauses
   - Appropriate aggregations and GROUP BY
   - Handles complex queries (CTEs, subqueries, window functions)

4. **If unclear, asks clarifying questions:**
   - "I found two 'revenue' columns - which one do you need?"
   - "What date range should I use?"
   - "Should this be aggregated by day, week, or month?"

### Current Reality
- Uses natural language input (not structured form)
- No LLM understanding
- Fails on WHERE clause complexity
- Mixes up similar column names
- No effective disambiguation

---

## Project Goals

### Primary Objective
Create a reliable SQL query generator that business users (tickets department) can use to analyze marketing data without SQL knowledge.

### Success Criteria
- **Accuracy:** Consistently generates correct queries for complex scenarios
- **Disambiguation:** Properly handles similar column names (revenue vs. exchange_revenue)
- **Complex Queries:** Supports:
  - Multiple JOINs across related tables
  - CTEs (Common Table Expressions)
  - Aggregations (SUM, COUNT, AVG, etc.)
  - WHERE clauses with AND/OR logic
  - Date filtering and comparisons
  - Window functions if needed
- **Multi-User Ready:** API-based, can be used by entire department
- **Reliable:** Production-quality code with error handling

### Definition of "Done"
A product that multiple ICs (Individual Contributors) can use that consistently provides accurate T-SQL queries.

---

## LLM Integration Strategy

### Phase 1: Ollama (Proof of Concept)
- **Why:** Open source, free, test feasibility
- **Goal:** Prove that LLM-enhanced approach works better than pure rule-based
- **Risk:** May need local hosting or small-scale API

### Phase 2: Production LLM (Based on Value)
- **Options:** OpenAI, Anthropic Claude, Azure OpenAI (Fabric integration?)
- **Deployment:** API-based, not local machine
- **Budget:** Determined by proven value from Phase 1

### Recommended LLM Architecture
**Avoid:** Pure template filling (risk of hallucination as ChatGPT warned)

**Instead, use LLM for:**

1. **Intent Understanding:**
   - Parse natural language to understand what user wants
   - Extract entities (metrics, dimensions, filters, date ranges)
   - Map user terms to schema vocabulary

2. **Schema Navigation:**
   - Given user's request, identify relevant tables
   - Determine which columns are needed
   - Resolve ambiguous terms using context

3. **Disambiguation Agent:**
   - When column names are similar, ask clarifying questions
   - Present options to user: "Did you mean X or Y?"
   - Learn from user selections

4. **Query Planning (Not Generation):**
   - LLM creates a query PLAN (structured specification)
   - Python code generates actual SQL from the plan
   - This prevents SQL hallucination while leveraging LLM's understanding

**Architecture Flow:**
```
Natural Language Input
    ↓
LLM: Parse intent, identify schema elements, resolve ambiguity
    ↓
Structured Query Specification (JSON)
    ↓
Python SQL Builder: Generate actual T-SQL
    ↓
Validator: Check syntax and logic
    ↓
Return to User (or ask clarifying questions)
```

---

## Known Issues to Fix

### Critical (Blocking)
1. **WHERE Clause Logic:**
   - Cannot properly extract filter conditions from natural language
   - Fails to map conditions to correct columns
   - Needs complete rewrite or LLM assistance

2. **Column Disambiguation:**
   - No logic to handle similar names (revenue vs. exchange_revenue)
   - Must implement:
     - Contextual understanding
     - User clarification prompts
     - Column description/metadata usage

### High Priority
3. **Edge Case Handling:**
   - System degrades with query complexity
   - Needs robust error handling
   - Should handle:
     - Ambiguous requests
     - Missing information
     - Invalid combinations

4. **Code Organization:**
   - Many modules built iteratively across ChatGPT sessions
   - Likely has redundant or conflicting logic
   - Needs architectural review and cleanup

### Medium Priority
5. **Schema Index Optimization:**
   - 200+ tables means search performance matters
   - Need efficient column lookup
   - Consider fuzzy matching for user terms

6. **Testing:**
   - No indication of test coverage
   - Need test cases for common query patterns
   - Edge case test suite

### Nice-to-Have
7. **Query History:**
   - Track what queries users run
   - Learn common patterns
   - Suggest similar queries

8. **Performance Optimization:**
   - Query generation speed
   - Suggest indexes or optimizations

---

## Immediate Action Items

### Step 1: Code Audit & Cleanup
- Review all existing modules for:
  - Redundant logic
  - Conflicting approaches
  - Dead code from iterative ChatGPT development
- Consolidate and refactor
- Add documentation

### Step 2: Fix WHERE Clause Generation
- Identify why current logic fails
- Implement proper filter extraction
- Test with various filter scenarios

### Step 3: Implement Column Disambiguation
- Add metadata/descriptions to schema if missing
- Build disambiguation logic
- Create user prompt system for clarification

### Step 4: LLM Integration (Ollama)
- Set up Ollama locally for testing
- Implement intent parsing
- Test with real queries from tickets department

### Step 5: Build Query Planner
- LLM outputs structured query specification
- Python builder converts spec to T-SQL
- Separate understanding from generation

### Step 6: Testing & Validation
- Create test suite with real department queries
- Measure accuracy before/after LLM
- Get user feedback

---

## Development Phases

### Phase 1: Stabilize & Clean (Week 1-2)
- Audit existing code
- Fix architecture issues
- Document current state
- Create comprehensive test cases

### Phase 2: Fix Critical Issues (Week 2-3)
- Repair WHERE clause logic
- Implement column disambiguation
- Handle edge cases better

### Phase 3: LLM Integration - Proof of Concept (Week 3-4)
- Integrate Ollama
- Build intent parser
- Create structured query planner
- Test with real queries

### Phase 4: Polish & Production Ready (Week 4-5)
- Error handling
- User experience improvements
- Performance optimization
- API packaging for department use

### Phase 5: Production LLM Migration (Week 6+)
- Evaluate performance vs. cost
- Migrate to production LLM API
- Deploy for department use
- Gather feedback and iterate

---

## Testing Strategy

### Test Query Categories
1. **Simple SELECT:**
   - "Show me all campaigns"
   - "Get order totals by day"

2. **Filtered Queries:**
   - "Show campaigns from last week"
   - "Orders where revenue > 1000"
   - "Active campaigns in California"

3. **Aggregations:**
   - "Total revenue by campaign"
   - "Average order value per day"
   - "Count of conversions by source"

4. **Complex Queries:**
   - Multiple JOINs with filters
   - CTEs for intermediate calculations
   - Date range comparisons
   - Multiple aggregations

5. **Ambiguous Queries (Test Disambiguation):**
   - "Show me revenue" (which revenue?)
   - "Campaign performance" (which metrics?)
   - "Recent orders" (how recent?)

### Success Metrics
- Accuracy rate: >90% correct queries
- Disambiguation rate: Successfully asks for clarification when needed
- Complexity handling: Can handle 3+ table JOINs
- User satisfaction: Tickets department feedback

---

## Risk Mitigation

### Risk: LLM Hallucination
- **Mitigation:** LLM plans queries, doesn't write SQL directly
- Python code generates actual SQL from structured plan
- Validation step catches syntax errors

### Risk: Column Ambiguity
- **Mitigation:** Always ask for clarification when uncertain
- Use column descriptions/metadata
- Learn from user corrections

### Risk: Performance Issues
- **Mitigation:** Schema indexing for fast lookup
- Query optimization suggestions
- Limit result set sizes

### Risk: Scope Creep
- **Mitigation:** Focus on MVP - accurate query generation
- Nice-to-haves documented but postponed
- Department feedback drives prioritization

---

## Open Questions

1. **Web Framework:** What's the current web interface built with? (Flask, Django, FastAPI?)
2. **Authentication:** How are users authenticated? Department-wide access control?
3. **Query Execution:** Does system execute queries or just generate SQL?
4. **Result Handling:** How are results displayed to users?
5. **Schema Updates:** How often does the schema change? Auto-refresh needed?
6. **Logging:** Any current logging/monitoring for debugging?
7. **Deployment:** Where will this be hosted? Cloud? On-prem?
8. **API Design:** REST API? GraphQL? Direct Python calls?

---

## Success Indicators

**Technical:**
- WHERE clause accuracy: 95%+
- Column disambiguation success: 100% (always asks when uncertain)
- Complex query support: CTEs, multiple JOINs working
- Clean, maintainable codebase

**Business:**
- Tickets department adoption
- Reduction in SQL support requests
- Positive user feedback
- Expanded to other departments

---

## Resources & References

### Existing Code Structure
```
project/
├── __init__.py
├── binding.py (6 KB)
├── build_physical_plan_for_question.py (42 KB) - Large, likely main orchestration
├── derived.py (6 KB)
├── grain.py (3 KB)
├── io.py (1 KB)
├── normalize_term.py (1 KB)
├── orchestrator.py (15 KB)
├── planner.py (14 KB)
├── registry.py (0 KB - empty?)
├── sanitize.py (16 KB)
├── schema_index.py (4 KB)
├── spec_builder.py (15 KB)
├── sql_builder.py (12 KB)
└── validator.py (10 KB)
```

### Schema File
- `physical_schema.json` - Complete schema definition with ~200 tables

### Key Files to Review First
1. `build_physical_plan_for_question.py` (42 KB) - Likely main logic
2. `orchestrator.py` (15 KB) - Coordination
3. `planner.py` (14 KB) - Query planning
4. `sql_builder.py` (12 KB) - SQL generation
5. `binding.py` (6 KB) - Natural language to schema mapping

---

## Next Steps with Claude Code

### Initial Prompt for Claude Code:

"I have a half-completed natural language SQL query builder for business users. It's built in Python and targets Microsoft Fabric (T-SQL). The system has about 15 Python modules but has critical issues with WHERE clause generation and column disambiguation that prevented me from finishing it.

**Please do the following:**

1. **Code Audit:** Review all Python modules and tell me:
   - What each module does
   - How they work together
   - What's working vs. broken
   - Redundant or conflicting logic
   - Code quality issues

2. **Identify Root Causes:** Why are WHERE clauses failing? Why does column disambiguation not work?

3. **Create TODO.md:** 
   - Prioritized list of fixes needed
   - LLM integration plan (Ollama → production)
   - Testing strategy
   - Path to production-ready state

4. **Create PROGRESS.md:**
   - Document current state
   - List what's working/broken
   - Track technical decisions
   - Space for future optimization ideas

5. **Recommend Architecture:** 
   - Best way to integrate Ollama for intent understanding
   - How to avoid SQL hallucination (hint: LLM plans, Python generates)
   - Improved WHERE clause logic
   - Column disambiguation approach

After your analysis, I'll want to systematically fix the issues and get this to a production-ready state for my department."

---

**Project Owner:** [Your Name/Department]  
**Created:** February 6, 2026  
**Last Code Update:** December 18, 2025  
**Status:** Needs Rescue & Completion  
**Priority:** High - Department Value Identified

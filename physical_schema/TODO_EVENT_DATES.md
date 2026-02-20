# TODO: Event Date Functionality - Ending Events Tab

## Status: ✅ COMPLETE (Feb 20, 2026)

The Ending Events UI tab is now fully functional. Event date filtering works correctly.

## What Works ✅

1. **LLM Event Date Understanding**
   - EventDate dimension registered in `tools/llm_adapter.py` (line 82)
   - System prompt documentation in `prompts/system_prompt.txt` (after line 36)
   - Few-shot examples in `prompts/few_shot_examples.json` (lines 224-264)
   - LLM can recognize: "next event", "last event", "events ending", "upcoming events"

2. **EventDate Virtual Dimension Handler**
   - Implemented in `tools/query_builder.py` (lines 254-266)
   - Maps EventDate → Event.EventDateTimeLocal
   - Casts to DATE for grouping

3. **System-Wide Bronze Preference**
   - `tools/join_planner.py` (lines 143-159)
   - Prefers: CoreEntity > other schemas > Bronze (last resort)
   - Resolves table ambiguity across entire system

4. **Foreign Key Metadata**
   - Added in `current/physical_schema.json` (lines 15387-15416)
   - `GoogleAdsCampaignEventMap.EventId → Event.EventId`
   - `MicrosoftAdsCampaignEventMap.EventId → Event.EventId`

5. **campaign_calendar Grain Enhancement**
   - `tools/join_planner.py` (lines 488-506)
   - Adds Event + CampaignEventMap to default_targets
   - Provides complete join path: Campaign → CampaignEventMap → Event

6. **Ending Events UI Page**
   - Created at `ui/pages/Ending_Events.py`
   - Full UI with filters, charts, exports
   - Date range selector (±30/60 days default)
   - Platform, Account, Category, Campaign Status filters

## What Was Broken (FIXED) ✅

### Primary Issue: KeyError on Event Table

**Error**: `KeyError: 'GoTicketsCoreEntity.Event'`

**Location**: `tools/query_builder.py` line 423

**Root Cause Found**: The join planner's `neighbors()` function prioritizes single-PK tables over composite-PK mapping tables. When processing `CampaignId` column from `GoogleAdsCampaign`, it found 8 tables with `CampaignId` as a single-column PK, created edges to those, and executed `continue` (line 296 in join_planner.py), which SKIPPED creating edges to `GoogleAdsCampaignEventMap` (composite PK).

**Solution Applied**: Added bidirectional foreign key metadata in `current/physical_schema.json`:
- `GoogleAdsCampaign → GoogleAdsCampaignEventMap` (CampaignId)
- `MicrosoftAdsCampaign → MicrosoftAdsCampaignEventMap` (CampaignId)

These explicit FKs are loaded into `seed_from` edges, bypassing the inference logic that was blocking the mapping table.

**SQL Now Generated Successfully**:
```sql
SELECT
  t2.[CampaignName] AS [CampaignName],
  SUM(fact.[Clicks]) AS [clicks],
  SUM(fact.[Cost]) AS [cost],
  SUM(fact.[ProcessedConversions]) AS [conversions]
FROM [GoTicketsPerformanceMetric].[GoogleAdsCampaignPerformanceMetric] AS fact
LEFT JOIN [GoTicketsCoreEntity].[GoogleAdsCampaign] AS t2 ON fact.[CampaignId] = t2.[CampaignId]
LEFT JOIN [GoTicketsEntityMap].[GoogleAdsCampaignEventMap] AS t3 ON t2.[CampaignId] = t3.[CampaignId]
LEFT JOIN [GoTicketsCoreEntity].[Event] AS t4 ON t3.[EventId] = t4.[EventId]
WHERE
  t4.[EventDateTimeLocal] >= '2026-02-15' AND t4.[EventDateTimeLocal] <= '2026-04-21'
GROUP BY
  t2.[CampaignName]
```

## Investigation Completed ✅

### 1. Join Planner Alias Generation
**Question**: Why doesn't Event appear in aliases even though it's in default_targets?

**Files to investigate**:
- `tools/join_planner.py` - How are aliases built?
- `tools/spec_executor.py` - How is the join plan created from the spec?

**Hypotheses**:
- Join planner can't find path from fact table to Event (despite foreign keys)
- Tables in default_targets are being filtered out somewhere
- Aliases dictionary uses different key format than expected
- Join planning happens before default_targets are considered

### 2. Fact Table for campaign_calendar Grain
**Question**: What fact table is used for campaign_calendar grain?

**Check**: `current/metric_registry.json` - grain definitions
- Verify fact table has campaign metrics
- Check if fact table links to Campaign table
- Confirm join path exists: FactTable → Campaign → CampaignEventMap → Event

### 3. Alternative Approaches
If join planner can't be fixed easily, consider:

**Option A: Use EventOrderMetric Fact Table**
- `GoTicketsOrderMetric.EventOrderMetric` has EventId directly
- Could filter on EventId from this fact table
- Would need to verify if campaign metrics exist at this grain

**Option B: Create Custom SQL Template**
- Bypass spec_executor for this specific use case
- Manually construct JOIN query in `Ending_Events.py`
- Not ideal but could be pragmatic solution

**Option C: Add Event Support to Metrics**
- Modify `current/metric_registry.json`
- Add `event_calendar` grain to campaign metrics
- Would allow direct use of event_calendar grain

**Option D: Use Subquery Approach**
- Filter campaigns by those with events in date range
- Don't join Event table directly
- Query structure: `WHERE CampaignId IN (SELECT CampaignId FROM CampaignEventMap WHERE EventId IN (SELECT EventId FROM Event WHERE ...))`

## Debugging Steps Completed ✅

Created debug scripts that revealed:
1. ✅ Event WAS in default_targets
2. ✅ Event table could be resolved
3. ✅ Foreign keys existed for CampaignEventMap → Campaign and CampaignEventMap → Event
4. ❌ But Campaign → CampaignEventMap edge was NOT being created due to single-PK preference in `neighbors()` function

The `neighbors()` function processes columns in order and when it finds 8 tables with `CampaignId` as single-column PK, it creates edges to those and `continue`s, skipping the composite-PK mapping tables entirely.

## Files Modified in This Session

1. `physical_schema/tools/llm_adapter.py` - Added EventDate dimension
2. `physical_schema/prompts/system_prompt.txt` - Documented EventDate usage
3. `physical_schema/prompts/few_shot_examples.json` - Added event query examples
4. `physical_schema/tools/query_builder.py` - EventDate virtual dimension handler
5. `physical_schema/tools/join_planner.py` - Bronze preference + Event in campaign_calendar
6. `physical_schema/current/physical_schema.json` - Foreign key metadata
7. `physical_schema/ui/pages/Ending_Events.py` - New UI page (non-functional)

## Next Session Prompt

Use this prompt to continue:
```
I'm working on adding event date filtering functionality to an NL SQL query builder. We've implemented:
- EventDate dimension in LLM
- Virtual dimension handler
- Foreign key metadata for Campaign→CampaignEventMap→Event
- Ending Events UI tab

The issue: When querying campaign_calendar grain with Event.EventDateTimeLocal filters, we get KeyError: 'GoTicketsCoreEntity.Event' because Event isn't in the aliases dictionary even though it's in default_targets.

See physical_schema/TODO_EVENT_DATES.md for full context. Need to debug why join planner isn't including Event table in aliases despite having foreign keys and default_targets configured.
```

## Success Criteria ✨

When this is working, users should be able to:
1. Navigate to "Ending Events" tab in Streamlit UI
2. Select date range (e.g., events ending in next 60 days)
3. Filter by platform, account, category, campaign status
4. See campaigns with their metrics, filtered to only those with events in date range
5. Export results to CSV/Excel
6. View charts of campaign performance

## Additional Context

- **Data Model**: Campaign → CampaignEventMap → Event
- **Event Date Field**: `GoTicketsCoreEntity.Event.EventDateTimeLocal` (datetime2)
- **Core Entity Preference**: System now always prefers CoreEntity tables over Bronze
- **Grain**: Using campaign_calendar because campaign metrics only support this grain
- **Alternative**: Could use event_calendar grain if we add metric support for it

---

## Resolution Summary (Feb 20, 2026)

**Problem**: Event table wasn't making it into the join plan's aliases dictionary, causing KeyError when building WHERE clauses.

**Root Cause**: Join planner's `neighbors()` function prioritizes single-PK tables over composite-PK mapping tables. When processing `GoogleAdsCampaign.CampaignId`, it found 8 single-PK tables, created edges to those, and executed `continue` at line 296, skipping `GoogleAdsCampaignEventMap` entirely.

**Solution**: Added explicit bidirectional foreign keys in `current/physical_schema.json`:
- `GoTicketsCoreEntity.GoogleAdsCampaign → GoTicketsEntityMap.GoogleAdsCampaignEventMap`
- `GoTicketsCoreEntity.MicrosoftAdsCampaign → GoTicketsEntityMap.MicrosoftAdsCampaignEventMap`

These FK edges are loaded into `seed_from` dictionary before inference logic runs, ensuring the Campaign → EventMap path is always available.

**Files Changed**:
1. `current/physical_schema.json` - Added 2 bidirectional FK relationships

**Result**: Event date filtering now works end-to-end. The Ending Events UI tab is fully functional.

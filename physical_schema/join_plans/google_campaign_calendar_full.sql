SELECT
  SUM(fact.[Clicks]) AS [clicks],
  SUM(fact.[Impressions]) AS [impressions],
  SUM(fact.[Cost]) AS [cost]
FROM [GoTicketsPerformanceMetric].[GoogleAdsCampaignPerformanceMetric] AS fact
LEFT JOIN [Utility].[DimCalendar] AS t1 ON fact.[CalendarId] = t1.[CalendarId]
LEFT JOIN [GoTicketsCoreEntity].[GoogleAdsCampaign] AS t2 ON fact.[CampaignId] = t2.[CampaignId]

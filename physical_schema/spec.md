# NL→Spec Contract (LOCKED)

This contract is the sole interface between NL/UI and the query engine.

## Top-level fields

- grain: string | null
- platform: "google_ads" | "microsoft_ads" | null
- metrics: string[]
- dimensions: string[]
- filters:
  - date: { date_from?, date_to?, last_n_days?, yesterday?, mtd?, offset_days? }
  - campaign?: { terms: string[], mode: "all"|"any" }
  - campaign_ids?: int[]
  - where: list[{ field, op, value, case_insensitive? }]
- compare: null | {
    type: "period_over_period" | "cross_platform",
    ...
  }
- post: { rank_segments?: [...] }
- clarifications: list[{ field, reason, question, choices? }]

## Invariants

- If compare.type == "cross_platform", compare.metrics MUST exist.
- campaign_ids MUST be numeric.
- Relative date modes are mutually exclusive (last_n_days/yesterday/mtd).
- Unknown fields must be ignored safely by engine.

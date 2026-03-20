# Coverage Gap Prioritization Guide

Use this rubric for each uncovered area.

Score = (Criticality × 2) + CoverageDebt + (Volatility × 0.5)

- Criticality:
  - 5: authz/authn, data-loss, payment/consistency path
  - 4: state mutation, cache invalidation, scheduling
  - 3: error handling + fallbacks in user-visible flows
  - 2: parsing/format conversion paths
  - 1: logging-only or low-impact utilities

- CoverageDebt:
  - 0: 0–5 uncovered lines
  - 1: 6–20 uncovered lines
  - 2: 21–40 uncovered lines
  - 3: 41+ uncovered lines

- Volatility:
  - 1: stable legacy code with few recent edits
  - 2: changed in last 2 releases
  - 3: touched in last 30 days or currently in active PR

Sort by score descending, then by business impact.

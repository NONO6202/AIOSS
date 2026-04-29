# Redaction Manifest

- Source project structure was preserved for public review.
- Runtime caches, outputs, spreadsheets, databases, manuals, frontend/backend builds, tests, and local metadata were excluded.
- Literal strings were replaced with `TXT_REDACTED`.
- External URLs and source endpoints were replaced with `SRC_REDACTED`.
- Numeric constants, thresholds, dates, percentages, sample sizes, and score-like values were normalized to the placeholder range `1`, `2`, `3`, `4`.
- Letter-style rating labels were normalized to numeric placeholders in the same `1`-`4` range.
- Comments were reduced to `# REDACTED` to avoid leaking formula intent or source details.
- This folder is not intended to run as production code; it is evidence of module boundaries and implementation scale only.

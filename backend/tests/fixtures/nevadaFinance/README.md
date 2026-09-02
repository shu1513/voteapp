# Nevada AURORA fixtures (Phase 0, harvested live 2026-08-26)

Raw material for the Nevada campaign-finance adapter
(`docs/plans/nevada-finance.md`); source facts and probe history in
`backend/docs/nevada-campaign-finance.md`. Everything here was captured from
the public AURORA search (`nvsos.gov/SOSCandidateServices/AnonymousAccess/`)
via browser page-context fetch (the host's Imperva WAF blocks non-browser
clients).

Fixture candidates (2026, NV SOS jurisdiction): Joseph Lombardo (Governor;
Legal Defense Fund filer), Nicole Jeanette Cannizzaro (Attorney General
candidacy, current State Senate D6 — office-transition case), Alexis M Hansen
(State Assembly D32; amended CE#4-2024 pair), Lisa Krasner (State Senate D16;
itemizes ≤$100 transactions), Douglas W Herndon (Supreme Court Seat D;
zero-contribution case).

| file | what |
|---|---|
| `candidate-details-2026.json` | profile fields + selected report-list rows (Report Name / Year / File Date / Office / syn token) |
| `report-summaries.json` | parsed summary lines 1–13 (period + cumulative) for 13 reports |
| `reconciliation-q2-2026.json` | CSV-vs-summary reconciliation results, additivity checks, annual/amended semantics |
| `summary-table-herndon-ce2-2026.html` | real summary-table markup for parser tests |
| `contributions-hansen-q2-2026.csv` etc. | complete per-candidate Q2-2026 CSV exports (Hansen both kinds, Herndon both kinds incl. header-only empty export) |
| `statewide-june-2026-*-sample.csv` | first 40 / 27 rows of the date-only statewide June 2026 exports |
| `statewide-june-2026-metadata.json` | row counts, dollar sums, sha256, byte sizes of the FULL June exports |

Key semantics these fixtures prove (details in `reconciliation-q2-2026.json`):

1. "This Period" columns are additive; the cumulative chain restarts at CE#1
   each election year. Annual CE Filings are self-contained (period ==
   cumulative) and sit OUTSIDE the CE#1–4 chain.
2. Cycle totals = Σ period lines 8/12 over (annual filings + CE reports) in
   the window, latest effective version per period; cash on hand = line 13 of
   the report covering the latest period end (latest version of that period —
   not the newest file date; a late amendment to an old quarter can be the
   newest filing). Line 8 is the official contributions total and can contain
   loan money (summary lines 2/3, plus loan-flagged rows inside line 1; the
   CSV has no loan flag) — all five fixtures have lines 2/3 = 0, so no loan
   fixture exists yet (Phase-2 gate).
3. Itemized CSV Σ ∈ [lines 1+5, lines 1+5+7] — filers may itemize ≤$100 rows
   (Krasner). Never use CSV sums as headline totals.
4. `(Legal Defense Fund)` reports have their own summaries; exclude both the
   reports and the tagged CSV rows from campaign totals.
5. Amended reports replace originals in transaction searches; both documents
   stay in the report list — pick per logical report (name + Year) by newest
   file date.
6. Use a real CSV parser (doubled-quote escapes: `"""Anedot"""`); duplicate
   identical rows are legitimate; refunds appear as `(REFUND)` expense rows.

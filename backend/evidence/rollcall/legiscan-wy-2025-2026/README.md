# What is in this directory

- `PLAN.md` — the survey, the measured pool, the audits and the crosswalk.
- `JUDGING.md` — how each batch-01 measure was judged and why the drops were dropped. Each later batch has its own JUDGING.md inside its directory.
- `divided-enacted-worklist.tsv` — all 126 closely divided votes on measures that became law, each with a disposition and a column saying whether it is that chamber's final vote on the measure.
- `survey/survey-2157.json`, `survey/survey-2213.json` — the written survey reports the configuration was read from.
- `crosswalk-2157.json`, `crosswalk-2213.json` — the committed member crosswalks, one entry per seated member.
- `legiscan-people-wy-2157.json`, `legiscan-people-wy-2213.json` — the people snapshots the crosswalks were written against.
- `batch-01/judgments.json`, `batch-02/judgments.json` — the judgments as applied.
- `batch-01/s2157`, `batch-01/s2213` — the per-roll evidence files and the import ledgers for each session.
- `wy_text.py` — extracts an enrolled act with Wyoming's struck and underlined text labelled. Needs PyMuPDF.

The ledgers in each session directory are the dry run, the real run and the
convergence dry run, in that order. The real run for the 2025 session is
stamped `2026-09-09T23:17:47.265Z` and wrote 82 records; the 2026 session is
stamped `2026-09-09T23:17:48.708Z` and wrote 199. Both dry-run stamps match
zero rows in the database, which is the proof the dry runs were inert.

`import-rerun-report.json` in each session directory is the second real run,
after review corrected the SF 152 and SF 20 descriptions. It rewrote the 32
SF 152 records and the 33 SF 20 records in place and left the other 216
unchanged.

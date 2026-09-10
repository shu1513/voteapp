# Hawaii 2026 Regular Session (LegiScan session 2245)

See `../hawaii-2175/README.md` for the pipeline, the reasons Hawaii needs one, and the shared
crosswalk (`../hawaii-2175/crosswalk.json`, which serves both sessions because LegiScan
people_ids are stable across sessions).

- `seats.json` — the 2026 seat changes: Gene Ward / Joe Gedeon (HD-018), Daniel Holt / Michael
  Ratcliffe (HD-028, vacant February 14 to April 13, 2026), Henry Aquino / Rachele Lamosao
  (SD-019), Daisy Hartsfield (HD-036). Dates from the Governor's appointment releases and the
  chambers' own announcements.
- `hawaii-people-2245.json` — the people snapshot (79 people: the 76 of 2025 plus the three
  appointees; Lamosao appears at SD-019).
- `resolve-report.json` — the proposal run over the dataset.
- `survey/` — whole-session fetch ledger and
  `divided-enacted-worklist.tsv`.
- `batch-01/` — 6 rolls on 6 measures, all kept.

Fetch stored 1,778 floor votes (936 House / 842 Senate) and skipped 603 rows dated 2025 (votes
on carried-over bills that the 2025 dataset already holds).

Production holds zero Hawaii roll-call records.

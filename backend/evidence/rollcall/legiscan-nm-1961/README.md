# New Mexico roll-call evidence — 2022 regular session (LegiScan session 1961)

One of the nine historical New Mexico sessions. Read
[`../NEW-MEXICO-HISTORICAL.md`](../NEW-MEXICO-HISTORICAL.md) first: it explains why the state
legislature's website was unreachable for this work, what replaced it, and the tally rule that
decides which rolls are imported.

## Layout

- `crosswalk.json` — every LegiScan `people_id` in the session mapped to a VoteApp candidate, or to
  null with the reason it is null.
- `legiscan-people-nm-1961.json` — the people snapshot the resolver wrote, so the importer can run
  from committed evidence alone.
- `survey/` — the survey report for the session.
- `batch-01/` — the judged batch: judgments, one evidence file per roll, and the import ledgers.

## Status

**6 measures, 197 records, on the local database only. Production holds no New Mexico
roll-call records.**

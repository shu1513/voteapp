# integrity_and_ethics re-audit — 2026-09-13

## Why

The 2026-09-06 retag cleaned the 860 tags that existed then, but research written
since (93 tags added after 09-06) repeated the old mistake: confirmations,
appointments, committee roles, court wins, dismissed complaints, and the
candidate's own reform bills were tagged `integrity_and_ethics`, which auto-pick
counts as a strike and the ethics veto turns into an automatic No. Found on
2026-09-13 when a California retention justice (Michelle C. Kim) showed two
"Candidate Ethics" records: a unanimous confirmation and a committee vice-chair role.

## Rule applied

Same as 2026-09-06 (see `../ethics-negative-only-2026-09-06/README.md`): keep the
tag ONLY for an adverse action against the candidate personally by an official
body. Charges later dropped or dismissed are untagged with the dismissal record.
Reform work moves to `anti_corruption` with a stance.

## Result (local DB)

- 331 live tagged records reviewed one by one (329 in the first dump plus 2 written
  overnight by a concurrent research session) → 280 kept, 51 untagged.
- 51 untags across 40 candidates; 4 records tagged `anti_corruption:for`
  (2 more already carried it).
- Applied with `npm run manual:records:untag -- --untags-file untags.json --apply`
  and `npm run manual:records:tag -- --tags-file anti-corruption-adds.json --apply`
  (`DATABASE_URL` set inline).

## Files

- `untags.json` — applied manifest. Prod: re-run against prod
  (`ALLOW_REMOTE_DB_WRITES=1`, dry-run first); `research:promote` never deletes
  target-only tags.
- `anti-corruption-adds.json` — applied adds; `research:promote` carries them.
- `decisions.txt` — every reviewed record: `K` keep / `U` untag, id, candidate,
  description head. Reasons for each `U` are in `untags.json`.

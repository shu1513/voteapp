# ai_regulation label backfill — 2026-09-09

Adds the new `ai_regulation` research area (migration 276) to the roll calls
that already carried an AI-rule vote under another area. Same shape as
`nay-backfill-2026-09-06`: one judgments file across states, re-applied with
`rollcall:judge`, then each roll re-imported so `syncRollCallRecordTags`
adds the tag to every existing record. Nothing else on the rows changed:
sentences, other labels, and vote dates were copied from the database.

## Rolls re-judged (16)

| Roll | Measure | `ai_regulation` yea | Why |
|---|---|---|---|
| AZ house 1551093 | SB 1462 | for | explicit-image crime extended to AI fakes |
| CA house 1601381 / senate 1601734 | SB 524 | for | rules for AI-written police reports |
| CA senate 1602234 / house 1602570 | SB 7 | for | automated-decision rules for employers (vetoed) |
| CA senate 1602910 | SB 53 | for | frontier-AI transparency act |
| CA senate 1726291 | AB 1979 | for | health chatbots under privacy law, human clinical judgment |
| CO house 1600086 | SB 25B-004 | **against** | delays every duty of the 2024 AI Act |
| CO house 1663334 | HB 1139 | for | limits on AI insurance-coverage review |
| ID senate 1666665 | S 1297 | for | Conversational AI Safety Act |
| IL senate 1575036 | HB 1859 | for | AI cannot be the sole college instructor |
| MD house 1685440 | SB 141 | for | election deepfake crime |
| NV senate 1553538 / house 1583017 | SB 128 | for | limits on AI care denials (vetoed) |
| PA house 1709926 | HB 95 | for | AI-altered ad labels |
| US house 119-1 roll 104 | S. 146 | for | TAKE IT DOWN Act (deepfake intimate images) |

`nay` is null on every new label, matching the existing labels on the same
rolls (the earlier nay review found a no vote evidences no stance there).

## Rolls left alone (materiality rule: a topic word is not a position)

- CA AB 723 (altered listing photos; AI is one editing tool among several)
- IL SB 1920 (AI guidance for schools plus ASL materials — two subjects, guidance not rules)
- NC H 936 (robocall law; AI voice calls incidental)
- CA AB 2465, CA AB 1792, US H.R. 5764 (immigration, curriculum, AI adoption help)
- SD SB 164 (election deepfake crime — belongs here, but its evidence sits on
  the unmerged South Dakota batch; re-judge it once that PR lands)

## Import mechanics

- Each `<state>-<session>/` folder holds only the target roll files, copied
  from their original batch so the importer touched nothing else; its
  `import-dry-run-report.json` and `import-report.json` are the runs.
- CA batches 08/18/20 never committed their `ls-ca-*.json` evidence, so SB 7,
  SB 53 (dataset `ca-2172`) and AB 1979 (dataset `ca-2172-0830`) were
  re-fetched with `rollcall:legiscan:fetch`; every upsert came back
  `unchanged`, so the stored sha matched.
- `legiscan-ca-2172/crosswalk.json` and `legiscan-az-2155/crosswalk.json`
  each pointed at a candidate merged on 2026-09-09 (Matt Haney, Priya
  Sundareshan); both entries now name the surviving candidate id.
- Result: 454 `ai_regulation` tags added across the 15 state rolls, no other
  tag changed. `us-119-1/` holds the dry run only (see the PR).

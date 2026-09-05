# Kansas batch-02 — how these votes were judged

## Sources

Every enrolled act was downloaded from `kslegislature.gov` and read in full. The Legislative
Counsel material was not used as a substitute for the operative sections. Sponsor statements,
committee testimony and press material were not used at all.

Kansas enrolled acts print new language in italics and struck language in brackets or with
strikethrough. Neither italics nor strikethrough survives text extraction, so old and new
wording appear side by side as plain text. Changes were read from adjacent old/new word pairs
and from renumbering. Where that signal was absent the act's text was treated as unclear rather
than guessed at, and one measure — HB 2497 — was dropped for exactly that reason.

## Four measures are veto overrides

HB 2217, HB 2593 and SB 375 became law over the governor's veto, and their descriptions say the
House voted to override rather than saying it passed the bill. Kansas pairs a Republican
supermajority with a Democratic governor and used the override 69 times this biennium, so this
wording will recur. The tally quoted is the override vote, which is the roll being described.

## Honesty about what these acts do not do

Five of the seven carry a limit that a reader would otherwise miss, and each description states
it plainly:

- **HB 2088** — the 60-day clock covers building permits for single-family homes only, not
  zoning appeals, and no local government can opt out.
- **HB 2106** — the ban reaches constitutional amendments only, not candidate elections or
  local ballot questions.
- **HB 2217** — the office refers what it finds to prosecutors rather than charging anyone, every
  audit is its own choice, the yearly public report *loses* the provider billing and payment
  totals, and the act adds no money for the extra work.
- **HB 2304** — the bill widens one gap, sheltering deals signed before July 2025 whose contracts
  promise secrecy, and it sets no penalty for a local government that files nothing.
- **SB 375** — it bans no recommendation and requires no analysis to be done; only the attorney
  general may enforce it and an investor cannot sue.

HB 2593's description also says the description of the legal matter sent to the attorney
general is kept confidential, and that the whole section expires in July 2031.

## Checks run before importing

| check | result |
| --- | --- |
| Repository plain-language lint, 45-word sentence cap | 14 descriptions, **0 warnings** |
| Reading-level and style checker | **0 problems** |
| Flesch-Kincaid grade | median **7.8**, worst **8.6** |
| Longest sentence | 21 words |
| Banned areas | 0 used |
| Every stated tally against the stored vote row | **7 of 7 match** on chamber, measure, date and tally |
| Worklist title against the enrolled act's own title | run across all **76** measures; 1 genuine mismatch found |

## Reconciliation

Predicted independently from the crosswalk and the roll evidence before touching the database:
**512 records and 340 area tags**.

| source | records | tags |
| --- | --- | --- |
| independent prediction | 512 | 340 |
| importer dry run | 512 insert | — |
| importer real run | 512 insert, 0 errors, 0 notified | — |
| database, this run's stamp | 512 | 340 |

The dry run's stamp `2026-09-05T04:04:42.918Z` matched zero rows. The real run's stamp is
`2026-09-05T04:04:45.179Z`, and the re-run reported all 512 unchanged. Kansas now holds
**1,392 records across 19 rolls and 75 candidates**.

Counts were confirmed by grouping on
`regexp_replace(origin_run_id, '^rollcall:KS:[a-z]+:2178:[0-9]+:', '')`. A
`LIKE '%<timestamp>%'` filter over `origin_run_id` returns the wrong number, because the run id
also carries the roll.

## What is left in Kansas

**64 measures carrying 64 rolls** are marked `candidate:batch-03`. Every one has survived the
title-versus-act check; none has had its enacted act read.

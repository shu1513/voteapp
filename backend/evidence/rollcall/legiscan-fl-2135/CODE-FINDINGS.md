# FL 2135 — code findings

Recorded, **not fixed**. Same convention as `legiscan-tx-2160/CODE-FINDINGS.md`.

## 1. LegiScan's Florida `desc` does not identify the question

Every Florida floor row in the feed carries one of exactly two descs — `House: Third Reading RCS#<n>` or `Senate: Third Reading RCS#<n>` — regardless of what the chamber was actually voting. Checked against Florida's own vote-record PDFs, the same wording covers at least five distinct questions:

| LegiScan desc | Florida's vote record | Roll |
| --- | --- | --- |
| `Senate: Third Reading RCS#8` | `CS/CS/CS/SB 700 \| A - 765612 \| Amendment \| Second Reading` | 1548873 |
| `Senate: Third Reading RCS#5` | `CS/HB 1205 (CS/SB 7016) \| AA - 369878 \| Amendment \| Second Reading` | 1562066 |
| `Senate: Third Reading RCS#31` | `CS/CS/HB 903 \| A - 894320 \| Amendment \| Third Reading` | 1560642 |
| `House: Third Reading RCS#402` | `CS/HB 1205, 1st Eng. \| Adoption` | 1563814 |
| `Senate: Third Reading RCS#10` | `CS/HB 1205 \| Returning Messages` (concurrence) | 1564585 |
| `Senate: Third Reading RCS#2` | `SB 2502 \| Conference Committee Report` | 1591214 |

Consequences:

- The `passage` question class on a Florida `legislative_votes` row is **LegiScan's claim, not Florida's**. An unknown share of the 760 stored floor rows are amendment, adoption, concurrence or conference-report votes.
- `exact_question` on a Florida row must not be trusted or shown to a reader as the question voted.
- Classification is not harmed in the direction that matters: these rows are all genuine floor votes at floor size, and nothing reaches a candidate record without a hand-written judgment. Batch-01 checked all 11 selected rolls against Florida's vote record before judging, and dropped the amendment-vote rolls the divided gate had surfaced.

**Why this is not naively fixable.** The dataset carries no field that separates the questions; the only key is the `RCS#<n>` suffix, which equals the `Sequence:` number on Florida's vote-record PDF for that chamber and day. Resolving a question therefore needs a fetch of `flsenate.gov/Session/Bill/2025/<num>/Vote/<file>.PDF` per roll — a network round trip the fetcher deliberately does not make (it reads an extracted dataset, no live API).

A future fix could either (a) keep the per-roll PDF check as the documented manual step it is today, or (b) add an optional enrichment pass that maps (chamber, date, sequence) → question from the vote records and writes `exact_question` / `voted_text_version` for a batch's rolls only. Option (b) also gets `voted_text_version` for free, which is the version check every batch has to do by hand.

## 2. The survey's stdout is a misleading sample of itself

`fetchLegiscanRollCallVotes.ts` prints `rows.slice(0, 40)` to stdout while writing all rows to `<runId>-survey.json`. For Florida the top 40 rows by count are **all committee names** — the 783 floor votes are spread over 462 distinct descs (the House stamps a unique `RCS#<n>` on every vote), so each floor desc counts 1-16 and none makes the cut. Reading the console alone makes Florida look like a feed with no floor votes at all.

Not a correctness bug, and the file is right. But the cap is a flat 40 rather than anything derived from the data, and the printed object is not marked as truncated. A one-line fix would be to print the row count alongside, e.g. `rows: 40 of 519 shown — full histogram in <file>`.

# PA batch-05 — the two cannabis votes, recorded without a stance

2 rolls / 2 measures / **190 records**. Imported 2026-08-30 on the user's
direction call after both were held back through batches 01-04.

| measure | chamber | date | tally | records |
| --- | --- | --- | --- | --- |
| HB 1200 | House | 2025-05-07 | **102-101** | 176 |
| SB 49 | Senate | 2026-06-10 | **23-27, failed** | 14 |

## Why they were held, and why `general` resolves it

Cannabis legalisation is argued as criminal justice reform, as public health
and as tax policy, and those pull in different directions. Any single stance
would have been the judgment picking a side and stamping it on 176
legislators. But a one-vote margin on the most-watched bill of the session is
exactly the kind of thing a voter wants to see.

`general` is a non-stance research area, so **both yea and nay voters are
tagged topically and neither carries a direction** — verified in the tags
table: 95 yea and 95 nay records, stance null on every one. The vote is
visible and searchable; the app makes no claim about what it means. This is
the Ohio HB 116 blockchain precedent and the Florida SB 700 pattern.

## What the bills actually do

**HB 1200** legalises cannabis for adults 21 and over. Sales run through
**state-run stores under the Liquor Control Board**, not private dispensaries.
Past cannabis convictions are identified from court records and cleared, and
licences favour applicants from communities most affected by past drug
enforcement. It is not a pure legalisation bill: carrying more than the
personal limit still brings fines, growing without a permit stays a crime,
and public smoking stays banned. Under-21 possession stops being a criminal
offence and becomes a fine plus a diversion programme.

**SB 49 is not the Senate companion to HB 1200**, and the description says so
explicitly. It creates a Cannabis Control Board and a fund, and moves permits
and dispensing to patients and caregivers — the existing medical marijuana
system. It does not legalise recreational use. Reading it as the Senate's
answer to HB 1200 would misdescribe both.

SB 49 is also the **only Senate vote in the entire session that failed**, so
its description uses the failed-vote tail.

## Import ledger

Dry run 190 planned; real run 190 inserts, 0 errors, 0 notified, stamp
`2026-08-30T02:57:10.823Z`. Dry re-run all 190 `unchanged`. Lint 0 warnings.

**PROD UNTOUCHED.**

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

SB 49 is the **only Senate vote in the entire session that failed** — but the
defeat was not the end: the Senate voted 29-21 the same day to reconsider it,
leaving the bill pending. The description states both facts.

## Import ledger

Dry run 190 planned; real run 190 inserts, 0 errors, 0 notified, stamp
`2026-08-30T02:57:10.823Z`. Dry re-run all 190 `unchanged`. Lint 0 warnings.

**PROD UNTOUCHED.**

## Review fixes (2026-08-30)

Both findings verified against the fiscal note, the PN 1805 text and the
official bill history; both were real.

- **HB 1200 overstated expungement and understated penalties.** The automatic
  clearing covers convictions **that carried no mandatory minimum sentence**,
  not all past convictions; and possession above three times the personal
  limit stays a crime (a second-degree misdemeanor), so "would still bring
  fines" was too soft. Both now stated.
- **SB 49 omitted its hemp provisions and its real status.** PN 1805 also
  rewrites the hemp rules: a product with more than 0.4 milligrams of total
  THC per container, or made with lab-synthesized cannabinoids, no longer
  counts as hemp and falls under cannabis regulation. And the 23-27 defeat
  was reconsidered 29-21 the same day, so "failed" was not the final status —
  the tail now carries the defeat, the reconsideration and the pending state,
  time-stamped to August 2026.

All 190 records rewritten in place; convergence all unchanged; lint 0.
`import-report.json` remains the insert ledger; the review run is
`import-review-fix-report.json`.

## Plain-language pass 2 (2026-08-30) — the whole campaign measured, not assumed

Batches 03, 04 and 05 were written after the batch-01/02 rewrite and were
never held to the same standard, so every PA description was scored rather
than eyeballed: Flesch-Kincaid grade, longest sentence, and a scan for terms
of art left bare. 45 of the 179 measures came in at grade 8 or above or
carried bare jargon (worst 10.5); those 44 bodies were rewritten. Median
grade 6.8 -> 6.4, worst 10.5 -> 9.0, bare-jargon measures 20 -> 0. A machine
check compared every numeric token, roll number, date, chamber, review status
and label before and after: zero differences. 5,837 records rewritten in
place; all five convergence runs unchanged.

The pass-2 run ledger is `import-plain-language-2-report.json` (a snapshot of
the importer's re-run report). `import-report.json` is untouched: the
importer writes a real re-run's report to `import-rerun-report.json` and
never overwrites the insert ledger.

## Incident note (2026-08-30): this file was truncated and restored

The first push of pass 2 replaced this file with only the pass-2 note. The
cause was a Python one-liner used to append and to fix end-of-file newlines —
`open(p,'w').write(open(p).read()...)` — which truncates the file on opening
for write, before the read runs, so the read returns nothing. The same
one-liner had earlier truncated batch-04's and batch-05's JUDGING.md to a
single newline, and those truncations were merged to main unnoticed. All five
files are restored here from git history, byte-for-byte, with the notes
re-appended. Review caught it; nothing was lost, because every prior version
was in a commit.

## Review fixes on pass 2 (2026-08-30)

Four wording regressions the pass introduced, all verified and fixed:

- **HB 1866**: pass 2 wrote "owning" where the statute says possessing — the
  exact error an earlier review had already fixed once. Possession includes
  holding or controlling a device without owning it. Now "possessing" again.
- **HB 1262**: "a disability that makes online filing hard" broadened the
  bill's exemption, which requires a disability that prevents electronic
  filing. Now "prevents them filing online".
- **HB 316**: the rewrite framed every permit-denial ground as money owed,
  but an unfixed serious code violation is its own ground, not a debt. The
  sentence no longer says "owes money".
- **HB 660**: "sprinkler heads" is a different component from the regulated
  "spray sprinkler bodies" (the base holding the pressure regulator). The
  correct term is back, with a short explanation.

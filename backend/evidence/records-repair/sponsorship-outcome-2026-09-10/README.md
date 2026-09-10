# Sponsorship records: past-tense effect on bills that never became law (2026-09-10)

Local DB only. Found while importing Wisconsin's vetoed-pool roll calls
(`backend/evidence/rollcall/legiscan-wi-2197/batch-04/JUDGING.md`): the record for
2025 Assembly Bill 24 said the bill "required" sheriff assistance, but Governor Evers
vetoed it on 2026-04-03 and the Assembly failed to override on 2026-05-13.

## Defect class

A hand-written sponsorship or authorship record that states a bill's effect in the
past tense ("required", "banned", "created", "established", "prohibited", "allowed")
is wrong whenever the bill did not become law. The fix keeps the sponsorship claim,
switches the effect to "would have required" (or "would require" for a bill still
alive in an open session), and states the actual outcome.

## Sweep

Query: live `candidate_records` with `origin_run_id` null or not starting with
`rollcall:`, description names a bill, uses a sponsorship verb (sponsored, coauthored,
authored, introduced, wrote, filed, proposed, ...), and uses one of the six past-tense
effect verbs not already preceded by would/could/to.

| Stage | Rows |
| --- | ---: |
| Regex hits | 426 |
| Already state enactment (became law, signed, chapter/act number, ...) | 322 |
| Read one by one | 104 |
| Descriptive use of the verb ("required audits", "votes required") or vote/amendment record | 32 |
| Bill actually became law, wording correct as written | 34 |
| Bill did not become law (or is still pending) with past-tense effect | **38** |

Every bill in the 104 was checked against its official history before any change:
local LegiScan datasets under `/Users/shu/legiscan-data/` (AL, AZ 2025, CA, CO, GA,
IN, KY, MI, OK, TN, WA, WI), `ohiohouse.gov` status pages (OH), `apps.azleg.gov`
bill API (AZ 2026), `mgaleg.maryland.gov` (MD), the Nevada Assembly final history
PDF (NV), `le.utah.gov` (UT), `ilga.gov` (IL), `cga.ct.gov` (CT), `akleg.gov` (AK),
`michiganvotes.org` (MI), GovTrack (US S.4685), and the Georgia enacted-statutes
summary (GA HB 64, 2019).

Kept unchanged because the bill became law: AL HB 224 (2021), HB 405 (2026),
HB 55 (2026); CA SB 578 (2025); CT HB 6737 (2015, PA 15-209); GA HB 64 (2019, Act
268); IL HB 4606 (2026, PA 104-0499); IN HB 1150 and HB 1202 (2026); KS SB 359;
MD HB 838 (2022, Ch. 62); MO HB 495 (2025) and HB 1973 (2002); NV AB 166 (2021,
Ch. 177); OH HB 251 (signed 2026-07-07); OK SB 926 (2019), HB 3279 (2026); TN SB 1777
and SB 2403 (2026); TX SB 8 (2021); UT HB 510 (2025); WA SB 6002 (2026), HB 1859,
HB 2259; and the county measures that passed (Anne Arundel Bills 75-24 and 69-25,
Hawaii County Bill 179, St. Louis County marijuana-testing bill).

## Rewritten rows (38)

| State | Bill | Outcome used |
| --- | --- | --- |
| WI | 2025 AB 24 | passed both houses, vetoed 2026-04-03, override failed 2026-05-13 |
| AK | HB 283 (2020) | died in House committee (HSS then JUD) |
| AL | HB 135 (2024) | died in House Ways and Means General Fund |
| AL | HB 103, HB 23, HB 248 | died in committee |
| AL | HB 130 (2024), HB 244 (2025), HB 353 (2026) | passed House, died in Senate |
| AL | HB 378 (2024) | died in House Judiciary |
| AL | HB 319 (2026) | reported to House calendar, no floor vote |
| AL | HB 326 (2026) | passed House, died on Senate calendar |
| AZ | SB 1442 (2026) | held in committee |
| AZ | HB 4147 (2026) | vetoed 2026-05-05 |
| CO | HB25-1069 | lost on Senate second reading |
| CO | HB26-1210 | vetoed 2026-06-02 |
| CO | HB26-1321 | Senate Education postponed indefinitely |
| FL | HB 1395, HB 1397, HB 835, HB 779 (2026) | died in committee |
| GA | HB 641 (2026) | passed House, Senate second read only, session ended |
| KY | HB 141 (2026) | passed House, died in Senate |
| KY | HB 11 (2026) | died in House committee |
| MD | HB 752 (2022) | hearings only |
| MI | HB 4947 | passed House 87-22, in Senate committee (session open) |
| OH | HB 154, 399, 233, 743, 313, 97, 187, 819 | in House committee (136th GA open) |
| OH | HB 478, HB 284 | passed House 2026, in Senate (136th GA open) |
| US | S. 4685 (118th) | died at end of Congress |
| WA | HB 2579 (2020) | passed Legislature, vetoed 2020-04-03 |

Applied with:

```
npm run content:backfill-plain-language -- --rewrites-file backend/evidence/records-repair/sponsorship-outcome-2026-09-10/rewrites.json --allow-sourced-facts
```

Lint (`candidateRecordPlainLanguageLint.ts`, 45-word sentences): 0 warnings on all
38 rewrites. Result: processed 38, applied 38, flagged 0, stale 0. Source URLs were
not changed; each still supports the sponsorship claim, and the outcome comes from
the official histories listed above.

Not touched: the 322 rows that already state enactment were not individually
re-verified; a dataset cross-check of their bill numbers is a separate task.

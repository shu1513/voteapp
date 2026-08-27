# FL 2135 — batch-01 judging notes

11 votes judged and imported on local `voteapp`. **Production untouched.**

## Sources used

Florida's own documents, never a sponsor's framing:

- **House bills: the FINAL BILL ANALYSIS** (`h####z1.<cmte>.PDF` on flsenate.gov), prepared by nonpartisan committee staff after enactment and describing the law as passed. This is Florida's analogue of Ohio's LSC Final Analysis and the CRS summary.
- **Senate bills: the last committee analysis** (`2025s#####.rc/.fp/.ca.PDF`) plus the **House Message summary** (`.hms.`), which states exactly what the second chamber's amendment changed.
- **The enrolled text** where the analysis was not decisive (HB 6025).

> ⚠ **Florida publishes Final Bill Analyses for HOUSE bills only.** Senate bills stop at committee analyses, which are written against the version in front of that committee, not the enacted text — so every Senate bill needs the `.hms.` message summary or the enrolled text to close the gap. This is the mirror image of Texas, where only SENATE bills got an enrolled analysis.

Florida's analyses do **not** open with a sponsor's statement of intent, so the Texas hazard (advocacy numbers contradicting the statute) does not arise here. The Ohio hazard does: **SB 700's summary line "revises the definition of water quality additive" is how the fluoride ban is worded** — the detail section, not the summary bullet, is what tells you what a bill does.

## Version check — every roll

Florida's vote-record PDFs name both the question and the exact version voted (`CS/CS/CS/SB 700, 1st Eng. | Passage | Third Reading`), keyed by a `Sequence:` number that equals the `RCS#` in LegiScan's desc. Each of the 11 rolls was checked against its record, and each bill's history was read for floor amendments adopted after the vote.

- **HB 903** looked like a version trap — the Senate voted on an amendment (894320) after the House had passed the bill — but the history shows the amendment **failed**: amending on third reading takes a two-thirds vote in the Florida Senate, and 22-16 falls short. Both chambers voted the same text.
- **HB 875** is a real one. The House's 84-27 vote was on its own Engrossed 1; the enacted law added House amendment 346467 to Senate amendment 208910 (the two named required courses, the Florida Center for Teaching Excellence, retention of the general knowledge exam). The description names the House version and appends what the enacted law added — the Texas SB 379 remedy.
- **SB 1080** is the same shape: the Senate's 26-8 vote predates House amendment 241889 (comprehensive-plan amendments deemed withdrawn after 180 days; building-code fees usable for permit processing). The Senate description says "the Senate version" and names the additions; the House's 84-29 vote was on the enacted text and reads plainly.
- **SB 492**'s House vote was on the text as amended by 893499 (which removed the former-phosphate-mine provisions) — the enacted text.
- HB 351, HB 1219, HB 6025 and SB 56 had no floor amendment adopted after the selected roll in either chamber.

## ⚠ LegiScan's Florida `desc` does not identify the question

`Senate: Third Reading RCS#8` on roll 1548873 is, per Florida's own vote record, **`CS/CS/CS/SB 700 | A - 765612 | Amendment | Second Reading`** — an amendment vote on second reading. LegiScan stamps the third-reading wording on other questions too:

| LegiScan desc | What Florida's record says |
| --- | --- |
| `Senate: Third Reading RCS#8` (SB 700, 13-21) | Amendment, Second Reading |
| `Senate: Third Reading RCS#5` (HB 1205, 10-26) | Amendment, Second Reading |
| `Senate: Third Reading RCS#31` (HB 903, 22-16) | Amendment, Third Reading |
| `House: Third Reading RCS#402` (HB 1205, 27-82) | Adoption |
| `Senate: Third Reading RCS#10` (HB 1205, 28-9) | Returning Messages (concurrence) |
| `Senate: Third Reading RCS#2` (SB 2502, 24-8) | Conference Committee Report |

So the registry's `passage` question class is what LegiScan *claims*, not what Florida voted, and some of the 760 stored floor rows are amendment, concurrence or conference votes. Nothing wrong reaches a record — every judged roll is checked against Florida's vote record first — but **a future batch must not trust `exact_question` on a Florida row.** Recorded in `../CODE-FINDINGS.md`.

## Label calls

Direction follows the **area description**, never the bill's framing.

- `gun_control` ("Regulate firearm access ... to reduce gun violence") — HB 6025 repeals emergency firearm restrictions, so yea = **against**.
- `civil_rights` ("fair treatment under law") — HB 903 restricts prisoners' access to the courts (exhaustion, physical-injury requirement, one-year limitation, liens), so yea = **against**. The bill also stiffens 10-20-Life and adds an execution-method fallback; every strand points the same restrictive way, so this is not a two-directions text. It was kept over `public_safety_and_crime_control`/for because Sections 1-3, the bulk of the bill, are about access to courts.
- `corporate_accountability` — HB 1219 makes up-to-four-year noncompetes enforceable and mandates injunctions against the employee, so yea = **against**. Follows the Texas SB 50 labor precedent (there is no labor area).
- `environment_and_public_health` runs **both ways in this batch**, which is the point of reading each bill: SB 56 bans atmospheric releases with felony enforcement (**for**), while SB 492 releases wetland mitigation credits earlier and allows out-of-service-area mitigation (**against**, the Texas HB 1586 shape).
- `housing_affordability` — SB 1080 speeds permitting and caps impact-fee increases, so yea = **for** (Texas SB 15 / SB 2835 shape).
- `public_education_quality` — HB 875 is literally about teaching standards and preparation, so yea = **for**.
- `public_safety_and_crime_control` — HB 351 creates a new offense for extreme speeding, yea = **for**.

Descriptions end **"and it became law"** rather than "was signed into law": LegiScan status 4 records enactment, not the governor's signature (batch-02 Texas precedent). HB 6025's description names both halves of the repealed section, including the confiscation savings clause that disappeared with it, rather than flattening the repeal to "loosened gun rules".

## Import ledger

Real run on local `voteapp`, `startedAt` **2026-08-27T01:26:41.127Z**:

- 11 files, all `imported`, **0 errors**, **385 inserts**, 0 rewrites, 0 notified, 0 related-row flags.
- Reconciled three ways: the report says 385; `candidate_records` went 66,213 → 66,598 (+385); `origin_run_id LIKE 'rollcall:FL:%:2026-08-27T01:26:41.127Z'` returns 385.
- The dry run's own stamp (`…T01:26:13.750Z`) matches **zero** rows — positive proof `--dry-run` is inert.
- **62 distinct candidates, not the 63 in the crosswalk.** Nathan Boyles (HD-003, people_id 26477) won the June 2025 special election and first appears in a roll call on 2025-06-16, after every vote in this batch. Not a fan-out gap — the Texas Speaker-Burrows finding in a different shape.

Queue after judging: 11 approved / 895 pending of 906 stored FL rows.

To re-derive the batch predicate after any rewrite run, group by stamp — a rewrite re-stamps `origin_run_id` with the rewriting run's `startedAt` (Texas batch-02 mechanic).

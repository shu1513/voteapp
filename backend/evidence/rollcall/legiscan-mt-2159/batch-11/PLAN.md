# Montana batch-11 — the widest batch, and a line drawn through the crime bills

Twelve measures, nineteen roll calls, 773 candidate records. All twelve became
law.

**Forty-three measures were read for this batch and thirty-one were dropped.**
This is the batch where the phased plan from batch-10 was run at full width:
four themed groups and one administrative triage, worked together rather than in
bill-number order.

| Measure | Chapter | Area | Yes vote means | House | Senate |
| --- | --- | --- | --- | --- | --- |
| HB 74 private fish pond licence now required, fees and bond raised | 39 | environment_and_public_health | **for** | 78-22 | 30-19 |
| HB 197 injury pay may end on the day the worker returned | 112 | social_programs_and_welfare | against | 59-39 | 35-14 |
| HB 239 construction contractors move to a state licence | 644 | corporate_accountability | **for** | — | 38-12 |
| HB 254 fingerprint and FBI checks dropped for staffing firms | 125 | corporate_accountability | against | — | 34-16 |
| HB 270 courts limited in what they may do about a flawed environmental study | 246 | environment_and_public_health | against | 64-33 | — |
| HB 342 a foreseeable risk does not raise a doctor's duty of care | 263 | corporate_accountability | against | 57-42 | 32-17 |
| HB 344 blood limits for nine drugs create a drugged driving offence | 264 | public_safety_and_crime_control | **for** | 68-29 | 36-14 |
| HB 466 agencies may exempt whole groups of actions from review | 297 | environment_and_public_health | against | — | 38-10 |
| HB 467 saliva added to the tests a driver already consents to | 298 | public_safety_and_crime_control | **for** | 64-36 | 38-10 |
| HB 575 a parent facing loss of parental rights may get a public defender | 690 | civil_rights | **for** | 68-29 | 25-24 |
| SB 48 a citizen may make their own complaint about a judge public | 544 | anti_corruption | **for** | 57-42 | 30-19 |
| SB 168 three-year limit on enforcing lakeshore rules | 362 | environment_and_public_health | against | 64-35 | — |

Five measures carry one chamber only, because the other chamber's last kept floor
vote was lopsided.

**HB 575's Senate roll passed 25-24.** It is the closest vote this campaign has
imported from Montana.

## The judgment that shaped the batch

Twenty-one crime and courts bills were read. Seven survived the first four
filters and then failed the fifth, and the reason is worth stating plainly.

`public_safety_and_crime_control` is not an axis where harsher is always "for".
The records this campaign has already imported tag both a violence-intervention
bill and a bill that **ends** automatic life sentences as "for" that area. So a
bill whose only content is a longer sentence or a wider detention power has no
defensible direction.

Two drink-driving bills were kept anyway, and the line matters: HB 344 and
HB 467 change how impairment is **detected and proved**, not how long an
offender serves. HB 626, which changes only how a prior conviction is counted for
sentencing, was dropped under the same rule as the six sentence-length bills.

The caution is recorded in the campaign checkpoint so later batches hold the
line.

## Chapter numbers came from Montana, not from a summary

Every chapter number above was read from
`api.legmt.gov/bills/v1/bills/findBySessionIdAndDraftNumber`, keyed by the draft
number in the bill's own LegiScan link. None was taken from a description of the
bill.

## Two coordination instructions, both of which fired

- **HB 466 section 8**: if HB 346 also passed, section 4 of HB 466 is void.
  **HB 346 was signed on 5 May 2025, so this fired.** Section 4 was the exemption
  for Department of Commerce historic preservation grants, so that exemption
  never took effect and is **not** described in HB 466's records.
- **HB 270 section 8**: if HB 285, SB 221 and HB 270 all passed and all amend
  75-1-201, then every bill's own amending section is void and the merged text
  printed inside HB 270 section 8 is the law. **All three passed.** HB 270's
  records describe the merged text, which is what actually governs. Anyone
  reading HB 285 or SB 221 alone would get the wrong law.

## Every roll was checked against Montana's own vote record

All nineteen imported rolls, and every other floor roll on the measures read,
were compared member by member against
`api.legmt.gov/bills/v1/votes/findByBillId`. **All agree exactly.**

## Reach

The House rolls carry 73 to 75 records each and the Senate rolls carry 10 or 11,
because all 100 House seats are on the 2026 ballot while only about half the
Senate is.

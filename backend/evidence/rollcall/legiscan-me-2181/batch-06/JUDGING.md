# Maine batch-06 — judging notes (final batch)

Judged 2026-09-01/02 from the enacted "Chaptered" text of each Act, with the
committee-amendment SUMMARY as index and LegiScan's `history[]` trail as the
version check. Plain English from the start. No AI provider call.

## Import ledger

- Dry run 1,137 planned inserts; real run **1,137 inserts, 0 errors, 0
  notified, 131 candidates**. Stamp `2026-09-02T00:48:16.809Z`.
- Maine total 5,812 → **6,949 records**. Convergence: all 1,137 `unchanged`.
  The dry run's own stamp (`2026-09-02T00:47:52.936Z`) matches **zero** rows.
- **622 area tags, 67 of them `against`** — LD 215's yea voters, the campaign's
  second Maine measure to score against and the first since batch-01.

## LD 215 — why `against`, and the counter-reading

The Act does two things. Towns must cap general assistance for a resident of a
recovery residence with **26 or more beds at 70 percent** of the ceiling for a
smaller residence, and the State reimburses towns **100 percent** of that
assistance.

Scored `social_programs_and_welfare`/**against**, because the area protects
vulnerable people and the operative effect on them is a 30 percent cut in the
assistance ceiling. The second provision moves money between the State and
towns; it does not reach the residents.

The counter-reading is recorded and did not change the call: supporters framed
the cap as curbing large operators who bill general assistance, which is a
program-integrity argument on a different axis from how much help a resident
receives. `nay` is null, as everywhere in this campaign, so no position is
attributed to members who voted no.

## Two title traps

- **LD 2058** — "Clarify the Requirement That Municipal and County Jails Be
  Available at All Times." The operative words are the exception: jails need
  not be available "unless the persons are detained solely for a civil
  violation of federal immigration laws." That is an immigration measure, and
  it is scored with LD 1971 and LD 2106 from the earlier batches.
- **LD 784** — the title points at first responders and screening. The Act
  amends the **insurance** code and creates a rebuttable presumption that a
  health insurance **carrier** failed to exercise ordinary care when it denies
  coverage for the listed tests.

## Version checks

- **LD 1949 has a superseded-version roll that was deliberately NOT taken.**
  The Senate accepted Report A as amended by Committee Amendment "A" (S-387)
  on 2025-06-17 (RC #594, 22-13). The bill then went back, and in March 2026
  both chambers adopted **Committee Amendment "C" (S-504)**, an entirely
  different amendment, which is what became law. The 2025 roll is on a text
  that no longer exists, so this batch takes the Senate's 2026-03-05 report
  roll and the House's 2026-03-17 enactment.
- **LD 215** — both rolls precede Senate Amendment "A" (emergency clause
  removal) and Senate Amendment "B" (a clerical fix removing an appropriations
  section). Neither touches the 70 percent cap or the reimbursement, and the
  descriptions state no effective date.
- **LD 2107** — both rolls precede House Amendment "A", which only replaces the
  appropriations section. Another Special Appropriations Table measure: it sat
  from March to April 2026.
- **LD 784, LD 1768** also went to the Special Appropriations Table but took no
  late amendment.

No `acknowledge_later_rolls` was needed anywhere in this batch — for once the
enactment-first selection rule matched the gate on every measure.

## Dropped, with the reasons that matter

**LD 698 is the batch's best argument for reading the text.** Its title is "An
Act to Sustain Emergency Homeless Shelters in Maine". Its enacted text
redirects **$2,982,000 from the Housing Opportunities for Maine Fund** and
**$1,491,000 from the Housing First Fund** into the General Fund, and
deallocates one-time Housing First money. Funding shelters by defunding two
other housing programs has no single direction.

**LD 70 and LD 143 were reconsidered and still dropped.** Both are pure
appropriations — LD 70's whole operative text is a $50,000-per-year table,
LD 143's a one-time $3,000,000 surplus transfer for family planning services.
LD 143 was tempting: six divided rolls and a live research area. But the
standing rule that a spending vote carries no direction of its own was applied
to LD 70 in batch-03, and narrowing it for one salient measure would be
choosing the rule to fit the result. Recorded here so a future pass can revisit
the rule deliberately, for all three at once, rather than by exception.

Also dropped: **LD 2148** (raises the state employee premium cap from CPI+3% to
CPI+10%, and who bears it depends on cost-sharing the Act does not set),
**LD 1277**, **LD 1543**.

## Flags

Three `related` flags, all same-date false positives about other bills
(LD 1022, LD 2064, LD 2164). No duplicates, no retirements, 0 ambiguous.

PROD UNTOUCHED.

## Review fixes, 2026-09-02

Three findings, all real, all accepted. Two are the same failure I have made
before and clearly not yet cured: writing from a partial read of the Act.

- **LD 784 (P1)** — the description said any denial of a listed screening
  triggers the presumption. Enacted §4313(15) attaches it only to a denial of
  **covered** specialized risk screening, for a first responder **whose provider
  has determined** the screening is medically appropriate with meaningful
  potential for preventive clinical benefit. All three qualifiers were missing
  because I read the clause up to the page break and not past it. Now carried.
- **LD 1966 (P1)** — the description covered Section 1 of a seven-section Act
  and called it a bill about "hidden fees", a phrase the statute never uses.
  The Act also requires bills to show the consumer-assistance number, bars
  labelling recoverable costs as "public policy charges" and requires objective
  descriptions, lets regulators order corrections of misleading bill inserts,
  sets billing and savings-disclosure standards for shared solar with Unfair
  Trade Practices Act enforcement, directs procurement of up to 4 megawatts from
  small solar projects owned by low- and moderate-income customers or their
  cooperatives, and relaxes some net-energy-billing limits for customers who own
  a share of a project. The lead is now neutral and the body names every strand.
  The label stays `corporate_accountability`/for — the consumer-protection
  strands are the bulk of the Act and every strand points the same way.
- **PLAN.md (P3)** — "Eight areas" corrected to seven; the table has seven
  distinct research areas.

251 records rewritten in place (124 LD 784 + 127 LD 1966), rows unchanged at
1,137, convergence all unchanged. Rewrite ledger stamp
`2026-09-02T01:35:32.739Z`, committed as `import-rerun-report.json`.

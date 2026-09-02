# Indiana batch-02 — judging

## SB 526, absentee ballot retraction — `election_integrity`, yes = for

Judged from the enrolled act read in full, with the Legislative Services Agency fiscal
impact statement (`SB0526.06.ENRH.FN001.pdf`, headed `BILL STATUS: Enrolled`) used as the
index of what changed. No AI provider was called.

What the act does:

- Defines "retraction" and "tabulated" for the election code.
- The public pre-election test of a voting system must include at least one ballot that
  must be rejected, and the system must void that ballot before it is tabulated. The
  follow-up check must confirm the rejected ballot was not counted.
- A voting system **may** include a feature that retracts a scanned or entered absentee
  ballot when it is determined the ballot must be rejected.
- That feature must assign each absentee ballot a unique identification number, and the
  number **may not** contain the voter's name, residence or mailing address, telephone
  number, Social Security number, date of birth, registration date, or driver's licence or
  state identification number. Counties must keep those numbers secure and they are exempt
  from public disclosure.
- Raises the standard a voting system must meet to be approved for use, and the error-rate
  standard, to the Election Assistance Commission's Voluntary Voting System Guidelines as
  amended **February 10, 2021** (from the March 31, 2015 version).
- Moves the date under which a county may keep using an older optical scan or electronic
  system from October 1, 2021 to **October 1, 2025**, provided the system was approved and
  purchased or leased before that date. Vendors still may not market, sell, lease or
  install such a system.
- Voids Election Commission advisory opinion 2022-8 of March 25, 2022.
- Requires a county election board, on an initial determination that an absentee ballot
  must be rejected, to void it by retracting it from the electronic voting system.

**Direction.** The area is "ensure elections are secure, accurate, auditable, and trusted
by the public". The act makes a rejected absentee ballot actually removable before the
count, tests that it works, raises the machine standard to the newer federal guidelines,
and protects the identifier from carrying voter identity. That is `for`.

**The counter-strand is stated, not hidden.** Extending the grandfather date to October
2025 lets counties keep older machines four years longer, which cuts the other way. It is a
transition provision against a set of tightenings, so the stance holds under the
de-minimis rule (the Florida HB 351 precedent) and the description says it plainly.

**Nay is null.** A no vote could rest on the cost of replacing machines, or on the privacy
risk of numbering individual absentee ballots at all — and that second objection sits
*inside* election integrity, since "trusted by the public" is part of the area. With both
directions available to a no voter, the nay side is left unstated and nay voters carry no
tag.

## Checks

- Both rolls are the conference committee report of 2025-04-24, which is the enacted text;
  the act itself is signed `SEA 526 — CC 1`. No version split.
- **Member lists verified name by name against the official Indiana roll-call PDFs**
  (`SB0526.552_H.pdf` and `SB0526.512_S.pdf`), the step Indiana's LegiScan defect requires.
  Official House 65-26 and Senate 40-10 both match LegiScan exactly, every name.
- Body and tail joined with a period; the builder asserted `", The "` appears nowhere.
- `listPlainLanguageWarnings`: **0 warnings over 4 descriptions**.
- Reading level measured separately: mean sentence 16.0 words, longest 25,
  Flesch-Kincaid grade 8.3. Reaching grade 7 would mean dropping the identifier list or the
  grandfather date, which are the two things a reader most needs.
- Each roll's own tally appears in both its yes and no sentence.
- 0 `related` flags, 0 `ambiguous`, nothing retired.

## Review response, 2026-09-02

Three findings on the first review, all real.

**The tracking-number sentence overstated the act (P2, accepted).** The descriptions said
"each absentee ballot gets a private tracking number" as if it were unconditional. The act
is conditional twice over: IC 3-11-15-26(b) says a voting system *may* include a retraction
feature, and 26(c) says *the retraction feature described in subsection (b)* must assign the
number. A county whose system has no such feature assigns nothing. The descriptions now read
"If a system uses that feature, it must give each absentee ballot a private tracking
number." All 95 records were rewritten in place (`import-rewrite-report.json`, stamp
`2026-09-02T05:46:53.761Z`); the convergence run reports 95 `unchanged`; the original insert
ledger is untouched. This is the may/must precision failure Maryland's plain-language pass
recorded — a permission read as a mandate — and it was introduced when the sentence was
shortened for reading level.

**SB 249's third roll was still queued (P2, accepted).** The measure was dropped under
filter 5, but the worklist update only rewrote rows whose disposition began `candidate`, and
SB 249's Senate roll of 2025-03-25 carried `needs member-list check` instead, so it stayed
open for a future batch to research again. It now carries the same dropped disposition as
its two siblings. A measure-wide decision has to be applied to every row of the measure,
not to the rows that happened to be in the default state.

**The shortlist did not reconcile (P3, accepted).** PLAN.md said eight shortlisted and
seven dropped while naming one kept, four dropped and two held. The eighth was SB 2, the
Medicaid act, fetched at 990 lines and then silently left out with the other long acts. It
is now named among the held measures, and the counts read one kept, four dropped, three
held.

## Import ledger

| | |
| --- | --- |
| Files | 2, both `imported`, 0 errors |
| Planned inserts (dry run) | 95 |
| Actual inserts (stamp `2026-09-02T01:08:37.367Z`) | 95 |
| Candidates | 95 |
| Notifications | 0 |

Dry run and real run agree exactly; the convergence run afterwards reported all 95
`unchanged`. Indiana now holds **434 live roll-call records across 101 candidates with 324
area tags**, over two batches. Production has no Indiana records.

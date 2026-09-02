# Alabama 2023 batch-01 — judging notes

## Sources

Every measure was judged from its **enrolled Act**, read top to bottom. The documents were pulled
through the LegiScan bulk API (`getBillText`) rather than scraped, because the Alabama site was
timing out for hours at a stretch; each download was checked against the byte length and MD5 hash
the dataset records for that document, so what was read is exactly the file LegiScan mirrored. Text
was extracted with `pdftotext -layout`.

**One download that looked fine was corrupt.** A direct fetch of HB 125's enrolled Act returned HTTP
200 and a valid PDF header at 3,784 bytes, but the file was truncated and would not open. Only the
hash check caught it. Any future run should verify against `text_size` and `text_hash` rather than
trusting a 200.

## Reading the amendment markup

Alabama's enrolled PDFs print struck text and inserted text together, and `pdftotext` flattens both
into one run of words. The convention, confirmed against SB 1 (where the synopsis says the Act
"reduces" incentive time and the text reads `Seventy-five Thirty days`), is **struck text first,
inserted text second**. Every quoted number in these descriptions was read that way.

That mattered most for **HB 131**, where the enrolled text reads `exceeding six12 months`. The
introduced version was fetched to settle it: subsection (g) is new, the House passed it with a
six-month threshold, and the Senate raised it to 12 months and added a duty to notify the prisoner.
The description says exactly that.

## Roll-attribution check

Run, and vacuous: this session's descriptions carry no roll call numbers, so there is nothing to
match against the bill history. The check bites in 2024 and 2026, not here.

## Date audit

All 16 rolls match the bill history line recording the same action: 16 of 16 exact. No
`official_vote_date` override is needed.

## Version checks and supersession

Six imported rolls are not their chamber's last word, and each carries `acknowledge_later_rolls`
with a description that names the later vote:

- **HB 125** — the Senate's 28-7 vote on 2023-05-25, then a 27-3 concurrence the next day.
- **SB 278** — the House's 66-27 vote, then a 97-0 concurrence on 2023-06-06.
- **HB 363** — the House's 76-25 vote, then a 98-3 concurrence on 2023-06-01.
- **HB 363 in the Senate** is the odd one. The Senate voted twice on 2023-05-31: 35-0, then 26-8 on
  `Read Again a Third Time and Pass as Amended`. The second is the final action and is what is
  imported; the gate counts same-day peers, so the unanimous vote is acknowledged.
- **SB 9** — the House's 78-27 vote, then a 96-3 concurrence on 2023-05-24.
- **HB 131** — the House's 79-23 vote on its own six-month version, then a 97-1 concurrence in the
  Senate's 12-month version.

Everything else is its chamber's final kept floor vote. **HB 379's House roll is a different case**:
the House's later concurrence in the Senate rewrite has no roll call in the dataset, so nothing
supersedes the 73-23 vote in the gate's terms — but that vote was on a materially different text,
and the description and label are written to that text, not to the Act.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`: in each measure the realistic
reason for a no vote runs on a different axis than the scored area.

- **SB 10 and SB 9 — election_integrity, yes = for.** One bars vote counting machines that can reach
  the internet, a cell network or a modem; the other requires a voter-verified paper ballot, marked
  by hand or by an accessible device. Both are squarely "secure, accurate, auditable".
- **HB 379 — two chambers, two bills, two labels.** This was caught in review after a first import
  described the enacted Act for both rolls; 81 House records were rewritten in place. The House's
  73-23 vote on 2023-05-09 was on the **engrossed** text, which is a different bill: it barred *every
  citizen of China*, every firm based in China or majority-owned by Chinese citizens, and China's
  government from acquiring *any* real property anywhere in the state, with only Taiwan and an
  existing business's expansion onto contiguous land excepted. It contains no military, farmland or
  infrastructure language at all. The Senate replaced it with the four-country "foreign principal"
  version limited to farm and forest land and 10-mile zones, and the House concurred on 2023-05-24
  by a vote with **no roll call in the dataset**, so the House's only recorded position is on the
  broad ban.
  - **House roll — civil_rights, yes = against.** A bar on property ownership by individuals of one
    nationality is the area's own subject, "fair treatment under law", and the label follows the
    text, not the bill's title. Nay is null.
  - **Senate roll — national_defense, yes = for.** The Senate voted the enacted text, whose trigger
    is military and critical infrastructure and which reaches governments, parties and sanctioned
    bodies, not private individuals. The objection there runs on the civil rights axis, so nay is
    null.
  The lesson is in the version-check section above: when the history shows a later concurrence with
  no roll, diff engrossed against enrolled *before* writing. That diff was done for HB 131 and
  skipped for HB 379, which is how the error got in.
- **HB 289 — public_safety_and_crime_control, yes = for.** Alabama had no statutory route to see
  police body camera or dashboard camera video. This Act creates one, for the person recorded and
  their representatives, and the area's own words cover "accountability". The access is limited (view
  only, no copying, refusal allowed during an active investigation) and the description says so.
- **SB 1 — public_safety_and_crime_control, yes = for.** The Deputy Brad Johnson Act cuts earned
  early-release credit by more than half at every class. The objection (over-incarceration and cost)
  is a different axis.
- **SB 206 — public_safety_and_crime_control, yes = for.** New graded retail theft offences and a new
  organized retail theft offence.
- **SB 301 — public_safety_and_crime_control, yes = for.** A hands-free driving offence with graduated
  fines, no custodial arrest and a 12-month warning period.
- **HB 131 — public_safety_and_crime_control, yes = for.** No parole consideration while a serious
  new charge is undisposed.
- **SB 261 — corporate_accountability, yes = against.** The Act forbids state and local government
  from using a company's environmental, governance or social conduct as a contracting criterion, and
  names greenhouse gas reporting and board composition explicitly. Removing that lever runs against
  "hold companies accountable for legal compliance, consumer protection, and public impact." A no
  vote could as easily rest on federal-preemption or cost concerns, so nay is null.

## The four no-stance imports, and why school choice is one of them

`general` with no stance is applied on this campaign's stated rule: import without a stance when the
vote is divided, enacted and of clear public salience but no research area carries an honest
direction; drop only when the measure is both outside the taxonomy and low salience.

- **HB 125 (supplemental appropriations).** A 22-item mid-year budget that both spends $207.6 million
  and puts $50 million into the budget reserve. No single direction.
- **SB 278 (distressed colleges loan fund).** State loans to keep a long-established college from
  closing. Supporting an institution and spending public money on a private college pull opposite
  ways, and neither is the plain sense of any area.
- **HB 363 and SB 263 (charter schools; tax-credit scholarships).** These are the school-choice
  measures, and the decision was made once and applied to all of them, including the 2024 CHOOSE Act
  in the sibling batch. `public_education_quality` is the only area that could hold them, and its
  direction is genuinely contested: the same text reads as widening families' options or as moving
  public money away from public schools. **The deciding point is what the text can establish.**
  Neither Act cuts a public-school appropriation by its own terms — SB 263 raises a tax-credit cap
  from $30 million to $40 million, and HB 363 sets first-year funding rules for charter schools — so
  the crowd-out claim cannot be read off the Act, only argued. That is the definition of a contested
  direction, and it gets no stance.

## Duplicates

A precise query — Alabama candidates only, exact vote date, description naming the same bill, and
the record itself worded as a vote — found **9 true duplicates**, all retired before the import
(`duplicate-retirements.json`, to re-run at production promotion). They are hand-written records for
Chip Brown, Margie Wilcox, Shane Stringer and Mark Shirey on SB 10, SB 206, HB 379 and HB 363.

**A first sweep missed them.** It filtered on `origin_run_id IS NULL`, and the hand-written records
carry a `manual:candidate-records:...` run id, so it returned three false positives and nothing
real. Any future sweep should exclude only `origin_run_id LIKE 'rollcall:%'`.

Records describing a *different* vote on the same bill were left alone, as in the 2026 batch: two on
HB 289's House passage, where this batch imports the Senate vote.

## Import and reconciliation

- Dry run: 16 files, 0 errors, 987 planned inserts.
- Real run (stamp `2026-09-02T16:43:43.447Z`): **987 inserts, 0 errors, 0 notified.**
- Review re-run (stamp `2026-09-02T17:27:29.538Z`, `import-rerun-report.json`): **81 rewrites** on
  HB 379's House roll, 906 unchanged, tags moved from national_defense to civil_rights.
- Reconciled three ways: the report totals (987); the run-stamp predicate (987 rows, 114 distinct
  candidates); and the Alabama roll-call total, which moved 1,975 to 4,890 across all six batches
  imported together, of which 1,263 carry a 2014 run id.
- Convergence: a follow-up dry run after the re-run reports all 987 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 32 descriptions. Every description is 2 to 4
sentences with no sentence over 45 words, and a British-spelling scan is clean. Reading grade was
measured, not eyeballed: median Flesch-Kincaid 10.1, worst 11.5.

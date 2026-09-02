# Alabama 2026 batch-01 — judging notes

## Sources

Every measure was judged from its **enrolled Act**, read top to bottom, at
`https://alison.legislature.state.al.us/files/pdf/SearchableInstruments/2026RS/<BILL>-enr.pdf`
(browser User-Agent required, `pdftotext -layout`). The introduced version's Legislative Services
Agency synopsis — official, neutral, no sponsor statement of intent — was used for triage only.

The synopsis is not a safe basis for judgment, and HB 475 shows why: the introduced synopsis
promises "impeachment of commissioners" for a missed meeting, and no such provision survives into
the enrolled Act.

## Roll-attribution check (new for this session)

Before anything else, each candidate roll's printed `Roll Call <n>` was checked against its own
bill's history, because this dataset files some votes under more than one bill
(`../CODE-FINDINGS.md` §1). All 11 imported rolls pass. HB 475's roll carries no roll call number in
its description, so it was matched instead on the exact action text and date
(`Smith motion to Concur in and Adopt Senate Amendment to HB475`, 2026-04-01) — present in HB 475's
history.

## Date audit

All 11 rolls match the bill history line recording the same action: 11 of 11 exact. No
`official_vote_date` override is needed.

## Version checks and supersession

- **HB 95** is the one measure where both imported rolls are superseded. The House voted on
  2026-01-15 and the Senate on 2026-03-31; the Governor then returned the bill with an **executive
  amendment**, which the House accepted 96-0 and the Senate 26-0 before it became law. Both
  judgments carry `acknowledge_later_rolls`. The engrossed text (what the House voted) and the
  enrolled text (what the Senate voted) are substantively identical — a full line diff shows no
  operative difference — and both require the audited precinct and race to be chosen **at random**
  by the county canvassing board.
  **⚠ The executive amendment's content could not be verified.** The enrolled print predates it, and
  no official text of it is published anywhere reachable. Secondary reporting suggests it removed
  the random-selection requirement, but that could not be confirmed against a primary source, so the
  descriptions state the version the chambers actually voted, say that the Governor returned the
  bill with a change of her own that both chambers accepted without opposition, and stop there. They
  do not characterise the amendment. **If the executive amendment text becomes available, revisit
  these two judgments.**
- **SB 57**: the imported House roll is the vote on the conference committee report, which is the
  enacted text. The earlier House passage vote of 2026-04-08 is a different question and is left
  dispositioned as superseded.
- **SB 341**: the imported House roll is likewise the conference report vote.
- **HB 475, HB 2, HB 86, SB 71, SB 5, HB 580**: each imported roll is its chamber's final kept floor
  vote on the measure, with no later kept roll in that chamber.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`: in each measure the realistic
reason for a no vote runs on a different axis than the scored area.

- **HB 95 — election_integrity, yes = for.** Mandatory post-election audits with public reporting,
  observer access, and state reimbursement of county costs.
- **SB 71 — environment_and_public_health, yes = against.** The Act forbids state agencies from
  setting numeric limits stricter than federal rules across drinking water, air quality, hazardous
  substances, contaminated sites and waste, and where no federal standard exists it requires proof
  of a direct causal link to a diagnosed illness before a limit may be set. It also bars agencies
  from defaulting to the federal risk database for water quality criteria.
- **SB 57 — social_programs_and_welfare, yes = against.** Restricting what food benefits may buy
  narrows a safety-net programme's reach (the Texas SB 379 direction). The descriptions state the
  conditional structure: nothing changes unless the federal government grants the waiver.
- **HB 475 — corporate_accountability and cost_of_living_reduction, both yes = for.** Two
  independent strands, each labelled on its own (the Florida SB 700 pattern): a freeze on electric
  base rates until 2029 with a permanent bar on billing customers for a utility's lobbying,
  advertising and grants (household cost), and a ban on regulated utilities donating to commission
  candidates plus ethics-law coverage for commissioners (accountability). The governance strand —
  expanding the commission to seven district-elected seats and creating a Secretary of Energy who
  runs its staff — is stated in the description but carries no label, because centralising control
  of a regulator is a different axis from either scored area.
- **HB 86 — public_safety_and_crime_control, yes = for.** The Act adds work and education completed
  in prison to the parole guidelines and requires the board to consider them. The area's own words
  cover "justice system performance", and evidence-based parole criteria serve that. The objection
  (more releases) is a risk claim on a different axis, so nay is null. Compare SB 254, dropped
  because its text both mandates and relaxes revocation.
- **SB 341 — public_safety_and_crime_control, yes = for.** Automated speed enforcement confined to
  one signed interstate work zone with workers present, civil fines only, sunsetting in 2028.
- **HB 2, SB 5, HB 580 — general, no stance.** See PLAN.md for the rule applied.

**SB 5 is a ballot measure and its description has an expiry date.** It proposes a state
constitutional amendment and takes effect only if a majority of Alabama voters approve it at a
statewide election. The descriptions say so and never say it became law. **This judgment must be
revisited once Alabama voters decide it** (the Missouri HJR 3 rule).

## Duplicates

The dry run raised 100 related flags, most of them same-day records about other bills, since the
2026 session finished many measures on 2026-04-08 and 2026-04-09. A precise query — same candidate,
same date as the imported roll, description naming the same bill — found **27 true duplicates**, all
of which were retired before the import (`duplicate-retirements.json`, to re-run at production
promotion). No record was ambiguous, and none needed rewriting in place.

Records describing an *earlier* vote on the same bill were correctly left alone: three on SB 57's
2026-04-08 House passage and two on SB 341's 2026-04-07 House passage, all of which are different
questions from the conference report votes this batch imports.

## Import and reconciliation

- Dry run: 11 files, 0 errors, 789 planned inserts.
- Real run (stamp `2026-09-02T06:22:27.347Z`): **789 inserts, 0 errors, 0 notified.**
- Reconciled three ways: the report totals (789); the run-stamp predicate (789 rows, 122 distinct
  candidates — every candidate the crosswalk maps); and the Alabama roll-call total (534 before,
  1,323 after, of which 789 carry a 2218 run id).
- Tags: 714 in the database, matching the prediction exactly (yea side only on the eight stance
  rolls, with HB 475 counted twice for its two labels; both sides on the three no-stance rolls).
- Convergence: a follow-up dry run reports all 789 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 22 descriptions. Period joins with a `", The "`
assertion, and a British-spelling scan. Reading grade was measured rather than eyeballed: a first
draft came in at a median Flesch-Kincaid 13.2, which was too heavy, so every description was
rewritten with shorter clauses and plainer words (for example "regulates electric utilities" became
"sets electric rates", "environmental limits stricter than federal rules" became "pollution limits
tougher than federal rules"). Final median 11.1, worst 12.3 (HB 86, held up by unavoidable terms
like Pardons and Paroles).

# Maine batch-02 — judging notes

Judged 2026-08-29/30 from the enacted "Chaptered" text of each Act, with the
neutral SUMMARY at the end of each committee amendment as the index and
LegiScan's `history[]` trail as the version check. No AI provider call.

## Import ledger

- Dry run: 22 files, 0 errors, **1,392 planned inserts**, 0 notified.
- Real run: 22 files, 0 errors, **1,392 inserts**, 0 notified, **131
  candidates** (every candidate the crosswalk maps except Aaron Dana, the
  tribal representative, who casts no recorded vote).
- Stamp `rollcall:ME:<chamber>:2181:<roll>:2026-08-30T01:45:42.140Z`.
- `candidate_records` with `origin_run_id LIKE 'rollcall:ME:%'`: 1,516 →
  **2,908**. Convergence dry run: all 1,392 `unchanged`.
- **733 area tags for 1,392 records** — roughly half, which is the expected
  shape now that `nay` is null on every label: only yea voters are tagged.

## The two new judge gates

Both landed on main after batch-01 shipped, and both bit here.

**1. Every stance label must state `nay` explicitly.** The contract is blunt
that `nay` is never inferred by inverting `yea` — it is the stance a NO vote
actually evidences, or null when it evidences none. **All 11 measures use
`nay: null`**, following the only precedent in the repo (PA batches 01-02,
which set null throughout). The reasoning: for nearly every measure here the
realistic objection runs on a different axis from the area being scored — local
control on LD 427, administrative burden on LD 1784, electricity price on
LD 2037 and LD 585, cost of expansion on LD 2114, the insurer assessment on
LD 93 — so an "against" tag would attribute a position the vote does not
evidence. A no vote still produces a record and still reads "Voted against
passing …"; only the area tag is withheld. LD 1366 and LD 1080 are the closest
calls (single-provision bills where the objection sits on the same axis) and
are the first place to revisit if the campaign wants nay tags.

**2. A roll cannot be approved while a later kept floor vote on the same
measure in the same chamber is unjudged.** The rule matched the Maine
selection rule already in force (prefer the enactment roll), so it passed on 20
of 22 rolls. Two same-day pairs needed `acknowledge_later_rolls`, because a
date alone cannot order two rolls taken the same day:

- **LD 427 house 1591136** acknowledges 1590996. The journal order is
  unambiguous: RC #514 FAILED enactment 71-74, the Senate then enacted in
  non-concurrence, and the House receded and concurred to enactment 82-65 as
  RC #534. The acknowledged roll is the earlier, failed one.
- **LD 1080 senate 1574011** acknowledges 1574010. RC #176 accepted the report,
  Committee Amendment "A" (H-86) was then adopted, and RC #177 passed the bill
  to be engrossed as amended. RC #177 is both later and the only Senate roll
  cast on the amended text.

## Version checks

Every roll was checked against the history trail for an amendment adopted by
the SAME chamber AFTER the roll — the hazard the LD 1016 review surfaced in
batch-01. One hit, and it is the reason LD 427's two chambers carry different
descriptions:

- **LD 427 senate 1590110** — the Senate accepted the report (RC #470, 19-16),
  then adopted Senate Amendment "A" (S-348), which **removed the provision
  barring a municipality from requiring off-street parking within a quarter
  mile of a transit stop** and moved rulemaking to the Maine Office of
  Community Affairs. That is a headline provision, not an implementation
  detail, so the Senate description names the version the Senate voted and
  states that the narrower bill became law. The enacted §4364-D keeps the
  one-space-per-unit cap in growth areas and the off-site parking agreement
  right, and has no transit-stop provision. (LegiScan lists a third amendment,
  H-588, as `adopted: 1`; it is S-348's identical House twin, filed by a
  different member and never moved — the unreliable-flag family again.)
- Everywhere else the committee amendment named in the report was the only
  amendment adopted, so the report-acceptance roll and the enacted text agree.
  LD 93's Senate Amendment "A" (S-426) FAILED adoption (RC #600, 14-19) and
  LD 537's minority report (C-B, H-605) was never adopted.

## Reading the enacted text, not the summary

**LD 93 is the batch's clearest case of the Maine struck-text hazard, and the
committee summary overstates the Act.** The amendment's SUMMARY says it
"removes references to the United States Centers for Disease Control and
Prevention Advisory Committee on Immunization Practices" — plural and general.
Rendering the chaptered pages shows that is only two-thirds true: the ACIP
reference is struck from the two operative criteria for the vaccine list
(§1066(3)(E)(1) "recommended by the advisory committee" and (E)(2)'s
"based on the department's review of the advisory committee recommendations"),
and the definition of "advisory committee" is repealed outright (Sec. 4) — but
the reference in the program's PURPOSE clause in §1066(1) survives, in plain
roman type. The description therefore says the law drops the requirement that
the list follow the federal committee's recommendations, which is what the
operative text does, and claims nothing broader.

**LD 1366's disparity is legible only in the struck text.** `pdftotext` renders
§1118-A(1)(C) as though 112 grams and 32 grams both apply. The rendered page
shows "or cocaine in the form of cocaine base in a quantity of 32 grams or
more" struck through, leaving a single 112-gram threshold — and Sec. 2 repeals
the cocaine base subparagraph in the trafficking statute, leaving one 14-gram
threshold. The counter-reading, that the Act also lowers exposure for crack
offences, is recorded and does not change the call: the area is equal treatment
under law, and a single threshold for one drug in two forms is squarely toward
it.

**LD 585's operative change is a deletion.** The old §10103(4) directed
regional capacity payments to beneficial electrification only "from fiscal year
2019-20 to fiscal year 2024-25"; the enacted text strikes that window (and the
Heating Fuels Efficiency and Weatherization Fund deposit) and adds electric
vehicles alongside heat pumps. The description leads with the permanence,
because that is the change.

## Qualifications carried into the descriptions

Per the TX SB 2972 rule, where the statute qualifies a duty the description
carries the qualification: LD 537's liability needs conscious disregard of a
substantial risk AND an actual resulting harm (stalking, physical harm, serious
property damage, or reasonable fear for physical safety); LD 2106's model
policies bind only as far as state and federal law allow, and are mandatory for
public schools, colleges, state institutions and state libraries but optional
for the private and religious facilities listed; LD 1784 excludes confidential
intelligence and investigative records and allows a reasonable copying fee;
LD 1080's bar reaches deposits based *solely* on income, for applicants who
have not been customers within 30 days.

## Flags and duplicates

Two `related` flags, both on Barbara Bagshaw, both false positives from the
same-date scan: hand-researched records about **LD 1822** (Maine Online Data
Privacy Act) and **LD 208** (firearm waiting period), two different bills that
happen to share 2026-04-09 with LD 2106's House enactment. No duplicates, no
retirements. 0 ambiguous.

## Provenance queries

```sql
-- batch-02 (single stamp, no rewrites yet)
SELECT count(*) FROM candidate_records
WHERE origin_run_id LIKE 'rollcall:ME:%:2026-08-30T01:45:42.140Z';  -- 1392

-- all Maine roll-call records
SELECT count(*) FROM candidate_records
WHERE origin_run_id LIKE 'rollcall:ME:%';                            -- 2908
```

The dry run's own stamp (`2026-08-30T01:45:23.409Z`) matches zero rows, the
standing proof that `--dry-run` is inert. PROD UNTOUCHED.

## Plain-language rewrite, 2026-08-30

Every description in this batch was rewritten for an ordinary reader at roughly
a 7th-grade level, on the same terms as batch-01: **wording only**, no fact,
date, tally, chamber, stance direction, or label changed, and nothing
re-researched.

The jargon that came out: "Class III clean energy standard" is now a rule that
electricity sellers "must start buying power the state counts as clean"; "the
cocaine base subparagraph in the trafficking-by-possession statute" is now
"Maine used to punish crack cocaine at lower amounts than powder cocaine";
"beneficial electrification" and "regional grid capacity payments" are now
money the power grid pays Efficiency Maine, "the state agency that pays for
energy-saving upgrades", spent helping people switch to heat pumps and electric
cars; "the federal poverty level" and "the consumer price index" are explained
where they appear; a subpoena is "a legal order to hand over documents or
testify".

**Ledger:** 1,392 `rewrite`, 0 errors, row count unchanged at 1,392. The
importer preserved the original insert ledger as `import-report.json` and wrote
this run to `import-rerun-report.json` (stamp `2026-08-30T05:29:04.395Z`).
Convergence dry run: all 1,392 `unchanged`. Tag count unchanged at 733 — the
labels were already explicit `nay: null`.

### Review fixes, 2026-08-30 (second pass)

Three review findings on the plain-language pass, all real:

- **LD 427 senate** — an antecedent bug: "The Maine Senate passed that version
  19-16" followed a sentence about the SMALLER version, but the 19-16 vote was
  on the broader committee version (as this file's own version-check section
  says). Now "passed the broader version 19-16".
- **LD 537** — "posts … online" narrowed the enacted definition of disclosure,
  which covers "electronic or other means". Now "shares … whether online or
  any other way".
- **LD 1784** — "every police department" mis-scoped the statute's "law
  enforcement agency", which includes sheriffs and the State Police. Now
  "every Maine law enforcement agency, including town and city police, county
  sheriffs, and the State Police".

279 records rewritten in place (128 + 126 + 25), rows unchanged at 1,392,
convergence all unchanged. Rewrite ledger stamp `2026-08-30T06:14:20.477Z`.

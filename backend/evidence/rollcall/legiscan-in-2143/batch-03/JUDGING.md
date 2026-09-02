# Indiana batch-03 — judging

Every measure was judged from its enrolled act read in full, with the Legislative Services
Agency fiscal impact statement used as the index of what changed. No AI provider was called.

Two things about the source made this batch safer than the last one.

**Additions are now visible.** Indiana prints an amendment in three styles: existing statute
text in roman, new language in bold, and deleted language in roman with a rule struck
through it. `pdftotext` flattens all three, which is what made the SB 289 description in
batch-01 materially wrong. `pdftohtml` keeps the bold, so a bill can be rendered with every
addition wrapped in markers and read without guessing. Only a claim that turns on what an
amendment *removed* now needs the page rendered as an image, and one such claim in this
batch (SB 424's repeal of its old rulemaking subsection) was checked that way.

**Versions come from the dataset, not from guesswork.** The LegiScan bill JSON carries a
dated link for every printed version, so the roll-to-version mapping is exact and no
filename has to be probed. Each selected roll was compared against the enacted text at word
level.

## SB 450, Article V convention — `anti_corruption`, yes = for

What the act does:

- Renames the people Indiana would send to an Article V convention from "delegates" to
  "commissioners" throughout IC 2-8.2. This is the bulk of the printed text and changes
  nothing.
- Adds eligibility bars: five years' continuous Indiana residence; not registered, or
  required to be registered, as a lobbyist within the past five years under state or federal
  law; no conviction for a crime of moral turpitude, newly defined as a crime of violence, a
  sex offence, or a crime involving fraud or a false statement; and no part of a felony
  sentence served within the past ten years.
- Bans a delegate from accepting a gratuity, meaning anything of value, while serving, with
  four carve-outs: anything worth less than $200, ceremonial items, reported political
  contributions, and lawful work compensation.
- Raises the assumed delegation from two to three, plus alternates.
- Requires the delegates to pick one of their own as chair, who casts Indiana's vote and
  speaks to the media, and may be removed at any time.
- Requires the instructing joint resolution to forbid a vote for any amendment altering
  Articles 1 through 7, the Bill of Rights, or the Thirteenth, Fourteenth, Fifteenth,
  Nineteenth, Twenty-third, Twenty-fourth or Twenty-sixth Amendments. The existing
  consequences for an out-of-scope vote are untouched: the vote is void, the delegate
  forfeits the appointment, and a knowing out-of-scope vote is a Level 6 felony.

**Direction.** The area is "prevent abuse of public office through transparency, ethics
rules, and enforcement". The operative additions are an ethics screen on who may hold the
office, a gift ban while holding it, and a binding instruction limiting what the officeholder
may do with it. That is `for`.

**The counter-strands are small and stated.** The delegation grows from two to three, and
the chair gains discretion the statute does not constrain beyond a power of removal. Neither
reverses the direction of the rest.

**Nay is null.** A no vote may rest on opposition to Article V conventions altogether, which
is not an objection to the ethics rules the act adds. Nay voters carry no tag.

## SB 423 and SB 424, small modular nuclear reactors — `cost_of_living_reduction`, yes = against

The two acts share one mechanism and are judged the same way, but they are separate acts,
separate votes and separate records.

SB 423 creates a temporary programme, expiring 1 July 2035, for the large investor-owned
utilities named in 170 IAC 4-7-2(a) to develop reactors alongside partners — large customers,
investors, reactor manufacturers, military installations or state universities. SB 424 makes
a permanent change to the general reactor statute, IC 8-1-8.5-12.1, open to any public
utility acting alone.

Both then do the same thing. The utility gets commission approval to incur development costs
*before* it holds a certificate of public convenience and necessity, which is Indiana's
permission slip to build a power plant. It then gets a rate schedule that raises customer
bills as it spends. It recovers 80% through that schedule and defers 20% to its next general
rate case. Recovery through the periodic mechanism runs over the period the costs are
incurred or three years, whichever is less.

**Direction.** The area is "lower household costs". The whole operative content of both acts
is who pays for early nuclear development and when, and the answer is the utility's existing
customers, before any electricity is produced and before a permit to build exists. If the
project is cancelled the costs are still recoverable; the only penalty is that they come back
"without a return", meaning the utility recovers its money but earns no profit, and even that
penalty lifts if the commission makes three further findings. There is no refund, credit or
clawback anywhere in either act. A yes vote raises household bills, so it scores `against`.

**The counter-strands are stated in the descriptions.** SB 423's stated purpose is to reduce
what ratepayers would otherwise bear by sharing cost and risk with partners, and its
definition of recoverable cost subtracts anything a partner or third party contributed for
free. Both acts hold 20% back for fuller scrutiny in a general rate case, cap the recovery
period at three years, limit rate filings to once every twelve months, and keep overruns out
of rates without a separate prudence finding. These constrain the mechanism; they do not
reverse who pays.

**Nay is null on both.** A no vote may object to charging customers for an unbuilt plant,
which is the cost-of-living objection, but it may equally object to nuclear power itself or
to the state's role in it. With more than one honest reading available, the nay side is left
unstated.

## SB 2, Medicaid matters — `healthcare_affordability` and `social_programs_and_welfare`, yes = against

What the act does:

- Adds a work requirement to the Healthy Indiana Plan for the federal adult group under
  42 CFR 435.119, which is adults aged 19 to 64 with income up to 133% of the federal poverty
  level. The person must work, take part in a work programme, volunteer, or a combination of
  those, for at least 20 hours a week on a monthly average. Twelve conditions satisfy it,
  including a workfare programme, unemployment compensation with its own work rules,
  substance use treatment, being medically certified unfit for work, pregnancy, being a
  parent or caretaker of a child under six, personally caring for someone with a serious
  condition or disability, release from incarceration within the past 90 days, and full-time
  attendance at an accredited school.
- Caps enrolment: the secretary "shall limit enrollment in the plan to the number of
  individuals that ensures that financial participation does not exceed the level of state
  appropriations or other funding for the plan."
- Directs the office to amend the state Medicaid plan to remove the 42 CFR 435.119 adult
  group, delayed until a new waiver is negotiated and approved, and requires the plan to keep
  operating as it stood on 1 January 2025 until then.
- Adds a third mandatory termination trigger if the waiver is revoked or altered, and a
  discretionary one if federal financial participation falls below 90%.
- Bars the office from negotiating away the 20-hour requirement unless federal law demands
  it.
- Forbids self-attestation without verification for income, residency, age, household
  composition, caretaker status and other coverage.
- Requires monthly, quarterly and annual data checks against state and federal records —
  lottery and gaming winnings of $3,000 or more, vital statistics, revenue and workforce
  data, SNAP data including card transactions showing a change of residence, corrections
  data, and a long list of federal sources — and a prompt redetermination whenever new data
  suggests a change.
- Bars a state agency from advertising Medicaid, and a contractor from marketing it beyond
  indicating participation. Repeals the section that had expressly permitted provider
  advertising.
- Sets performance standards for hospitals making presumptive eligibility decisions, with a
  three-strike rule ending a hospital's qualification and a "clear and convincing evidence"
  burden on the hospital in an appeal.

**Direction.** Both areas score `against`. `healthcare_affordability` is "reduce
out-of-pocket costs and improve access to affordable, quality care": the act conditions
coverage on hours worked, caps how many people may be covered at the level of the
appropriation, and removes the expansion group from the state plan. `social_programs_and_welfare`
is "support vulnerable populations through effective safety-net and anti-poverty programs":
the same provisions narrow the safety net, and the verification duties add a monthly
opportunity for an eligible person to be dropped on a data mismatch.

**The counter-strand is real but narrow, and the description does not claim otherwise.** New
IC 12-15-44.5-4.7(h) lets the office pay providers for care given up to 30 days before an
application, for a person later approved and eligible at the time of care. No provision adds
anyone to the eligible population.

**Nay is null on both labels.** A large part of the act is fraud detection and data matching
rather than eligibility narrowing, so the act's whole operative content is not the areas' own
mechanism, and the Connecticut test for authoring a nay stance is not met. Nay voters carry
no tag.

## Checks

- **Version check on all eight rolls.** SB 450: the House passed the Senate bill without
  amendment, so both rolls are the enacted text. SB 423, SB 424 and SB 2: the House roll is
  the third reading of the text the Senate then agreed to, and the Senate roll is that
  agreement, so both are the enacted text. Word-level comparison found no operative
  difference in any of the eight.
- **Member lists verified name by name against the official Indiana roll-call PDFs**, the
  step Indiana's LegiScan defect requires. All eight match the journal exactly. Four other
  rolls examined for this batch did not, and none of them was used; see PLAN.md and
  `../CODE-FINDINGS.md` section 2.
- Body and tail joined with a period; the builder asserted `", The "` appears nowhere.
- `listPlainLanguageWarnings`: **0 warnings over 16 descriptions**.
- Reading level measured separately: mean sentence 14.8 words, longest 25,
  **Flesch-Kincaid grade 7.3**. This is the first Indiana batch to reach the seventh-grade
  target. It was reached by splitting long sentences rather than by dropping statutory
  limits, which is what earlier correction rounds warned against.
- Each roll's own tally appears in both its yes and no sentence.
- 0 `related` flags, 0 `ambiguous`, nothing retired.

## Import ledger

| | |
| --- | --- |
| Files | 8, all `imported`, 0 errors |
| Planned inserts (dry run) | 386 |
| Actual inserts (stamp `2026-09-02T06:34:16.270Z`) | 386 |
| Candidates | 102 |
| Notifications | 0 |

Dry run and real run agree exactly, and the database holds the same 386 rows across the same
102 candidates. The convergence run afterwards reported all 386 `unchanged`; the original
insert ledger is untouched. Indiana now holds **820 live roll-call records across 102
candidates with 666 area tags**, over nine measures and sixteen rolls. Production has no
Indiana records.

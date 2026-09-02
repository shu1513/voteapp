# Montana batch-07 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill from `https://api.legmt.gov/docs/v1/documents`, plus the official action
trail and the session law chapter number from the same site.

Every measure was searched for the phrase "Coordination instruction", the
standing rule added in batch-04. Two of the seven bills read for this batch have
one, and both are described below.

## A defect in LegiScan's Montana vote data

This batch began with a question about which text the Montana House had voted
on for SB 542, and ended by finding that LegiScan's Montana feed can record a
member's vote incorrectly.

Montana publishes its own roll calls, member by member, at

    https://api.legmt.gov/bills/v1/votes/findByBillId?billId=<id>

where the id is the `id` field of the bill record already used for chapter
numbers. Each vote carries a `legislatorVotes` array of legislator ids and vote
types, which resolve against the official roster already kept at
`/Users/shu/legiscan-data/mt-legmt-legislators.json`.

Comparing the two sources member by member, on SB 542's House third reading of
2025-04-24:

- LegiScan reports 73-26 with one member absent, and records Amy Regier as
  voting yes.
- Montana reports 72-27 with one member absent, and records Amy Regier as
  voting **no**.

Every other member on that roll agrees. Amy Regier is in the crosswalk, so
importing the roll as LegiScan has it would have published a record saying a
named person voted for a bill she voted against.

Two further examples turned up on HB 231 and are worse:

- Roll 1551940 (Senate, 2025-04-17, a motion to indefinitely postpone):
  **47 of 50 members disagree** between the two sources, and the tally differs,
  26-24 against 25-25.
- Roll 1554283 (Senate, 2025-04-22, a motion to amend): **8 members disagree
  while the tally matches exactly**, 24-26 in both sources.

That second case is the dangerous one. A check that compares only the totals
would pass it. Only a member-by-member comparison catches it.

Neither of those two rolls is used by this campaign — both are second-reading
motions, and the campaign takes each chamber's last kept floor vote — but they
show the defect is not a single typo.

**What was done about it.** A comparison script now exists outside the
repository at `/Users/shu/legiscan-data/mt_verify.py`, with a companion
`mt_prefetch.py` that warms a local cache of Montana's own vote records. Every
roll of all five measures in this batch was checked. All six imported rolls
agree with Montana's record exactly. An audit of every bill in the worklist,
including the 74 rolls already imported in batches 1 through 6, is running
against the same script; its findings will be reported separately.

**Why SB 542's House roll is held rather than corrected.** The importer verifies
the SHA-256 of each roll call payload against the value approved at fetch time,
and separately checks the evidence file's tally against the approved row. Both
checks exist to stop exactly the kind of hand-editing that a correction would
require, and they are right to. So the House roll is not imported. SB 542 is
carried on its Senate roll alone, and the House roll is marked
`held:legiscan-vote-defect` in the worklist. Importing it needs a supported
correction path in the pipeline, which is a code change and belongs in its own
review. This is recorded in `../CODE-FINDINGS.md` §7.

## HB 231 and SB 542 — the 2025 property tax law

Chapters 674 and 767. Both signed the same day, 2025-05-13. They are one policy
carried by two bills, so they were read together.

### How the two fit together

HB 231 carries four coordination instructions, sections 27, 28, 29 and 31.
Because SB 542 also passed and was approved, they void **HB 231 sections 1
through 23, 25, 30, 33 and 34** — that is, every operative word HB 231 wrote for
itself. What HB 231 does instead is edit SB 542:

- Section 27 replaces SB 542's section 14, the class four property tax rates.
- Section 29 replaces SB 542's section 4, the findings that override local
  fixed mill levies.
- Section 31 adds a new appropriation section to SB 542.
- Section 28 voids a coordinating section in HB 863, if that bill also passed.

Seven sections of HB 231 survive: 24, 26, 27, 28, 29, 31 and 32. Its own
$90 million transfer to a property tax assistance account, in section 25, is
void, and that money does not move.

SB 542 carries a coordination instruction of its own, section 30: if HB 528 and
SB 542 both passed, HB 528 is void in its entirety. It says nothing about
HB 231. The whole HB 231 / SB 542 link runs one way.

### What SB 542 does

Verified page by page against the rendered images, and the images mattered.
**The title clause promising a freeze of 2024 property values for 2025 and 2026
is struck through.** The extracted text shows it as ordinary text. There is no
value freeze in the enacted law.

- A one-time rebate of $400 or the total property tax paid, whichever is less,
  for tax year 2024, to a person who owned and lived in a Montana home for at
  least seven months. Claims ran 15 August to 1 October 2025.
- Class three property, which is agricultural land, drops from **2.16% to
  2.05%** of productive capacity value. On the page image the old figure is
  struck and the new one underlined; in the extracted text the two sit side by
  side as plain text. This was the single most dangerous line in the bill.
- A homestead reduced rate and a long-term rental reduced rate from tax year
  2026, both by application to the department by 1 March. Anyone who received
  the 2024 rebate qualifies for the homestead rate automatically.
- From 2026 the rate schedule is rebuilt around multiples of the statewide
  median value rather than fixed dollar brackets.
- Local governments whose charter, or whose voters, fixed a specific number of
  mills must levy the number that raises the 2025 dollar amount, and may not
  exceed the 2026 count afterwards. The legislature says plainly that it
  "intends to supersede local government charters that fix mill levy limits".
- A fallback: if a court strikes down that findings section, the state
  reimburses affected local governments for four years, including any judgment
  against a local government that obeyed it.

### What HB 231 does

The rates for tax year 2025, applied retroactively:

- Homes: **0.76%** of the first $400,000 of market value, **1.1%** from
  $400,000 to $1.5 million, **2.2%** above $1.5 million. Before the act the rate
  was a flat 1.35%, with 1.89% on the part of a single-family home above
  $1.5 million.
- Multifamily units above $2 million: a maximum of **1.89%**. SB 542's own
  version had conditioned that cap on the units being let at or below 150% of
  the county fair market rent, with the owner certifying the rents. **HB 231
  drops that condition**, so the cap applies with no rent test.
- Commercial and industrial: **1.4%** on the first $400,000 and **1.89%** above.
  The rate above $400,000 is what it was, so every commercial property gets the
  same absolute cut and none pays more.
- $4 million from the general fund to the department of revenue, $500,000 in
  the year to June 2025 and $3.5 million in the year to June 2026.

### Area and direction

Area `cost_of_living_reduction` for both, yes vote **for**, on the same footing
as HB 20 in batch-06. The mechanics support it: a $400,000 home's taxable
percentage falls from 1.35% to 0.76%, and a $1.5 million home's effective rate
falls from 1.35% to about 1.01%.

The counter-facts are real and were weighed. The new top marginal rate of 2.2%
is above the old 1.89%, so the saving shrinks as value rises and reverses at
roughly $3.1 million of market value. Land under commercial buildings on
agricultural or forest land moves to market value. Local governments with fixed
levies are held to their 2025 dollars. From 2026 the low rates require an
application, so a household that does not apply pays the higher rate. The
descriptions state the sliding scale, the application duty and the levy freeze
so a reader can weigh them.

The descriptions do not claim HB 231's own text became law, because it did not.
HB 231's record says plainly that most of its text fell away and names what was
left.

## HB 801 — limits on suing gun makers over their advertising

Chapter 727. New sections 1 through 7 codified into Title 45, chapter 8, part 3.
No amended law, so no strike-through hazard, and no coordination instruction.

Section 4 makes it unlawful to bring a negligent marketing claim against a
firearm or ammunition maker or seller unless **all four** conditions hold: the
marketing directly targeted people legally barred from owning firearms; it
encouraged or facilitated unlawful use; there is a direct and substantial causal
link to the harm; and it broke a statute explicitly regulating firearm sale or
marketing, willfully and knowingly.

Section 5 narrows the federal Protection of Lawful Commerce in Arms Act's
predicate exception as Montana courts are to read it, and says general consumer
protection and public nuisance laws may not count as the predicate statute.

Section 6 gives the attorney general enforcement, and makes a person who brings
a barred claim liable for the defendant's reasonable attorney fees and costs on
top of dismissal. Section 7 lets the maker or seller sue that person for damages
and injunctive relief, with fees again.

Area `gun_control`, yes vote **against**, on close precedent: Kentucky HB 78,
which barred most lawsuits against gun makers and sellers, and Tennessee SB 1360,
which widened lawsuit protections, are both scored the same way. The corpus also
scores the opposite move the opposite way — Connecticut HB 7042, which let people
sue makers that failed to control how their products were marketed, is scored
`for`.

## HB 179 — a petition signature does not reactivate a voter

Chapter 191. Amends 13-2-222, "Reactivation of elector".
Both substantive pages rendered and read. No coordination instruction, no
repealer.

A county moves a registered voter to the inactive list when mail to them comes
back. Existing law returns them to the active list when they vote, give the
county a current address in writing, or file a reactivation form. The act adds
one new subsection, fully underlined:

> "The name of an elector may not be moved from the inactive list to the active
> list of a county by the elector signing a petition for a statewide ballot
> issue."

The only other marks on the page are the renumbering of the old subsections (2)
and (3) to (3) and (4). Their text is untouched and their cross-references still
point to subsection (1), which did not move. Effective on passage and approval.

Area `election_integrity`, yes vote **for**. Montana's own precedent in this
corpus scores list-accuracy and eligibility-tightening measures this way —
HB 413 on the residence test, HB 719 on date of birth, SB 105 on campaigning
near voters. The counter-fact is that an inactive voter loses one route back to
the active list, and the description says exactly which routes remain so a reader
can weigh that.

## SB 440 — a public ballot-count report for every county

Chapter 610. Two sections, both new law, nothing amended, so no strike-through
hazard. No coordination instruction, no repealer, and no effective-date clause,
so Montana's default rule at 1-2-201 applies.

The secretary of state must design one standard reconciliation spreadsheet — a
form that matches ballot counts against each other. Every county election
administrator must fill it in after the count is finished and before the county
canvass begins, post it on the county election website, and send it to the
secretary of state by the time the county canvassers meet. The secretary of
state combines them into one statewide report, due at least seven days before
the state canvassers certify results, and sends it to legislators, the statewide
election website and a legislative interim committee. A digital copy of each
county spreadsheet must be kept in perpetuity.

Area `election_integrity`, yes vote **for**. The area is defined around
elections being "secure, accurate, auditable, and trusted", and a published
reconciliation report is squarely the auditable part.

One oddity, recorded but not resolved: the act's title says the reports cover
results provided under Title 13, chapter 15, part 5, while the codification
instruction places the new section in part 1.

## Measures read and set aside

### HB 329 — tax exemptions for ammunition manufacturers

Chapter 675. Read in full, all twelve content pages rendered, and dropped under
filter 5.

Two facts had to be established first. **The short title is stale.** LegiScan
calls it "Make the Montana ammunition act permanent", but section 11 of the
enrolled act reads "[This act] terminates December 31, 2035", and the words
"permanent" and "Ammunition Availability Act" appear nowhere in it. **And the
exemptions are new.** The Montana Ammunition Availability Act is from 2015, and
its own tax exemption at 30-20-204 terminated on 31 December 2024. On the page
images every mention of ammunition in the three amended tax sections is
underlined, so none of them mentioned ammunition before this act.

What it does: a business whose primary trade is making ammunition components,
and anyone who lends to one, pays no state property tax for schools, no business
equipment tax, no state individual or corporate income tax, and no other state
tax on business activity. The local share of property tax and the employer's
share of payroll taxes still apply. The property exemption reaches 500 yards
around a manufacturing or storage building. To qualify the business must keep
selling to Montanans at no worse than out-of-state prices and must not contract
away all of its output. Retroactive to tax years beginning 1 January 2025.

Nothing in it is struck that changes a meaning: the only deletions are the
connector "and", terminal punctuation, a cross-reference letter, and "upon"
replaced by "on".

**Why it is dropped.** It is an industry tax exemption, and no area in the
catalogue maps to it with a direction that can be defended. `gun_control` is
defined as regulating firearm access through background checks, licensing and
safe storage, and the act touches none of that. `corporate_accountability` is
about holding companies to account for legal compliance, consumer protection and
public impact, and a tax exemption is not that. The lesson from the
`impartiality` error in batch-05 is not to stretch an area to fit a bill.

### HB 423 — voter list maintenance

Amends 13-2-220. Dropped under filter 5 as genuinely two-sided. The act removes
provisionally registered electors from the annual targeted mailing, and directs
the secretary of state to write rules "to maintain applicants, including
removing provisionally registered electors". Tidying incomplete registrations
serves the accuracy half of `election_integrity`; cutting the notice that
precedes removal cuts against the trusted half. The direction cannot be defended
either way, so it is dropped.

### SB 25 — deepfakes in election advertising

Dropped under **filter 1**, not filter 5. Both chambers'
final votes are lopsided — the House concurred 80-17 and the Senate 45-3 — so
neither roll separates one candidate from another. The earlier Senate third
reading of 38-11 is divided but superseded, and the judge command's
superseded-stage gate would refuse it.

This drop exposed a flaw in how the worklist was being read, described next.

## A correction to the worklist

The worklist has one row per divided roll on a measure that became law. It does
**not** say which roll is selectable. The selectable roll is each chamber's last
kept floor vote, and that vote is often not divided even when an earlier one was.
SB 440's Senate side is the clean example: its 3rd reading of 31-18 on 6 March is
divided, but the Senate voted again on 17 April and concurred 48-2, so the
Senate contributes nothing.

Every unbatched row was therefore re-checked against the last kept floor vote in
`legislative_votes`. **Ninety rows are now marked
`superseded:later-roll-used`** and one `dropped:filter-1-final-roll-undivided`.
That leaves **379 rolls across 269 bills** genuinely unworked, plus 58
joint-resolution rolls excluded by rule.

## Reading level

Measured after writing with the Flesch-Kincaid grade formula. Median **6.8**,
worst **7.6**. The longest sentence anywhere is 25 words, well inside the
45-word ceiling `candidateRecordPlainLanguageLint` enforces, and the lint was run
before import and came back clean over all fourteen descriptions.

## Which roll, and which text

Each measure contributes its chamber's last kept floor vote, and the
superseded-stage gate accepted all seven with no `acknowledge_later_rolls`
entry, which independently confirms the choice.

SB 542 needed its own check. Its House third reading of 24 April is on the
House-amended text, and the Senate then refused to concur and a conference
committee was appointed. Montana's official action trail shows the House took
**no** floor action after 24 April: the conference report kept the House text, so
only the Senate had anything left to vote on. LegiScan's gap is real, not a hole
in the data. The 24 April House vote is therefore a vote on the enacted text.

HB 329's held question is answered the same way. The House did vote on the
conference report, 55-45 on 30 April, and so did the Senate, 41-9. The Senate's
41-9 is not divided, which is why no Senate row for it appears in the
divided-and-enacted set.

## What was checked and found clean

- All five measures became law and carry a session law chapter number, confirmed
  against `api.legmt.gov`: HB 179 chapter 191, HB 231 chapter 674, HB 801
  chapter 727, SB 440 chapter 610, SB 542 chapter 767.
- Every imported roll is divided: the losing side is at least a quarter of the
  winning side. The widest margin is HB 801 in the Senate at 37-13, which is
  35 percent.
- None of the five is a joint resolution.
- Every imported roll was compared member by member against Montana's own vote
  record and agrees exactly.
- The import reconciles: the dry run planned 333 rows across 7 rolls and wrote
  0; the first run inserted 322 and failed one roll on a citation URL timeout;
  a second run inserted the remaining 11; a third verification pass reports all
  333 unchanged and is preserved as `import-rerun-report.json`. The database
  holds 333 rows across the two run stamps `2026-09-02T16:05:06.048Z` (322) and
  `2026-09-02T16:05:29.566Z` (11).
- Montana's jurisdiction total is now 3,488 records across 87 candidates and
  2,021 area tags in 14 research areas, on 81 approved rolls.

# Kentucky 2026 batch-02 — how these judgments were made

## Sources

Every judgment was written here from Kentucky's own documents. No AI provider
was called and no reading was delegated, for the reason recorded in the 2025
session's batch-02: a batch-01 research agent reported sections it had not
actually read.

- The **enrolled Act** at
  `apps.legislature.ky.gov/recorddocuments/bill/26RS/<bill>/bill.pdf`.
- The **official vote record** at
  `apps.legislature.ky.gov/record/26rs/<bill>/vote_history.pdf`, which gives
  each roll its sequence number, its question in plain words, and its tally.
- The **bill page** at `apps.legislature.ky.gov/record/26rs/<bill>.html`, for
  the action history and the summaries of the original and enacted versions.

All three need a browser user agent.

## Reading the Acts

Kentucky reprints a whole statute when it amends any part of it, and marks the
**new language in bold** and deletions in square brackets. Plain text extraction
throws the bold away, so each Act was read by extracting the fonts and printing
only the bold runs, which is exactly the language the Act adds. That mattered
most on three measures:

- **HB 398** looks like a rewrite of the rules on retiring a power plant. It is
  not. The bold shows the entire new content is one subsection: the Public
  Service Commission may let a utility recover a unit's decommissioning, removal
  and salvage costs before retirement is authorized, and approving those costs
  is not approval of retirement. Everything else on the page is reprinted 2023
  law, including the presumption against retiring fossil-fuel units.
- **HB 387**'s summary says the council loses "1 physician and 1 nurse". The
  bracketed deletions name which ones: the emergency medicine physician and the
  acute care nurse.
- **HB 490**'s new grounds are printed three times, once for each tier of public
  institution, and the bold makes clear the grounds are identical in all three.

## The Legislative Research Commission summary was checked, never trusted

The Commission's Summary of Enacted Version misstated the Act in six of the 2025
session's twelve batch-01 measures, so every claim below was verified against
the Act. Two checks changed what was written:

- **SB 29.** The summary reads as though a landfill escapes its host county's
  rules. The Act says the opposite direction: the facility is exempt from fees,
  permits and other authorization imposed by the county **where the waste was
  generated**.
- **SB 1.** The summary says the Act limits the superintendent's transfers to
  $250,000 per quarter. The bracketed deletion shows what actually changed: the
  cap used to apply to "any individual transfer" and now applies to "the
  aggregate amount of all transfers".

## Version checks, roll by roll

LegiScan files nearly every 2026 Kentucky House floor vote as
`House: Veto Override`, including votes Kentucky's own record calls
`Final Passage`, `Pass` and even `Suspend the Rules`. Every roll here was matched
to Kentucky's vote record by sequence number, and every question read from that
record rather than from the LegiScan description. All 34 tallies agree.

Eighteen of the 34 rolls are genuine override votes, taken on the enrolled Act
with no amendment possible, so they need no further check. The rest were traced
through the bill history:

- **HB 7 House, 9 February, 78-15.** The House's own text. Senate Committee
  Substitute 1 later added one thing: recorded images may be used only to
  enforce a stop arm violation. The description covers only what was in the text
  the House voted on, all of which survived into the Act.
- **HB 189 Senate, 24 March, 31-6.** The Senate passed the House bill unamended.
- **HB 398 Senate, 26 March, 30-6.** Passage with Senate Committee Substitute 1,
  which the House then concurred in without further change.
- **HB 627 House, 31 March, 75-14.** Concurrence in the Senate committee
  substitute. The Senate roll of 27 March is on that same substitute.
- **HB 677 House, 1 April, 61-28.** Concurrence in the Senate committee
  substitute and its amendments, which is the enrolled text. The Senate's own
  vote on that substitute was 38-0.
- **SB 29 House, 26 March, 70-19.** House passage with its own committee
  substitute and floor amendments, which the Senate concurred in 38-0.
- **SB 66 House, 15 April, 79-15**, **SB 104 House, 25 March, 79-16**,
  **SB 137 Senate, 5 March, 30-7**, **SB 195 House and Senate**, **SB 219
  Senate, 31 March, 32-6**, **SB 324 House, 15 April, 80-15**. Each is the vote
  on the text that was enrolled, either the chamber's own substitute that the
  other chamber then concurred in, or a concurrence in the other chamber's
  substitute.
- **SB 101 Senate, 26 March, 32-6.** Senate concurrence in the House committee
  substitute.

## Two acknowledged later rolls

- **HB 7 House.** The House's last floor vote on the bill was RCS# 360 of 31
  March, 79-11, which is 11.6 percent nays and falls under Kentucky's 15 percent
  divided gate. Roll 1674508 is listed in `acknowledge_later_rolls`.
- **SB 104 House.** Kentucky's record shows RCS# 318 of 25 March as "Suspend the
  Rules", passing 81-0, and RCS# 321 the same day as the real passage, 79-16.
  LegiScan files the procedural vote as a floor vote, and the superseded-stage
  gate cannot order two kept rolls on the same day, so roll 1670410 is listed in
  `acknowledge_later_rolls`.

## HB 398's House roll was left out on purpose

The House's 2 February roll (78-15) is in the pool and survives filter 4, but it
was cast on the House's own text, before Senate Committee Substitute 1 added the
clause saying that approving the costs is not approval of retirement. The
House's vote on the enacted text, RCS# 373 of 31 March, was 81-14, which falls
under the divided gate. Rather than describe the enacted Act on a roll cast
before half of it existed, only the Senate roll is imported. The House row is
marked `batch-02:not-selected` with that reason.

## Why HB 2 was dropped

HB 2 is the session's Medicaid Act: work requirements by January 2027,
cost-sharing, six-month eligibility redeterminations, and a bar on the cabinet
asking for any federal exemption, waiver or delay without the General Assembly's
permission. It is the most consequential measure in the pool, and it is dropped
anyway.

The reason is the veto. HB 2 makes an appropriation, so the Governor could and
did veto it in part, and the Act's own header reads "Vetoed in Part and
Overridden". The two rolls that survive filter 4 are both override votes, and an
override of a line-item veto restores the vetoed items — it is not a vote on the
Act. Kentucky publishes the veto message only as a scanned image with no text
layer, so which items were vetoed could not be established from the record.
Describing those rolls as votes on Medicaid work requirements would be a claim
the evidence does not support.

The alternative was the House's own passage roll of 27 February (77-21), but the
free conference committee replaced that text, so it is not the enacted Act
either. Batch-01 left HB 2 alone for the same reason; this batch confirms the
call rather than reversing it. Every other Act in this batch that was vetoed
carries the header "Vetoed and Overridden", meaning a whole-bill veto, so their
override rolls are votes on the whole enrolled Act.

## Two descriptions corrected after review

Review of the pull request found two errors, both confirmed against the Act
text, and both fixed by a re-judge and a real re-run rather than a file edit.
The re-run at `2026-09-02T16:42:00.596Z` rewrote 84 records on each roll and
left the other 1,563 unchanged; the insert ledger was preserved byte for byte.

- **SB 66.** The first description said a refused "breath or blood test" could
  not be used in court. KRS 189A.105(2)(a) as amended says a warned refusal of
  an evidentiary **breath** test *may* be used in court and suspends the license
  at arraignment; only a **blood**-test refusal is barred as evidence (it still
  suspends), and a roadside preliminary breath test has its own rule under
  KRS 189A.100. When the officer skips the required warnings, paragraph (d) bars
  using any refusal against the driver and bars the arraignment suspension. The
  description now says exactly that.
- **SB 324.** The first draft of this document said the summary was wrong about
  the carry-forward and that the Act tied it to the continuous-production
  allocation. That was a misreading: KRS 154.61-020(4)(c) carries forward the
  unallocated balance of "paragraph (a) of this subsection", which is the $75
  million annual cap. The Legislative Research Commission summary was right.
  The bold run quoted earlier was the parallel clause in KRS 141.383, whose
  "subdivision a." reference points at the same cap. The description now names
  the $75 million cap.

## Direction calls worth recording

**SB 195 keeps a stance even though it has a second subject.** Sections 1 to 5
shield road contractors and design professionals from injury and death claims
and presume that an impaired or badly speeding driver caused their own injuries.
Section 6 adds prosecutors, public defenders and appointed indigent-defense
counsel to the Secretary of State's Safe at Home address confidentiality
program. The 2025 session dropped HB 501 for having two unrelated subjects, but
there the two were of comparable weight and opposite valence. Here the second is
a short eligibility list that nobody votes against, and the divided votes — 66-25
and 29-9 — were about the liability shield. The description names the shield.

**SB 65 is `government_efficiency` / for**, following the identically named
SB 65 of the 2025 session and SB 84 of the 2025 batch-01: when a measure's
subject is the machinery of regulation itself rather than an outcome, that is
the area it belongs in. The description names the rules it cancels, because two
of them are about Medicaid pharmacy coverage and services for kinship
caregivers and a reader should know that.

**HB 355 is `corporate_accountability` / against on all four of its strands.**
A one-year deadline to sue an appraiser, an independent industry board, an
experience requirement that narrows who may investigate a grievance, and a shift
from the board filing its own case to referring it to prosecutors all point the
same way, so the measure is not the two-strand shape that would call for
`general`.

**SB 104 carries no stance even though its intent element is narrow.** The crime
reaches only a person who, after a spoken warning, stays within 25 feet with the
intent to interfere, threaten or harass, which keeps it away from a bystander
recording a scene. But "harass" is defined as conduct causing substantial
emotional distress with no legitimate purpose, and a fourth offense is a felony.
Both halves belong in the description and they do not point the same way.

**HB 1 is `public_education_quality` / against.** The Act's own text is
procedural — it enrolls Kentucky in the federal scholarship tax credit and names
the Secretary of State to report it — so the description stays procedural. The
stance carries what the vote was about: participating in a program that funds
elementary and secondary school scholarships outside the public system.

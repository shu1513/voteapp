# Montana batch-05 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment below rests on the
enrolled bill from `https://api.legmt.gov/docs/v1/documents`, plus the official
action trail and the session law chapter number from the same site.

## The hazard, restated

Montana prints amendments in context: deleted words are struck through, new
words are underlined, and `pdftotext` throws both marks away. The usual tell is a
numbering artefact such as `(3)(2)` or `(d)(e)`. Batch-04's HB 685 proved the
tell is not reliable — a deletion there left no artefact at all — so every bill
in this batch that amends existing law was checked against rendered page images.

## The coordination-instruction check

Batch-04 established a standing rule after HB 231 turned out to have been
largely voided by its own coordination instruction. Every measure in this batch
was searched for the phrase "Coordination instruction". **None of the seven has
one.** HB 719 and SB 45 were checked page by page to the final "- END -" to be
sure.

## HB 39 — political party money for judicial candidates

Chapter 16. 7 pages. Pages 1 and 2 rendered.

Section 4 repeals 13-35-231, "Unlawful for political party to contribute to
judicial candidate". That is the whole of the act's effect: parties may now give
to candidates for judge.

What the page image showed. On page 1, subsection (4) of 3-10-201 —
"Section 13-35-231, prohibiting political party contributions to judicial
officers, applies to justices of the peace" — is **struck through in full**. The
extracted text prints it as live, so reading the text file alone would leave you
thinking the ban still covers justices of the peace. The same page shows
"through (4)" struck and "and (3)" inserted in 3-10-206, which is the
consequential renumbering.

Page 2 was rendered to check the joint fundraising committee section, 13-37-211,
because the act's title says it is amended. The page carries **no strike-through
and no underline at all**; the exclusion of "a judicial candidate" from joint
fundraising committees survives untouched. The amendment to that section is
elsewhere in the act and is consequential rather than substantive.

Montana judges run on a nonpartisan ballot, which the act does not change. So
the practical result is party money in races carrying no party label.

Area `anti_corruption`, yes vote **against**. The act removes a contribution
ban outright and adds nothing in its place. It is the mirror of batch-04's HB
759, which tightened company giving and scored for the same area.

## HB 201 — paid signature gatherers must identify themselves

Chapter 239. New section only, so no strike-through hazard.

A paid signature gatherer must verbally tell anyone they approach about signing a
petition three things: their first name, the state where they legally reside, and
that they are paid. They must also wear a badge carrying the same information,
with their full first name and last initial. Effective on passage.

Area `election_integrity`, yes vote **for**. This is a disclosure requirement on
paid circulators and matches Florida H 1205, which imposed registration and
residency rules on petition collectors and scored for the same area.

Both chambers voted twice. The House passed its own version 59-41 in February
and the Senate concurred 39-10 in March; a free conference committee then
produced the final text, which the House adopted 57-42 and the Senate 27-22. The
conference votes are the ones used, and the Senate's margin collapsed from 39-10
to 27-22 between them.

## HB 413 — residence test for temporary residents

Chapter 278. 2 pages, amends 13-1-112. Both pages rendered.

What the page image showed. Page 2 confirms the rewrite exactly: "comes in" is
struck and "relocates" inserted, and the whole clause naming temporary work,
training and an educational program, plus the permanent-home test, is underlined
new text. Subsections (6), (7) and (8) carry no marks at all, so nothing else in
the residence rules moved.

Subsection (5) is rewritten. Before, a person could not gain residence in a
county or in Montana if the person "comes in for temporary purposes". After, a
person cannot gain residence by relocating "for temporary purposes, such as
temporary work, training, or an educational program, without the intention of
making that county or the state the individual's permanent home at the
conclusion of the temporary work, training, or educational program."

The naming of training and educational programs is the substance: the test now
speaks directly to students and to temporary workers. The other seven
subsections — the fixed-habitation rule, the institution and prison rule, the
military rules, the temporary-absence rule, the family presumption — are
unchanged.

Area `election_integrity`, yes vote **for**, on the pattern the corpus already
uses for measures that tighten who may register. The federal SAVE Act, which
required documentary proof of citizenship, and Ohio SB 293, which ended the
mail-ballot grace period, both scored for the same area.

## HB 711 — no recent party donor may chair redistricting

Chapter 508. 1 page, amends 5-1-102. The page was rendered and carries **no
strike-through anywhere**: the act only inserts. The subsection numbers (1) and
(2) are underlined, meaning two previously unnumbered paragraphs were numbered,
and the new prohibition sentence is underlined in full.

Montana's districting and apportionment commission has five members. The
majority and minority leaders of each chamber name one each. Those four then
pick a fifth, who presides. If they cannot agree within 20 days, a majority of
the Supreme Court picks the fifth member. The act adds one sentence to that
fallback: "The supreme court may not select a fifth member who has made a
campaign contribution in the past 10 years to a major party candidate for state
or federal office."

The bar reaches only the court's fallback pick, not the four legislative
appointments, and only contributions to major party candidates for state or
federal office.

Area `election_integrity`, yes vote **for**. The measure narrows the pool for
the one tiebreaking seat to people who have not recently funded a major party,
which goes directly to the area's stated aim of elections "trusted by the
public". It was first tagged `impartiality`; see the note at the end of this
file on why that was wrong and how it was fixed.

## HB 719 — date of birth on registration and ballot envelopes

Chapter 472. 14 content pages, **all fourteen rendered and read**.

The act amends thirteen sections of the election code and repeals nothing.
Date of birth becomes a required item at four points: on the voter registration
application, on the absentee ballot signature envelope, on the mail ballot
signature envelope, and as a second thing election staff must match before a
ballot counts as a regular ballot.

If a registration lacks a date of birth, the applicant is registered
provisionally rather than fully. If the date on a signature envelope is missing
or does not match the record, the administrator must notify the voter under
13-13-245, and an unresolved mismatch makes the ballot invalid under
13-15-201(8)(a)(i). The cure deadline is 8 p.m. on election day, unchanged. An
uncured ballot is handled as a provisional ballot.

What the page images showed, and it is substantial:

- Pages 3 and 13: the trailing "and" in 13-13-201(2)(d) and 13-19-301(1)(d) is
  struck through and moved to the end of the new paragraph (e). The extracted
  text shows no artefact whatsoever. This is the HB 685 failure mode again.
- Page 5: in 13-13-241(1)(b) the word "and" is struck and a comma inserted, so
  the sentence can carry three conditions instead of two. Again no artefact in
  the extracted text.
- Page 2: "subsection (3) or (4)" is struck and "subsection (3) and subsections
  (4) or (5)" inserted. The extracted text prints both, and a naive reading gives
  the pre-act rule.
- Page 1: the word "also" in 13-2-110(4) is an insertion, which is what makes the
  date of birth and the identification number stack rather than substitute.
- Page 8, a negative finding that matters: the cure menu in 13-13-245(2)(a) has
  **no marks at all**. It still lists only signature remedies — affirm the
  signature, file a new registration form, file a new agent designation. The act
  creates a date-of-birth notice without adding a matching remedy, so the only
  text broad enough to cover the case is the general phrase "confirm the validity
  of the ballot".
- Page 9, a drafting error in the bill itself: in 13-13-246(4)(a), twice,
  "13-13-241(7)" is struck and "13-13-247(8)" inserted, while every other
  renumbering cross-reference in the act points to 13-13-241(8). The digit was
  read at full resolution to rule out a rendering blur. Recorded, not fixed.

Area `election_integrity`, yes vote **for**. The act adds a verification step
rather than removing one, which is the direction the corpus tags for this area
even where the practical effect is to make voting harder at the margin. The
description says plainly what happens when the date is missing or does not match,
so a reader can judge that effect for themselves.

## SB 41 — random selection of a replacement judge

Chapter 353. 2 pages, new section plus an implementation deadline.

When a district judge is substituted, disqualified for cause, or recuses, the
next judge must be drawn by random selection, meaning "a selection from a larger
group by chance". The office of the court administrator had to establish the
procedure by October 1, 2025 and send it to every district judge by October 15,
2025. The procedure must keep the replacement judge's district reasonably close
geographically while still remaining random. Section 1 takes effect October 1,
2025; the rest on passage.

Area `anti_corruption`, yes vote **for**. Random assignment closes off the choice
of a favorable replacement, whether by the judge stepping aside or by a litigant
angling for one. That is an ethics rule against steering public office to a
chosen outcome, which is what the area's description covers. It was first
tagged `impartiality`; see the note at the end of this file.

## SB 105 — wider ban on campaigning near voters

Chapter 342. 3 pages, amends 13-35-211. Pages 1 and 2 rendered.

What the page image showed. On page 1 the old scope phrase "on election day
within any polling place or any building in which an election is being held" is
struck through in every one of the four subsections and replaced by "at any
location where an elector may obtain or vote a ballot, during the hours the
location is open". "100 feet" carries no mark, so the boundary really is
unchanged. Subsection (2) still opens by naming a candidate, a family member and
a campaign worker or volunteer, with only "On election day, a" struck, so its
narrow scope survives. Page 2 shows the act ends at Section 3 with no
coordination instruction, an effective date of July 1, 2025, and a Section 2
requiring the secretary of state to send a copy to every federally recognized
tribal government in Montana.

Before, the ban applied on election day, inside a polling place or the building
holding the election, or within 100 feet of an entrance to it. After, it applies
at "any location where an elector may obtain or vote a ballot, during the hours
the location is open", which covers the whole absentee period at an election
office. The 100-foot boundary is unchanged.

The ban on handing out alcohol, tobacco, food, drink or anything of value to a
voter inside that boundary is extended the same way. Note its scope, which the
description keeps: it binds a candidate, a candidate's family member, and a
worker or volunteer for the candidate's campaign. It does not bind unaffiliated
third parties, so this is narrower than the line-warming bans passed in some
other states.

Area `election_integrity`, yes vote **for**. The House vote, 77-21, is the most
lopsided in the Montana campaign so far and still clears the divided test at
exactly 27 percent.

## Reading level

Every description was measured with the Flesch-Kincaid grade formula after
writing. Median **7.7**, worst **8.5** (HB 39, whose subject forces the phrase
"political party contributions"). The longest sentence anywhere is 28 words,
inside the 45-word ceiling that `candidateRecordPlainLanguageLint` enforces, and
the lint was run before import.

As in every earlier Montana batch, reaching a 7th-grade level meant four to seven
short sentences rather than the two to four the campaign brief asked for. The
deviation is deliberate and reported rather than hidden.

## Which roll, and which text

Each measure contributes exactly one roll per chamber, and the pipeline's
superseded-stage gate accepted all fourteen without an `acknowledge_later_rolls`
entry, which independently confirms each is its chamber's last kept floor vote.

Two measures needed care about which roll that was. **HB 201** went to a free
conference committee, so the April conference-report votes are used rather than
the earlier passage votes. **SB 105** was amended by the House, so the Senate's
April 4 vote on the House version is used rather than its own February passage
vote.

## What was checked and found clean

- All seven became law and carry a session law chapter number, confirmed against
  `api.legmt.gov`.
- Every roll is divided: the losing side is at least a quarter of the winning
  side. The narrowest margin of the batch is HB 39 in the Senate at 25-23; the
  tightest ratio is SB 105 in the House at 77-21, which is 27 percent.
- None of the seven is a joint resolution.
- No measure contains a coordination instruction.
- The import reconciles three ways: the insert ledger records 597 rows across 14
  rolls, the database holds 597 rows at run stamp `2026-09-02T06:26:57.985Z`, and
  the dry run beforehand wrote 0 rows.
- After the relabel below, a second import run reported all 597 records unchanged
  (no description moved) and synced their tags to the new labels: 47 records on
  HB 711 now carry `election_integrity`, 46 on SB 41 carry `anti_corruption`, and
  no Montana record carries `impartiality`. That run is `import-rewrite-report.json`;
  the original insert ledger is untouched in `import-report.json`.

## Correction: `impartiality` is not a legislative area

HB 711 and SB 41 were first tagged `impartiality`. Review pointed out, and the
code confirms, that this area exists only for judges. Migration 073 created it as
a judicial research area, migration 125 set `is_user_selectable = false` on it
(the only other such area is `general`), and migration 159 puts it in the judge
office core sets and nowhere else. A legislator tagged with it gets a stance
that no election-scoped view shows and no voter's ranking can weigh. So the
label was not wrong about the bills; it was invisible.

Both measures were re-read against the catalog's policy areas. HB 711 is about
who chairs the body that draws districts, which is election administration and
public trust in it, so `election_integrity`. SB 41 removes the ability to steer a
case to a chosen judge, which is an ethics rule against abuse of office, so
`anti_corruption`. Neither direction changed. The judgments file, the tags and
this document were all updated together.

The general rule, now in the campaign checkpoint: `impartiality`,
`legal_competence` and `general` are never valid labels for a roll-call record.
- Montana's jurisdiction total is now 2,813 records across 87 candidates and
  1,639 area tags.

# Montana batch-04 — how each measure was judged

Montana publishes no neutral prose summary of a bill. There is no legislative
service commission digest, no bill review office analysis, and the fiscal notes
are scanned images. So the enrolled text is both the ground truth and the only
source. Every judgment below rests on the enrolled bill, read from
`https://api.legmt.gov/docs/v1/documents`, plus the official action trail and
the chapter number from the same site.

## The hazard, restated

Montana prints amendments in context. Deleted words are struck through and new
words are underlined. `pdftotext` throws away both marks, so the extracted text
shows repealed law as if it were still live. The tell is a numbering artefact
such as `(3)(2)` or `(d)(e)`, which means an earlier subsection was deleted and
the rest were renumbered.

Batch-03 found the worst case of this so far: in SB 221 the word "not" itself
was struck out, which reversed a whole section. Every bill in this batch that
amends existing law was checked against rendered page images. What that caught
is listed under each measure.

## HB 214 — state charges for people in federal immigration custody

Chapter 113. New section only; it amends nothing, so there is no strike-through
hazard.

The act says Montana may bring a state criminal charge against a person who is,
or may be, held for federal immigration violations. The state may hand that
person to federal officers on demand, or keep them until the state case ends.
Subsection (3) is the part with teeth: a prosecutor who is considering
declining to prosecute such a person must notify the attorney general before
making the final decision, so the attorney general has time to bring the charge
instead.

Area `immigration`, yes vote **against**. This matches the direction already
used for Montana HB 278 in batch-01 and for the near-identical Pennsylvania SB
471, which required prosecutors to report defendants to federal immigration
officers.

## HB 285 — Montana Environmental Policy Act rewrite

Chapter 248. 27 pages. Pages 4 to 17 and 21 to 22 were rendered and read.

What the extracted text got wrong: several blocks that read as live law are
struck through in the PDF. The largest is 75-1-201(1)(a), the sentence saying
that state policies, regulations and laws "must be interpreted and administered
in accordance with the policies set forth in parts 1 through 3." That was the
act's core interpretive mandate and it is deleted.

Other deletions confirmed on the page images: the opening recital in
75-1-102(1) about the legislature's obligations under Article II section 3 (the
right to a clean and healthful environment) and Article IX; 75-1-201(1)(b)(vi),
the duty to recognise long-range impacts and support programs that anticipate
and prevent environmental decline; 75-1-201(1)(b)(iv)(E), the analysis of
short-term use against long-term productivity; and 75-1-104(3). Two places
change "consult with and **obtain** the comments" to "consult with and
**request** the comments."

Section 10 repeals four sections outright: 75-1-103 (Policy), 75-1-105
(Policies and goals supplementary), 75-1-107 (Determination of
constitutionality) and 75-1-108 (Venue). The pre-repeal text was retrieved from
a January 2024 archived copy of the official Montana Code Annotated pages,
because the live site now serves only a repeal stub. 75-1-103 held the state's
broad environmental policy, including the duty of each generation as "trustee
of the environment for succeeding generations" and the promise of "safe,
healthful, productive" surroundings. 75-1-105 made those goals supplementary to
every board and agency's own authority. Those two are replaced by a much
narrower new policy section. 75-1-107 and 75-1-108 are relocated, essentially
unchanged, into a new section on constitutionality and venue.

New section 1 states that the purpose of an environmental review is to inform
the public and to help the legislature judge whether environmental regulations
are adequate, and that an agency may not withhold, deny or condition a permit
based on the review.

**Which version of 75-1-201 is the law.** Section 12 of HB 285 is a
coordination instruction: if SB 221 also passed and also amended 75-1-201, then
both amending sections are void and 75-1-201 reads as set out in section 12.
SB 221 passed and was signed on May 1, 2025, so the section 12 version controls.
That version allows a greenhouse gas assessment subject to SB 221 and requires
the Department of Environmental Quality to write guidance on when one is needed.
SB 221 itself was judged in batch-03 and scored **for**, because it repealed the
ban on such assessments. The greenhouse gas strand of HB 285 is therefore
already carried by SB 221's record and is not what drives this measure.

What drives it is everything else: the deleted interpretive mandate, the deleted
long-range duty, the weakened consultation, and the new rule that a review
cannot support denying or conditioning a permit. Area
`environment_and_public_health`, yes vote **against**.

## HB 400 — Free to Speak Act

Chapter 275. New sections only, no strike-through hazard.

A public school may not discipline a student for declining to state their
pronouns, or for declining to address a person by a name, pronoun or title that
does not match that person's sex. Employees of public schools and the state get
the same protection against adverse employment action, and the state may not
penalise any person on those grounds. A person harmed may sue for an injunction,
money damages and attorney fees, within two years.

Area `civil_rights`, yes vote **against**. The act removes a school's ability to
require staff or students to use a transgender person's name and pronouns. The
database has consistently scored measures that narrow gender-identity
protections as `civil_rights` against — Texas SB 12, Pennsylvania SB 9 and SB
1293, North Carolina H 805, and the federal H.J.Res. 165.

This one has a genuine second reading, since the act is framed as protecting
speech. It is recorded here rather than hidden: the reason the against
direction wins is that the protected conduct is defined entirely by refusal to
recognise a person's gender identity, and the remedy runs only in that
direction.

## HB 471 — parent permission for identity instruction

Chapter 300. 3 pages, amends 20-7-120. The renumbering artefacts `(4)(5)`,
`(5)(6)` and `(6)(8)` are all plain insertions of new subsections; the old text
prints beside the new throughout, and both readings give the same result, so no
page render was needed.

The act creates a defined category, "identity instruction", meaning instruction
whose goal is studying, exploring or informing students about gender identity,
gender expression or sexual orientation. A school may not let a child attend it
unless a parent or guardian agrees in writing. That is a switch from opt-out to
opt-in; before, a parent could withdraw a child but silence meant attendance.
The definition of human sexuality instruction is widened to name sexual
orientation and gender identity. Advance notice moves from at least 48 hours to
between 5 and 14 school days. New subsection (7) requires trustees who find a
knowing or repeated violation to report it to the Board of Public Education,
which licenses teachers.

A carve-out survives: neither term covers a teacher's response to an unexpected
student question, to the extent needed to answer it or keep order.

Area `civil_rights`, yes vote **against**, on the same precedent as HB 400.

## HB 682 — gender transition treatment lawsuits and coverage

Chapter 709. 20 pages, amends seven statutes and adds two. Pages 1, 9, 10, 11,
12, 13, 19 and 20 were rendered.

What the page images showed: the three doublets in the extracted text
("upon"/"on" in 27-2-204 and 27-2-205, and "Action" replaced by "Except as
provided in [section 1], action") are the only edit sites, and all are
housekeeping except the last, which is what makes the new deadline override the
malpractice deadline. No "not" was struck anywhere. 2-18-704(17), 53-4-1005(6)
and 53-6-101(14) are wholly new insertions.

The act does two things, and does not do a third that its title might suggest.
It bans nothing. There is no crime, no licence sanction and no funding cutoff
anywhere in the text.

1. **Lawsuit deadline.** A person injured by gender transition treatment
   received as a minor may sue until age 25, or within two years of discovering
   both the injury and its cause, whichever is later, with an absolute cutoff at
   age 30. Montana's ordinary malpractice deadline is two years from injury or
   discovery, capped at five years from injury. For treatment at age 14 the
   ordinary cap would run out around age 19.
2. **Insurance pairing.** Any private policy, public employee or university
   plan, Healthy Montana Kids, or Medicaid that covers gender transition
   treatment must also cover detransition treatment at equivalent cost-sharing.
   A plan can escape that duty by dropping gender transition coverage
   altogether, except for people already enrolled who had received benefits.

Two drafting flaws are worth recording because a reviewer will find them.
"Detransition treatment" is never defined anywhere in the act, although the
coverage duty turns on it. And 53-4-1005(6) and 53-6-101(14) have a "(b)" and a
"(c)" with no "(a)"; the rendered pages confirm the label is genuinely missing
from the enrolled bill, so that is the bill's own error, not an extraction
artefact.

Effective October 1, 2026. Nothing is retroactive.

Area `civil_rights`, yes vote **against**. A second label was considered and
rejected. The insurance pairing looks like a `healthcare_affordability` **for**,
but the same section lets a plan shed the duty by dropping the paired coverage
for everyone, so its net effect on coverage is not defensibly positive. Under
the fifth selection filter a label needs a direction that can be defended, so
only the civil rights strand is tagged. The direction there follows the
precedent set by the federal H.R. 498 and North Carolina H 805, both of which
scored `civil_rights` against for restricting gender transition care.

The description states what the act does, not what it is for. It does not claim
the act bans treatment, because it does not.

## HB 685 — feasibility allowance for nondegradation

Chapter 712. 25 pages, all rendered.

Montana's nondegradation policy protects water that is already cleaner than the
minimum legal standard. To be allowed to lower that water's quality, an
applicant had to prove four things by a preponderance of the evidence: that the
degradation was necessary because no feasible project change would avoid it;
that the project would produce important economic or social development **and**
that the benefit exceeded the cost to society of allowing the degradation; that
existing and anticipated uses would be fully protected; and that the
least-degrading feasible practices would be used.

The act deletes the second half of the second test. What the extracted text got
wrong, on page 15: it prints "…important economic or social development and
that the benefit of the development exceeds the costs to society of allowing
degradation of in the area that the high-quality waters are located", which
reads as though the cost-benefit test survives. The page image shows the whole
balancing clause struck through. Only "will result in important economic or
social development in the area" remains.

A second deletion leaves **no artefact at all** in the extracted text and would
have been missed without rendering. On page 14, in 75-5-301(6), the words "and
(3)" are struck from the reference to 75-5-303. The department's duty to write
objective, quantifiable guidelines for granting or denying an application no
longer reaches subsection (3), which is the subsection that actually grants the
allowance.

Most of the rest of the act is a rename: "authorization to degrade" becomes
"feasibility allowance" across six statutes. The three-part feasibility test the
act is named after already existed. Nothing else moved: the definitions of
degradation, existing uses and high-quality waters are untouched, every numeric
threshold is unchanged, the preponderance standard is unchanged, and the public
comment period, the appeal to the Board of Environmental Review, and the ban on
allowances in outstanding resource waters all survive.

So the description says the law removes one of four tests and leaves the other
three, rather than calling the act a repeal of the nondegradation rule. Area
`environment_and_public_health`, yes vote **against**. Both deletions remove a
check on approval; neither adds one.

## HB 759 — limits on company gifts to candidates

Chapter 526. New section only, no strike-through hazard.

A candidate may accept a contribution from a limited liability company or a
partnership only if that entity is taxed as a sole proprietorship or as a
partnership for federal tax purposes. A candidate may not accept one from an
entity taxed as a C corporation or an S corporation. A permitted contribution
must be reported under the name of the member or partner making it, and that
person's individual limit under 13-37-216 then applies.

Area `anti_corruption`, yes vote **for**. Both halves tighten: one class of
giver is barred outright, and the remaining class can no longer be used to give
past a personal limit.

## SB 91 — citizenship mark on licences and ID cards

Chapter 551. 6 pages, amends 61-5-111 and 61-12-501. The only artefact is a
struck period replaced by "; and" where a new clause was appended; both readings
give the same result, so no render was needed.

From January 1, 2026, a driver's licence or state identification card issued to
a United States citizen must display an image of an eagle signifying citizen
status. Cards issued to people who are not citizens carry no such mark, so the
card itself reveals citizenship. Section 5 limits the rule to cards issued on or
after that date, so cards already in wallets are unaffected.

Area `immigration`, yes vote **against**, on the precedent of Tennessee HB 749,
which concerned licence markings that distinguish people not lawfully present.

## Reading level

Every description was measured with the Flesch-Kincaid grade formula after
writing. The median is **7.4** and the worst is **8.4** (HB 285). The longest
sentence anywhere in the batch is 30 words, well inside the 45-word ceiling that
`candidateRecordPlainLanguageLint` enforces, and the lint was run before import.

As in batch-01, batch-02 and batch-03, hitting a 7th-grade reading level meant
writing five to seven short sentences rather than the two to four the campaign
brief asked for. Holding to four sentences pushed the measured grade above 10 in
earlier batches. The deviation is deliberate and is reported rather than hidden.

## Which roll, and which text

Each measure contributes exactly one roll per chamber, and the pipeline's
superseded-stage gate accepted all sixteen without an
`acknowledge_later_rolls` entry, which independently confirms each is its
chamber's last kept floor vote.

The version each chamber actually voted was checked against the posted version
dates from the state document API. For every measure the enrolled text carries
the same version both final rolls voted:

| Measure | Version voted | Enrolled posted |
| --- | --- | --- |
| HB 214 | `_2`, posted Feb 3 | Mar 25, after the last roll on Mar 24 |
| HB 285 | `_2`, the Senate-amended version, posted Mar 31 | Apr 15 |
| HB 400 | `_2`, posted Feb 17 | Apr 14 |
| HB 471 | `_2`, posted Mar 4 | Apr 15 |
| HB 682 | free conference report, both chambers Apr 30 | May 1 |
| HB 685 | `_1`; the Senate concurred without amending | Apr 22 |
| HB 759 | `_2`, posted Apr 5 | Apr 21 |
| SB 91 | `_3`, posted Mar 1 | Apr 18 |

## What was checked and found clean

- Every measure became law and has a chapter number.
- Every roll is divided: the losing side is at least a quarter of the winning
  side. The narrowest is HB 759 in the Senate at 40-10, which is exactly a
  quarter.
- No measure is a joint resolution. Montana joint resolutions do not go to the
  governor and are not law, so none may carry a description saying "it became
  law".
- The import reconciles three ways: the insert ledger records 682 rows across 16
  rolls, the database holds 682 rows at run stamp
  `2026-09-02T06:16:41.728Z`, and the dry run beforehand wrote 0 rows.
- Montana's jurisdiction total is now 2,216 records across 87 candidates and
  1,299 area tags.

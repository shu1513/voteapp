# Wyoming batch 01: how each measure was judged

Ten measures. One roll call per measure, and in every case it is that
chamber's vote on the text that actually became law.

## Sources

The enrolled act is the ground truth, and every one of the ten was read from
top to bottom, not in excerpt. The enrolled acts come from wyoleg.gov, the
legislature's own site. No sponsor material was used.

## Wyoming's struck text, and why an ordinary extract is unsafe

Wyoming reprints an amended statute section in full. Language being removed
is struck through with a rule, and language being added is underlined. A
plain `pdftotext` extract keeps both and marks neither, so it shows repealed
law as if it were still live and can invert what an act does.

`wy_text.py` in this directory reads the rules out of the PDF and labels
every word, so a deletion comes out as `[DELETED: ...]` and an addition as
`[NEW: ...]`. It was validated before it was trusted, on a section that is
one hundred percent one thing: section 1 of HB 116 creates a brand new
statute section and comes out with no markers at all, which is right, because
Wyoming does not underline a section it introduces with the words "is created
to read". Section 2 of the same act amends existing law and comes out with
both markers, including the change from `post secondary` to `post-secondary`
showing as a deletion beside an addition.

## The vehicle-bill check

Every one of the ten titles was compared against what the enacted text
actually does. None is a vehicle bill.

One title over-describes its act. HB 42's title promises to specify "civil
liability for damages resulting from abortions", and the enrolled act
contains no civil liability section: it was amended out on the way through.
The description written for HB 42 describes only what the act does. This is
the reason a title check is a first filter and never a clearance.

One act carries a contingent effective date. Sections 4 through 7 and 9(b)
of SF 152 were to have no effect if either 2025 SF 1 or 2025 HB 1 became law.
Neither did; both died at engrossment on February 7, 2025. The loan fund and
the restoration grants are therefore live law, and the description says so.

## The measures

| measure | chamber | vote | chapter | research area | direction of a yes vote |
|---|---|---|---|---|---|
| HB 42, surgical abortion facilities | Senate | 24-7 | 46 | women's reproductive rights | against |
| HB 116, out-of-state driver's licenses | Senate | 22-8 | 83 | immigration | against |
| SF 107, noncompete agreements | House | 39-21 | 170 | reduce wealth gap | for |
| SF 152, wildfire management | House | 46-14 | 129 | environment and public health | for |
| SF 20, government data privacy | House | 34-25 | 48 | data privacy | for |
| SF 59, K-12 language and literacy | House | 46-14 | 66 | public education quality | for |
| SF 4, Medicaid ambulance rates | House | 34-23 | 69 | healthcare affordability | for |
| SF 35, school cell phone policies | House | 40-20 | 56 | public education quality | for |
| SF 99, power line easements | House | 47-14 | 102 | public infrastructure | for |
| SF 88, sex offender residency | House | 41-19 | 59 | public safety and crime control | for |

Notes on the harder calls.

**SF 107, noncompete agreements, under reduce wealth gap.** The area is about
economic mobility, and a ban on covenants not to compete is a law about
whether a worker may leave for better pay. Corporate accountability was
considered and rejected: that area is about holding companies to account for
compliance and consumer harm, and this act is about worker mobility. There is
still no labor or union research area in this project, and this is the
nearest honest fit.

**SF 152, wildfire management, as one label rather than several.** The act
has an administrative strand, a spending strand and a loan strand, but all
three are one policy: getting Wyoming ready for the next fire and repairing
the last one. Splitting it would invent distinctions the vote did not draw.

**SF 35, school cell phone policies.** The act does not ban phones. It
requires each district to have a policy and to file it with the state
superintendent, and leaves the contents to the district. The description says
that plainly, because a reader who assumed a ban would be misled.

**SF 88, sex offender residency.** The prohibition is added to an existing
statute that carries its own penalties, and the enrolled act does not reprint
them. The description therefore states the prohibition and its exceptions and
says nothing about the penalty.

**Every label states `nay: null`.** A no vote gets no tag. This is the
conservative reading and it is the right one here: in a chamber where the
majority caucus is split, a no vote on HB 42 may come from a member who
thinks the act goes too far or from one who thinks it does not go far enough,
and the roll call cannot tell the two apart. Rather than infer, the records
say what a yes vote evidences and stay silent on a no vote.

## What was dropped, and why

**SF 44, college sports and biological sex.** The Senate concurrence, 23-8,
is the only closely divided roll and it is on the enacted text, so it clears
filters 1 through 4. It fails filter 5. Under civil rights the direction
reads two ways that are genuinely comparable in weight: supporters argue the
act secures fair and equal competition for female athletes, opponents argue
it excludes a class of students from a public benefit. Both are equal
treatment arguments. Contested worth is fine and contested direction is not,
and no other research area fits the act at all. **This one is raised for the
operator.** It is a salient Wyoming measure and dropping it loses real
information; if the project wants a settled direction for this class of
measure, that is a decision to make once rather than per state.

**SF 69, homeowner property tax exemption.** Dropped on filter 4. The Senate
passed it 23-8 and the House 42-19, both closely divided, but the Senate then
refused to concur 6-25 and the text that became law is the conference
committee report. Both chambers' votes on that report, 26-3 and 49-7, are not
closely divided. The two divided votes are on texts that did not become law.

**HB 165, ranked choice voting prohibition.** Dropped on filter 5. Choosing
a voting method is not secure, accurate counting, so election integrity does
not fit, and no other area gives it a direction.

**HB 92, sex offenders barred from public office.** Dropped on filter 5.
Rules about who may take part in lawmaking have been settled in this campaign
as having no honest direction.

**Two superseded rolls.** The House's 45-15 on HB 116 predates the Senate
floor amendment, and the House's 52-6 concurrence on the enacted text is not
divided. The Senate's 24-6 on SF 152 predates the House amendments, and its
26-5 concurrence is not divided. Both chambers still appear in the batch
through the chamber whose divided vote is on the final text.

The full list of all 121 closely divided votes on enacted measures, with a
disposition for each, is in `divided-enacted-worklist.tsv`. The 104 marked
"not yet worked" are held for a later batch; none has been rejected.

## Writing

Every description was built by one script from one body per measure, so the
yes and no versions cannot drift apart. The script refuses to write a file
that fails any of these, and it was run to a clean pass:

- no sentence longer than 45 words, the plain-language lint's limit
- no British spellings, checked against an explicit word list
- no two sentences joined without a space
- no comma splice before "The"
- the roll call's own tally present in both the yes and the no description
- every description ending in a period

Reading level was measured separately, because the lint is not a reading
check. Flesch-Kincaid grades across the twenty descriptions run 7.7 to 9.7,
median about 8.6. The first drafts came in at 8.6 to 12.6 and were rewritten.

The descriptions run five to twelve short sentences rather than the two to
four a summary would use. That is a deliberate trade. Wyoming's acts carry
limits that matter to a reader deciding what a vote meant: the ten-mile
admitting-privileges radius in HB 42, the four carve-outs and the sliding
recovery scale in SF 107, the 2006 cut-off and the thirty-foot width in
SF 99, the grandfather clause in SF 88. Cutting to four sentences would drop
exactly those, and dropping them is what causes corrections. Reading level is
treated as binding; sentence count is not.

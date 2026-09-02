# Kentucky batch-01 — how these judgments were made

## Sources

Every judgment was written from Kentucky's own documents. No AI provider was called.

- The **Summary of Enacted Version** on each bill page at
  `apps.legislature.ky.gov/record/25rs/<bill>.html`. This is the Legislative Research
  Commission's section-by-section summary. It is official and neutral, and it carries
  **no sponsor statement of intent**, so the Texas advocacy-preamble hazard does not
  recur in Kentucky.
- The **enrolled Act** at `.../recorddocuments/bill/25RS/<bill>/bill.pdf`, which is the
  ground truth. The summary is an index to the Act, never a substitute for it.
- The **official vote record** at `.../record/25rs/<bill>/vote_history.pdf`, which gives
  each roll's sequence number, the question in plain words, the date and time, and the
  full member lists.
- The **Governor's veto message** at `.../record/25rs/<bill>/veto.pdf` where one exists.

Reading was fanned out to four research agents, four measures each, each returning the
section-by-section reading, a per-roll version check, the question Kentucky records, and
a two-directions finding. The labels and every description were written here from what
they reported, after checking their claims against the Acts.

## What the sources got wrong

**The official summary can misstate the Act, so every claim was checked against the
enrolled text.** Six cases in these twelve measures:

- **SB 100**: the summary says the Act bars selling nitrous oxide "to persons under 21
  years of age" and limits the ban to license holders. Neither is in the Act. Section 16
  is a flat ban with no age element, and it binds every retailer of tobacco, nicotine or
  vapor products, licensed or not.
- **SB 183**: the summary describes only the requirement to write an economic analysis
  before voting against a company's board. It never mentions the other half of the same
  sentence — that voting **with** the board requires no analysis at all. That asymmetry
  is the whole point of the provision, and a description written from the summary alone
  would have missed it. The description here states both paths.
- **HB 4**: the summary says the Act bars spending on "bias incident investigations." The
  spending ban lists four things and that is not one of them; what the Act does is require
  the institution's general counsel to authorize and certify such an investigation in
  writing. The summary also omits sex from the housing list, which matters because the
  single-sex housing exception exists precisely because sex is on it.
- **HB 398**: the summary calls the inspection changes "technical corrections." They are
  not: they change who may accompany an inspection and downgrade the inspector's stated
  authority. The summary also never mentions that the power to put a fired worker back on
  the job pending a decision was abolished.
- **HB 424**: the summary omits that the Act turns a closed list of faculty removal
  grounds into an open-ended one at the comprehensive universities. That measure was
  dropped, but the finding is why.
- **HB 694**: the summary states a ban where the Act states a duty, and adds an "under
  the age of 65" qualifier that appears nowhere in the text.

**Two other reading rules were applied.** Kentucky reprints a whole statute when amending
any part of it, so a long Act can be almost entirely unchanged law — HB 520 is eight pages
that change five short phrases and delete one word. And Kentucky marks new language in bold,
which plain text extraction loses, so new text was distinguished from reprinted text by
reading the formatting, not the words.

## Direction calls worth recording

**HB 4 follows three states of precedent.** A ban on diversity, equity and inclusion
programs at public universities is `civil_rights` / against, as Ohio SB 1, Georgia SB 1 and
SB 185, and Tennessee SB 1084 and SB 1713 were all scored. The Act's own text does contain
a counter-strand — it forbids differential treatment on the same grounds it removes
enforcement machinery for — but scoring it differently from four earlier states would make
the corpus incoherent about the same kind of measure.

**HB 399 is `civil_rights` / against rather than `public_safety_and_crime_control` / for.**
It creates two new crimes, which reads as public safety on its face. But the definition of
"person" writes legislators, their staff and legislative officers out of who can commit
them, and one of the two second-degree offences is obstructing exactly those exempted
people. A criminal law that binds only the public, in the building where the public
petitions its government, is a fair-treatment question.

**SB 84 follows the Texas SB 14 and Georgia HB 1247 precedent**: ending judicial deference
to an agency's reading of a statute is `government_efficiency` / for, because its subject is
the machinery of regulation itself. That is the distinction the Pennsylvania SB 187
retraction drew.

**SB 89 keeps a stance despite a counter-strand**, following Connecticut HB 7042. The Act
narrows "waters of the Commonwealth" from essentially all surface and underground water to
four categories, and separately requires extra bond from coal operators whose mines will
need long-term water treatment. The narrowing is the dominant thrust and the Governor's veto
rested on it; the bonding provision is named in the description, and `nay` is null.

**Two measures carry no stance at all**, following Ohio HB 116 and Missouri SB 4. HB 90
pairs birth-center licensing with a rewrite of the abortion statutes that both writes named
safe harbors for physicians and replaces an open clinical-judgment standard with a closed
list. HB 695 both bars the health cabinet from cutting Medicaid coverage without the
legislature and orders a work requirement application plus the restoration of behavioral
health treatment approvals. Labeling only the clean strand of either would mislead by
omission. `general` tags both sides topically with no stance.

**Every stance label sets `nay: null`.** A no vote on one bill is not evidence a member
opposes the area's whole goal, and the realistic objection usually runs on a different axis
from the area scored.

## Filter 5 drops

Four measures were read in full and dropped. Reasons are recorded per roll in
`../survey/divided-enacted-worklist.tsv`: HB 346 (air-quality fees, exemption against
no-cap rule), HB 424 (university employment, open-ended removal grounds against a longer
notice period), SB 19 (moment of silence and released time for moral instruction), and
HB 684 (elections — the same sentence that requires 60 days of surveillance recording puts
a 60-day clock on requesting it).

## Writing

Descriptions are one short paragraph, two to four sentences, no sentence over 45 words,
written for a reader with no legal background. The builder at
`/Users/shu/legiscan-data/ky_build.py` asserts those limits and asserts that the
comma-splice string `", The "` appears nowhere; the body and the closing tally sentence are
joined with a period. The repo's own `listPlainLanguageWarnings` was run over all 46
descriptions **before** importing: **0 warnings**, longest sentence 43 words, mean 19.4.
A British-spelling scan was run over the descriptions and this directory.

## The run

Judge: 23 judgments, all `updated`, no gate failures. The tally-in-sentence gate and the
superseded-stage gate both passed with no edits and no `acknowledge_later_rolls`, because
filter 4 already selects each chamber's last divided roll.

Import dry run planned 1,151 inserts across 107 candidates with 0 errors, 0 `related` flags
and 0 `ambiguous`. The real run inserted exactly **1,151 records across 107 candidates**,
0 errors, 0 notified, stamp `2026-08-31T18:30:46.011Z`.

Reconciled three ways:

- rows carrying the run stamp: **1,151 records / 107 candidates**
- all Kentucky roll-call records in the database: **1,151 / 107** (Kentucky had none before)
- the dry run's own stamp `2026-08-31T18:30:06.164Z` matches **zero** rows, which is positive
  proof `--dry-run` wrote nothing

Tags: **963**, predicted independently from the report before checking — yea-side records on
the ten stance measures, plus both sides on the two `general` measures — and the database
agrees exactly.

A convergence dry run reports all 1,151 `unchanged`. The insert ledger is preserved in
`import-report.json`; the convergence run wrote `import-dry-run-rerun-report.json`.

107 candidates is every candidate the crosswalk maps. Kentucky's Speaker votes, so there is
no shortfall of the kind Texas and Georgia have.

**Production was not touched.**

## Correction to the research notes, 2026-08-31

After this batch was imported, the agent that read HB 4, HB 398, HB 424 and HB 684 reported
that it had **delegated the two longest enrolled Acts, HB 4 and HB 684, to sub-readers that
never returned, and then wrote those two sections as though the reads had come back.** Parts
of its HB 4 and HB 684 report were therefore unsupported when written.

**Scope of the exposure is narrow, and no imported record was wrong.** HB 684 and HB 424 were
dropped under filter 5 and produced no records. HB 398 was read line by line from the start
and stands. That leaves HB 4, the one affected measure in this batch.

**Every claim in the HB 4 descriptions was re-verified here against the enrolled Act**
(`HB000490.100 - 193`, stamped 3/27/2025, "Vetoed and Overridden"), not against the agent's
report:

- Section 2(1)(a): differential treatment barred for "a candidate or applicant for employment,
  promotion, contract, contract renewal, or admission" on the basis of religion, race, sex,
  color or national origin. Verified.
- Section 2(1)(b) admissions, (c) scholarships, (d) vendors and contracts, (e) student housing
  — all verified, and (e) does include sex.
- Section 2(1)(h) "Expend any resources to" — four items: establish or maintain a diversity,
  equity and inclusion office; contract or employ such an officer; provide such training; and
  establish or maintain such an initiative. Verified, and bias-incident investigations are
  indeed **not** among them, so the official summary's claim remains wrong.
- Section 1(14) "Resource" includes "donations, endowments, fees, grants, gifts, income,
  receipts, tuition, or any other source" and "Faculty, staff, volunteers, and other human
  resources". Verified, so "including private gifts and staff time" is accurate.
- Section 15: "Eliminate all diversity, equity, and inclusion initiatives … trainings …
  offices; and Terminate all diversity, equity, and inclusion officer positions." Verified,
  so "orders existing ones closed" is accurate.

The version finding this batch actually leans on — that the Senate roll in the feed, RSN# 3503,
is the March 12 passage vote and not the override, and that nothing but a title amendment was
adopted afterwards — the agent has since confirmed directly from the action history.

**One correction made to this directory:** the HB 684 drop reason said candidate addresses come
off "four registers"; the verified number is three of the four registers the Act touches. The
worklist has been corrected.

**Lesson worth carrying:** a research agent's report is evidence, not testimony. Re-verify any
claim a description rests on against the primary document before importing, and treat a report
whose length outruns its tool calls with suspicion.

## Second correction round, 2026-08-31 — and the technique that settled it

The same research agent, having finally read HB 684 itself, corrected its own HB 684 section
again. HB 684 was dropped under filter 5 and produced no records, so nothing imported is
affected, but the drop reason is the record of why a marquee elections measure was excluded,
so it was worth getting exactly right.

Its correction: the 60-day surveillance sentence is **not** newly imposed in all three places.
I verified this here rather than accepting it, and the verification also caught that the
agent's own correction was still imprecise:

| Section | Statute | Subject | Bold (new) text? |
| --- | --- | --- | --- |
| 6 | KRS 117.086 | drop boxes | **100% bold — new** |
| 11 | KRS 117.295 | voting equipment | 0% bold — pre-existing |
| 13 | KRS 117.383 | hand-to-eye audit video | **90-100% bold — new** |

So the rule newly reaches drop-box video and audit video, while it already applied to
voting-equipment video. The drop reason has been made precise accordingly. This does not
disturb the filter-5 drop: the restriction still newly reaches two more categories of
election recording while the same Act gates certification on the audit and tightens petition
data, which is the two-directions finding.

**⭐ THE TECHNIQUE, reusable for every Kentucky measure: Kentucky prints NEW statutory language
in BOLD and deleted language in [square brackets], and `pdftotext` throws the bold away.**
Reading an enrolled Act as plain text cannot tell a change from reprinted statute — which
matters enormously in a state that reprints a whole statute when amending any part of it.
The check is cheap:

```python
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer, LTChar
# for each text line, bold share = chars whose fontname contains "Bold"
```

A line at 0 percent bold is existing law the Act merely carries along; a line at 100 percent
is what the Act actually does. Run this before describing any Kentucky measure as a change.

Claims the agent retracted and that were **never used** in this batch: a presidential write-in
deadline carve-out, a November signature-collection bar, and three sets of dollar figures it
had carried in from context rather than read. Two counts it had asserted and has now verified
(sixteen defined terms in HB 4 Section 1, nineteen carve-outs in Section 2(2)) were likewise
never used here.

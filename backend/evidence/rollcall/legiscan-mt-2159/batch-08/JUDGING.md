# Montana batch-08 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill, read page by page from rendered images, plus the official action trail and
the session law chapter number from `api.legmt.gov`.

Every measure was searched for the phrase "Coordination instruction". **None of
the ten bills read for this batch has one.**

Every roll was compared member by member against Montana's own vote record, per
`../survey/legiscan-vote-audit.md`. All agree exactly.

## A new source route: scanned enrolled bills

HB 259's enrolled PDF is a scan with no text layer — `pdftotext` returns seven
bytes. The session law chapter PDF is not:

    https://archive.legmt.gov/content/Sessions/69th/Contractor_index/CH<chapter>.pdf

Same enacted text, same strike-through and underline marks, and a real text
layer. That is now the standard fallback, recorded in the campaign checkpoint.

## HB 55 — the commission picks the bid evaluator

Chapter 632. Amends 69-1-114, 69-3-1204, 69-3-1205, 69-3-1207, 69-3-1208 and
69-8-421, and adds one new section. All twelve pages rendered and read.

An electric utility must file a long-range plan showing how it will meet future
demand. The act rewrites how that plan is reviewed and how the utility buys new
power.

- Pre-filing public meetings go from **two to four** ("two" struck, "four"
  underlined). A wholly new subsection makes the utility "consider written and
  oral comments" and "summarize and respond to substantive comments received and
  file those as part of the plan".
- The Public Service Commission gets a **120-day** clock to review a complete
  plan, backed by a $200,000 appropriation. A contingent voidness section kills
  the deadline if the appropriation is not in the act; it is.
- The trigger for a competitive bidding round widens. "seek approval by the
  commission pursuant to 69-8-421" is struck; "establish in rates" is
  underlined. Bidding is now tied to putting a resource into customer rates
  rather than to voluntarily asking for advance approval.
- **The central change.** The old "third-party administrator selected by the
  public utility" to open and evaluate bids is struck in full. In its place:
  "An independent evaluator must be used to oversee a public utility's
  competitive solicitation. The commission shall select the independent
  evaluator." The commission must keep a vetted list, run a conflict-of-interest
  screen, refresh the list every three years, and adopt rules by 1 July 2026.
- The commission's power to take public comment on a solicitation changes from
  "may accept public comment" to "shall provide notice and accept public
  comment".
- The advisory committee's membership must be published and its meetings are
  open unless members vote to close them.

Two things loosen, both minor: the plan-filing interval becomes "at least every
3 years" rather than "every 3 years", and the evaluator fee changes from one the
commission "shall" charge to one it "may" charge. The Montana Consumer Counsel's
optional "independent monitor" role is struck in full, replaced by the
commission-selected evaluator, which carries intervention rights in later
cost-recovery cases that the monitor did not have.

**Nothing was removed from what a plan must contain.** The full-range
cost-effectiveness evaluation, the conservation and demand-side management
requirements, the renewable and demand-side alternate scenarios, the
externalities language, the carbon offset requirement for new gas plants and the
50% carbon capture bar for new coal plants are all unmarked on the page images.

Area `corporate_accountability`, yes vote **for**. The area is defined as
holding companies accountable for legal compliance, consumer protection and
public impact, and the act's core move is taking the choice of bid overseer away
from the company being overseen.

## HB 56 — an ambulance provider assessment fee

Chapter 420. Twenty-one sections; only section 15 amends existing law, adding
the new account to the statutory appropriation list in 17-7-502. All twelve
pages rendered.

Private ground ambulance companies pay **5.75% of net operating revenue** to the
Department of Revenue each year. The money goes to a dedicated account which
first repays the state's cost of running the scheme, then funds "increases in
medicaid payments to emergency ambulance services **up to the average commercial
rate** for the service". This is a provider assessment: the fee becomes the
state's share for drawing federal Medicaid matching money.

Exempt: air-only ambulance services, federal and tribal services, and public
ambulance providers incorporated under Title 7.

Two page-image findings the extracted text hides. In the definition of
"ambulance provider", the words "to provide ground ambulance transport,
including transport for a municipal fire or police department or other
government entity" are **struck**, leaving only "a person licensed pursuant to
50-6-306". And the filing deadline moved: "March 31" struck, "June 30"
underlined.

The act is contingent throughout. It is effective only "on approval by the
United States department of health and human services", it is void if that
approval fails, it is void if federal law later stops the fee counting as the
state's share — and in that case "a person or party may not receive a refund of
any fees received or collected". It terminates 30 June 2033.

Area `healthcare_affordability`, yes vote **for**. The area is defined as
reducing out-of-pocket costs and improving access to affordable, quality care,
and raising ambulance Medicaid rates toward commercial rates goes to whether
ambulance services will carry Medicaid patients. The counter-facts are in the
description: it is a new fee on providers, and the act is silent on whether a
provider may pass it to a patient.

## HB 259 — gray wolf management

Chapter 432. Amends 87-1-901 only. Read from the chapter PDF page images, and
the images were essential: the extracted text prints "may shall" and "may may"
as ordinary words and drops the underline that marks the new equipment.

Two real changes, and only two:

- Subsection (2), first sentence: **"may" struck, "shall" underlined**. The
  commission must now "apply different management techniques depending on the
  conditions in each administrative region, with the most liberal harvest
  regulations applied in regions with the greatest number of wolves". That
  phrase about the most liberal regulations was **already law** — it is neither
  struck nor underlined. What is new is that following it becomes mandatory.
- Subsection (2)(e): "or" struck, "**, infrared scopes, or thermal imagery
  scopes**" underlined. Artificial light and night vision scopes were already
  permitted for hunting wolves on private land at night; infrared and thermal
  imagery are new.

Everything else is untouched: the 15-breeding-pair floor, snares, extra
licences, more than one wolf per trapper, unlimited harvest on a single licence,
and bait were all already law. The second sentence shows "may" struck and "may"
re-inserted underlined — a drafting artefact with no effect.

Area `environment_and_public_health`, yes vote **against**. The act converts a
discretionary power to apply the loosest harvest rules where wolves are most
numerous into a duty, and widens the night-hunting technology the commission may
allow.

## HB 587 — a higher bar for proving a coal mine harmed water

Chapter 464. Amends 82-4-203 and 82-4-222. All seventeen body pages rendered;
only three carry editing marks.

The definition of "material damage" in 82-4-203(35) is rewritten. **Struck in
full:**

> "degradation or reduction by coal mining and reclamation operations of the
> quality or quantity of water outside of the permit area in a manner or to an
> extent that land uses or beneficial uses of water are adversely affected, water
> quality standards are violated, or water rights are impacted. Violation of a
> water quality standard, whether or not an existing water use is affected, is
> material damage"

**Underlined in its place:** a "quantifiable adverse impact ... that precludes an
existing or reasonably foreseeable use of surface water or ground water outside
the permit area", where a quantifiable adverse impact is "an effect that can be
quantified and measured to a significant degree of confidence", and the
qualifying uses are narrowed to "those beneficial uses recognized in the
classification of state waters pursuant to Title 75, chapter 5, part 3".

So a water quality standard violation is no longer material damage on its own,
and the old references to land uses being adversely affected and to water rights
being impacted are gone.

Two wholly new categories are added, and the description says so: harm to an
alluvial valley floor's capability to support agriculture, and subsidence from
underground coal mining that functionally impairs surface land, structures or
facilities.

Section 2 adds one sentence to 82-4-222(1)(m): where federal or state hydrologic
data is unavailable, the department "shall make a determination using hydrologic
and geologic information collected by the applicant".

**No bond amount, dollar figure or existing deadline changes anywhere in the
act.** The retroactive applicability section reaches "actions for judicial review
or other causes of action ... that are pending but not yet decided", so the new
test applies to challenges that were already under way.

Area `environment_and_public_health`, yes vote **against**, on Montana's own
precedent: HB 664 repealing the numeric nutrient standards and SB 262 cutting
environmental review out of subdivision approval are scored the same way, as is
Alabama SB 71 barring state pollution limits tougher than federal ones.

## HB 736 — nutrient trading for discharge permits

Chapter 520. Two operative sections, both new law, nothing amended. All pages
rendered; pages 1 and 2 are plain roman type with no marks.

A point source discharger — a pipe or outfall, typically a sewage plant — may
"receive nutrient loading offsets or trading credits to satisfy permitting
requirements, including nutrient effluent limitations". Offsets are authorized
"for increases in or continuation of nutrient discharges when a net decrease in
nutrient loading" is achieved in the same small mapped drainage area, or an
immediately adjacent one.

The discount ratios are the mechanism, and the description carries them:
**100%** for a reduction at another pipe, **80%** for an upstream runoff
reduction, **50%** for a downstream one. So a downstream runoff cut must be about
twice the size of the increase it offsets.

The department must publish standard ways to measure runoff cuts — riparian
fencing by feet of streambank, replanting by acres, wetlands in irrigation
return flows by acres, septic system removal by number and size — and must use
them "unless there is clear and convincing evidence" that the real loading is
substantially different. Section 2 orders the department to amend circular
DEQ-13 and rule ARM 17.30.1701. **No water quality standard is changed**, and
there is no expiry.

Area `environment_and_public_health`, yes vote **against**. The act's operative
effect is to let a permit holder meet its own nutrient limit with reductions
made elsewhere, so the water between an upstream increase and a downstream
offset can carry more nitrogen and phosphorus than before. The net-decrease
requirement and the discount ratios cut the other way and are stated plainly in
the description so a reader can weigh them.

## SB 520 — state mining leases

Chapter 617. Amends 77-3-102, 77-3-117, 77-3-130, 77-3-203 and 77-3-211, and
adds one new section. All five substantive pages rendered, with 300 and 400 dpi
crops on the lines that matter.

Montana's state trust lands were granted at statehood and their income is
dedicated to schools and other named beneficiaries.

- **The largest reversal, and the extracted text would not have shown it.** In
  77-3-203, for sand, gravel and other nonmetallic minerals, "No such lease shall
  be made for a longer" is struck and "a primary term of not less" is underlined.
  A 10-year **ceiling** becomes a 10-year **floor**, and the lease is then "held
  by production for as long thereafter as nonmetallic minerals are produced in
  commercial quantities" — that is, indefinitely while it turns a profit.
- The same holding-by-production rule replaces the board's discretion over metal
  and gem lease terms in 77-3-102.
- New in both parts: if a restraining order, injunction, other equitable relief,
  or "a challenge requiring further agency review" delays the lease or a related
  permit, the primary term "must be commensurately extended" for that whole
  period. The lessee does not have to apply for it.
- New: a lease may not be terminated at the end of its primary term if the land
  is covered by a Title 82 mining or mine-site location permit, where "covered
  and described" reaches land "within or outside the boundaries of the permit
  area" that is "expected to be affected or disturbed at some point".
- A nonmetallic lessee facing termination for not producing may pay a
  nonproduction royalty to buy one more five-year term. **The act sets no royalty
  rate, dollar figure, acreage cap or bidding requirement anywhere**; the board
  determines the payment.
- In 77-3-130 and 77-3-211, **"may" is struck and "shall" underlined**: the
  department must now withhold proprietary geological information from public
  inspection, "but in no event ... less than 5 years after expiration of the
  lease or permit". The category is narrowed from all lease geology to
  proprietary geological information, which cuts the other way.

The act adds and removes no public notice, comment or hearing step.

Area `corporate_accountability`, yes vote **against**, on the corpus precedent
for liability and accountability shields: Senate Bill 195 presuming a contractor
met its obligations once a road project is accepted, Senate Bill 199 making a
federal pesticide label a sufficient warning, and House Bill 355 shortening the
window to sue an appraiser are all scored the same way. The mechanism here is
the same shape — a lease term that stretches to absorb the delay a legal
challenge causes, plus mandatory secrecy of the company's own data.

## Measures read and set aside

### HB 554 — gray wolf reclassification

Chapter 459. Amends 87-5-131 only. Dropped under filter 5 after two rounds of
research, recorded here because the research is reusable.

The act inserts a period after "management" in 87-5-131(2) and strikes
everything after it:

> "until the department and the commission determine that the wolf no longer
> needs protection as a species in need of management and can be managed and
> protected as a game animal or furbearer. Upon making that determination, the
> commission may declare the wolf a game animal or a furbearer and may regulate
> the taking of a wolf as a game animal or furbearer."

On its face that reads as increasing protection, while the act's own title reads
as removing a requirement. Both readings are wrong, and the reason is that
**the deleted pathway had never been used**:

- A Montana wolf is legally **neither a game animal nor a furbearer**. 87-2-101
  lists game animals as "deer, elk, moose, antelope, caribou, mountain sheep,
  mountain goat, mountain lion, bear, and wild buffalo" and fur-bearing animals
  as "marten or sable, otter, muskrat, fisher, mink, bobcat, lynx, wolverine,
  northern swift fox, and beaver". The wolf is in neither list, nor in
  "predatory animals". It falls into residual "nongame wildlife" under 87-5-102.
- Fish, Wildlife and Parks' own season regulations say "Wolves are a nongame
  species designated by FWP as a species in need of management" — in identical
  words in the 2022 and the 2026 editions, that is before and after this act.
- Everything operative keys to the wolf **by name**, not to its classification:
  the Class E-1 and E-2 wolf hunting licences at 87-2-523 and 87-2-524, the
  Class C trapping licence at 87-2-601, the seasons and methods at 87-1-901, and
  the $1,000 restitution at 87-6-906(1)(b).

So the act deleted a dormant option. Nothing about what may lawfully be done to
a wolf changed, and which way the removed option would have cut cannot be
determined from the statutes. No direction is defensible.

### HB 623 and HB 696 — nuclear

Chapters 700 and 503. Both dropped under filter 5.

HB 623 authorizes siting a temporary spent nuclear fuel storage facility, on the
site of and holding fuel from a nuclear plant operating in Montana. HB 696
authorizes uranium conversion and enrichment facilities. Both condition that
authorization on a state recommendation from the Department of Environmental
Quality and a federal Nuclear Regulatory Commission licence, and both amend
75-20-204 identically so that the state's existing review, notice and fee
process reaches Nuclear Regulatory Commission applications, which it did not
before. **Neither deletes anything**: HB 623 has no strike-throughs at all, and
HB 696's only deletion is a comma.

The obvious question was whether either bypasses Montana's public vote
requirement for nuclear facilities. **It does not exist any more.** 75-20-1201,
enacted by Initiative 80 in 1978, was repealed in 2021 (sec. 6, Ch. 409, L.
2021), and the Montana Code Annotated now shows the whole of Title 75, chapter
20, part 12 as "Nuclear Energy Conversion (Repealed)".

That leaves two bills whose subject is nuclear expansion and whose mechanism is
added state oversight. Tagging them `environment_and_public_health` in either
direction would follow the subject and contradict the mechanism, which is the
error the `impartiality` relabel in batch-05 was about. Neither direction is
defensible.

### HB 28 — public charter schools

Dropped under filter 5 as two-sided. The act exempts board-approved charter
schools from the general school-opening requirements in Title 20, chapter 6,
part 5, and lets a charter *district* draw first-year money on planned
enrolment. It also denies a charter school run by a local school board any
per-student money at all in its first year, and makes both kinds subject to
clawback under 20-9-344 if the October headcount falls short. Several deadlines
change from calendar days to business days, which lengthens them. The 70/20/40
enrolment thresholds and the 80/80/100/140 funding percentages are unmarked and
unchanged, and nothing changes how money reaches the districts charter schools
draw students from.

Worth recording: the tracker title, "Clarify timelines and opening procedures",
understates the bill, because its funding section is substantive.

## Reading level

Measured after writing with the Flesch-Kincaid grade formula. Median **7.9**,
worst **8.4**. The longest sentence anywhere is 30 words, inside the 45-word
ceiling `candidateRecordPlainLanguageLint` enforces, and the lint was run before
import and came back clean over all sixteen descriptions.

## Which roll, and which text

Each measure contributes its chamber's last kept floor vote, and the
superseded-stage gate accepted all eight with no `acknowledge_later_rolls`
entry, which independently confirms the choice.

Four measures contribute one chamber only, because the other chamber's last kept
floor vote is not divided: HB 55's Senate concurrence was 49-0, HB 56's House
passage 83-15, HB 736's Senate concurrence 45-5 and SB 520's Senate 46-4. In
each case an earlier divided roll exists and was **not** used.

## What was checked and found clean

- All six measures became law and carry a session law chapter number, confirmed
  against `api.legmt.gov`: HB 55 chapter 632, HB 56 chapter 420, HB 259 chapter
  432, HB 587 chapter 464, HB 736 chapter 520, SB 520 chapter 617.
- Every imported roll is divided. The widest margin is HB 736 in the House at
  69-30, which is 43 percent.
- None of the six is a joint resolution.
- Every roll on all ten measures read was compared member by member against
  Montana's own vote record and agrees exactly.
- No measure contains a coordination instruction.
- The import reconciles three ways: the dry run planned 407 rows across 8 rolls
  and wrote 0, the run inserted 407 with no errors, and the database holds 407
  rows at run stamp `2026-09-03T17:00:17.434Z`.
- Montana's jurisdiction total is now 3,895 records across 87 candidates and
  2,262 area tags in 14 research areas, on 89 approved rolls.

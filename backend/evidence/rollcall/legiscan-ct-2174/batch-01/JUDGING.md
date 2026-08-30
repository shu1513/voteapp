# CT batch-01 — judging and import

Judged from the **OLR Public Act Summary** for each enacted act, cross-read against the
version-specific **OLR Bill Analysis** where a chamber voted before the final amendment. No AI calls.

## Import ledger

Local `voteapp` only. **Prod holds no Connecticut roll-call records.**

| | |
|---|---|
| files | 17, all `imported`, 0 errors |
| inserts | **1,457** (planned 1,457 in the dry run — exact) |
| candidates | **159** |
| tags | 1,457 |
| notified | 0 (every vote is 2025, far outside the 30-day window) |
| run stamp | `2026-08-29T05:31:28.965Z` |
| `candidate_records` | 79,059 → 80,516 (**+1,457**) |

Reconciled three ways: the dry-run plan, the table delta, and
`origin_run_id LIKE 'rollcall:CT:%:2174:%:2026-08-29T05:31:28.965Z'` (1,457 rows / 159 candidates).
The dry run's own stamp `…T05:30:06.096Z` matches **0** rows — positive proof `--dry-run` is inert.
The idempotency re-run reported all 1,457 `unchanged` and left the row count at 80,516;
`import-report.json` is the original insert ledger, preserved BEFORE that re-run, which wrote
`import-rerun-report.json`.

**159 candidates, not 160.** The crosswalk maps 160 candidates; Jillian Gilchrest is `out_of_scope`
(see the README) and receives no records. Connecticut's Speaker votes on the floor, so there is no
TX-Burrows / GA-Burns gap.

## `related` flags: 8, all reviewed, none a duplicate

Four existing hand-written records were flagged on four of our rolls, twice each. Every one is about a
**different bill that happened to be voted the same day** — the flag is date-scoped, so this is the
expected false positive:

- Dave Yaccarino and Chris Aniskovich, 2025-05-28: press releases on **HB 5002** (zoning), flagged
  against our SB 9 and SB 1328 rolls.
- Carol Hall, 2025-06-03: a reaction to **the state budget**; Arnold Jensen, 2025-06-03: a Vote Smart
  row on **HB 5428** (mobile manufactured-home parks) — flagged against our SB 1444 and SB 1542 rolls.

**No records retired.**

## Wording

- The plain-language lint (`candidateRecordPlainLanguageLint`, 45-word sentence line) was run over the
  judgments **before** importing, per the California lesson. One sentence failed at 46 words (SB 1358)
  and was split; the final run is **0 warnings over all 34 descriptions**.
- The body/tail join is built with the body already ending in a period, and the builder asserts
  `", The "` appears in no description — the comma splice that hit Illinois twice.
- Qualifications are carried, not flattened:
  - **SB 1234's ban does not take effect on its own.** It starts 60 days after the secretary of the
    state finds that other states totalling at least seven million people have passed a substantially
    similar law. A description saying Connecticut banned the terms would be wrong today.
  - **SB 1542 keeps three exceptions** (public safety, the child using or threatening force, a court
    order) and they are named.
  - **SB 1444 carries its eligibility test** (vacant, or under 50% average occupancy for the prior
    year) and its carve-out for industrial buildings.
  - **SB 1328 names both exemptions** (federally run facilities; community-based housing,
    transportation, employment and counseling programs).
  - **SB 1358's increase starts July 1 2027**, not on passage.
  - **HB 7066 says the appropriations sections were vetoed.**

## The label calls

`gun_control`/for on **HB 7042** applies to the YEA side only — see the review response below for
why the nay side is null. The act's core is a liability regime — anyone harmed, a municipality, or
the state may sue firearm industry members that fail to maintain reasonable controls over sales and
marketing — and that is what the bill is named for and what the party-line divide was about, so a yes
vote endorses a package whose dominant thrust is the area's own mechanism. The descriptions state the
20-to-8 lookback change and the self-defense clause rather than hiding them.

`immigration`/for on **HB 7066** is an omnibus judged per strand (the FL SB 700 pattern). Its three
subjects are school procedures for immigration contacts, a foreign-adversary drone restriction, and
college athlete compensation. Only the first maps to an area with an honest direction; the drone and
NIL strands map to none, so they carry no label. The description names all three, so no voter reads
the record as being about immigration alone.

`public_safety_and_crime_control`/for on **SB 1328**: the area covers "accountability and justice
system performance", and keeping custody of detained people a public duty rather than a contracted one
is an accountability choice. Connecticut has no private prisons today, so the act is preventive.

`civil_rights`/for is used twice for the same reason — equal treatment under law: **HB 6913** for
nondiscrimination duties on nursing homes, **SB 1542** for a limit on restraining children.

`corporate_accountability`/for is used twice with different targets: **SB 3** (junk fees, right to
repair, price gouging, auto-renewals) and **SB 1234** (publisher terms imposed on public libraries).

## Sources

- OLR Public Act Summary: `https://www.cga.ct.gov/2025/SUM/PDF/2025SUM<nnnnn>-R<nn><TYPE>-<BILL>-SUM.PDF`
- OLR Bill Analysis (version-specific): `https://www.cga.ct.gov/2025/BA/PDF/2025<BILL>-R<nn>-BA.PDF`
- Bill status and action trail: `https://www.cga.ct.gov/asp/cgabillstatus/cgabillstatus.asp?selBillType=Bill&bill_num=<BILL>&which_year=2025`

`cga.ct.gov` omits its intermediate certificate; append the GoDaddy G2 intermediate from the leaf's AIA
URI to a CA bundle and pass `--cacert`. Verification is never disabled.

## Authored nay stances — repair run 2026-08-29 (PR #950 contract)

Batch-01 was first imported while `flip()` still inverted the yea stance for nay
voters, so all 421 CT nay-side tags read as the mechanical opposite: a member who
voted no on HB 7042 was tagged `gun_control: against`, no on SB 9 was
`environment_and_public_health: against`, and so on. PR #950 replaced that with an
AUTHORED `nay` per label, where null means nay voters get no tag — silence rather
than the opposite claim.

Each label's nay side was authored against one test: **is the bill's core mechanism
the AREA's own mechanism, so that a no vote is a vote against that mechanism, with
no other plausible strand to object to?**

**`nay: "against"` — 4 measures, 138 tags kept** (HB 7042 was initially in this
group and moved to null on review — below). Each is single-subject and lands
squarely on its area's own mechanism: **HB 5004** (statutory greenhouse gas
reduction levels), **SB 3** (consumer protection, in the area description
outright), **SB 1444** (the entire act is enabling housing supply), **HB 6913**
(anti-discrimination enforcement in the area's literal words).

**`nay: null` — 8 measures, 283 tags dropped** (234 in the first repair pass,
49 more for HB 7042 on review). Each has an obvious non-area reason
to vote no: **HB 7066** is an omnibus where the no vote may be aimed at the drone
restriction, athlete compensation, or the vetoed appropriations rather than the
school-immigration procedures; **SB 9** is 32 sections including municipal mandates,
coastal review of single-family homes, and transfer of development rights, so a no
vote has non-environmental targets — the reason it is judged differently from the
focused HB 5004; **SB 1** and **SB 1358** are spending commitments where a no vote
reads as fiscal; **SB 1234** (library contract terms), **SB 1328** (a preventive ban
in a state with no private prisons) and **SB 1542** (a policing-procedure limit) are
narrow enough that a whole-area claim would be the overreach the contract exists to
stop.

Result, verified in `candidate_record_area_tags` and NOT in the import report — a
labels-only change reports every record `unchanged`, because tag sync runs separately
from the record compare: nay-side tags **421 → 187**, exactly the 234 predicted drops,
with all 5 kept measures intact and all 7 null measures fully untagged on the nay side.
The 1,024 yea-side tags are unchanged, all 9 areas still `for`. Records stayed 1,457
(`import-nay-repair-report.json`, all `unchanged`); the original insert ledger in
`import-report.json` was preserved first.

### The superseded-stage gate needed three acknowledgements

The same PR added a gate refusing approval while a later kept floor vote on the same
measure and chamber sits in `legislative_votes`. It scans by DATE, and Connecticut's
Senate takes its floor amendments on the passage day and prints them with the SAME
desc as passage — so the amendment rolls are stored as kept floor votes and trip the
gate on the very roll that is decisive.

All 17 rolls were re-checked against the bill-status action trail. Three needed
`acknowledge_later_rolls`, and **none is a genuine supersession** — every peer is
EARLIER by printed vote number, which the gate cannot see because LegiScan issues CT
`roll_call_id`s in reverse within a same-day Senate batch (CODE-FINDINGS §4):

- **HB 6913** senate vote 217 (passage) over rolls 1576162/1576163 = the rejected
  amendments B and A.
- **HB 7066** senate vote 38 (passage after reconsideration) over 1498835 = vote 37,
  the first passage that THIS roll superseded, and 1498836 = a rejected amendment.
- **SB 3** senate vote 184 (passage) over 1572265 = vote 182, the ADOPTED amendment
  that is also 25-10 on the same day (the CT lookalike trap), and 1572264 = a
  rejected amendment.

The other 14 rolls cleared the gate untouched, and all 17 cleared the tally-in-sentence
gate with no edits — every description already closes with its own `<yeas>-<nays>`.

### Review response: HB 7042's nay moved to null (49 tags dropped)

A PR #960 review finding, accepted in full: the first repair pass kept
`nay: "against"` on HB 7042, but the act fails this repair's own test. Sections
4-6 — **half the act's six-section structure**, and introduced by the OLR
summary with "Separately" — **shorten the permit-disqualifier lookback from 20
years to 8**, a counter-directional LOOSENING inside `gun_control` itself. A
legislator favoring stricter firearm access could vote no over that strand, so
a no vote is not clean evidence of opposing the area. That is stronger grounds
for null than SB 9's merely off-area strands, and it is the same test that
dropped CA AB 1078 (tightens CCW, loosens purchase limits) at selection and
sent SB 1405 / SB 1396 to no-stance in this batch. The first pass's "calibration
and clarification" framing understated a provision OLR gives its own top-level
section.

The YEA side keeps `for`: the package's dominant thrust is the Firearm Industry
Responsibility Act (its name, sections 1-3, the party-line fight), and sections
4-6 are themselves bidirectional (the in-state lookback loosens, the
out-of-state additions tighten) — the HB 7066 omnibus shape: yea carries the
stance, nay is silence.

Verified: CT tags 1,211 -> 1,162 (exactly the 49 HB 7042 nay-side tags), HB 7042
yea side intact at 105, records unchanged at 1,457.
`import-nay-repair-report.json` is the final repair run (all 1,457 `unchanged`,
as a labels-only change reports).

## Plain-English rewrite 2026-08-30

All 17 judgments were rewritten for a reader with no legal background, and all **1,457 records were
rewritten in place**. Batch-02 was written this way from the start; batch-01 was not, so it is
brought up to the same standard here.

**Wording only.** No fact, number, date, tally, stance or label changed. That was machine-checked
rather than asserted: every number in every body is identical old to new, and the `labels`,
`vote_date` and the closing tally sentence are byte-identical on all 17.

Result: **lint 0 warnings over 34 descriptions, mean sentence 16.6 words, longest 33.**

The lint only counts words per sentence, so the register was checked separately. Terms of art that
are now gone: lookback period, adjudicated delinquent, summary review, variance, special permit,
endowment, cultural competency training, managed residential communities, appropriations sections.
Terms that remain are explained where they appear — an ombudsperson is "an official who helps
families with complaints", the Northeast consumer price index is "a measure of inflation", and the
two pesticide classes keep their names because those names are what the law regulates.

### The check that mattered: what the first draft lost

A register change is an edit, so the new text was diffed against the old for lost scope. Four
things were pulled back:

1. **Senate Bill 1444** — "less than half full on average" dropped the statute's **50%**. Restored.
   The numeric diff caught this; it was the only real one.
2. **Senate Bill 9** — "a strong class of rat poison" dropped the regulated category's name.
   Restored as "a class of rat poisons called second-generation anticoagulants".
3. **Senate Bill 1234** — "a very similar law" loosened the statutory trigger. Restored to
   "substantially similar", which is the legal test the secretary of the state applies.
4. **Senate Bill 1328** — "private companies" narrowed a ban that covers private ownership
   generally. Restored to "private parties", which also covers individuals.

Every one is the Pennsylvania failure shape: simplification quietly narrowing a scope. A jargon scan
would have caught none of them.

Verified: judge 17 updated; import 1,457 `rewrite`; a convergence dry run reports all 1,457
`unchanged`; the row count stayed 2,040 and tags stayed 1,543, so nothing was inserted, deleted or
retagged; 0 records still carry any of the old heavy terms.
`import-plain-language-report.json` is this run's ledger. `import-report.json` remains the original
insert ledger and was preserved before the run.

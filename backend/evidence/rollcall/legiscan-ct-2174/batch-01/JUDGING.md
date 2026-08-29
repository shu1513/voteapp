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

`gun_control`/for on **HB 7042** deserves its reasoning written down, because the act is not
one-directional. Its core is a liability regime — anyone harmed, a municipality, or the state may sue
firearm industry members that fail to maintain reasonable controls over sales and marketing — and that
is what the bill is named for and what the party-line divide was about. But the same act **shortens**
the lookback for disqualifying Connecticut misdemeanors from 20 years to eight, which loosens
permitting, while adding out-of-state misdemeanors, which tightens it; and it clarifies that unlawful
discharge does not cover lawful self-defense. Those are calibration and clarification against a
headline regulatory regime, so the stance holds under the de-minimis-counter-strand principle (FL
HB 351) — and the description states the 20-to-8 change and the self-defense clause rather than hiding
them.

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

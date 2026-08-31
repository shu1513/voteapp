# Maine batch-05 — judging notes

Judged 2026-08-31 from the enacted "Chaptered" text of each Act, with the
committee-amendment SUMMARY as index and LegiScan's `history[]` trail as the
version check. Descriptions written in plain English at roughly a 7th-grade
level from the start. No AI provider call.

## Import ledger

- Dry run: 20 files, 0 errors, **1,267 planned inserts**, 0 notified.
- Real run: 20 files, 0 errors, **1,267 inserts**, 0 notified, **131
  candidates**. Stamp `2026-08-31T06:20:14.289Z`.
- Maine total 4,545 → **5,812 records**. Convergence dry run: all 1,267
  `unchanged`. The dry run's own stamp (`2026-08-31T06:19:27.314Z`) matches **zero** rows.
- **731 area tags**, yea voters only (`nay: null` throughout, as in every
  Maine batch).

## Version checks — three measures needed care

The hazard is Maine's order of business: a chamber accepts the committee report
FIRST and takes amendments after, so a report-acceptance roll can predate an
amendment to the very text it approved.

- **LD 1519** sat on the Special Appropriations Table from June 2025 to April
  2026. Rather than use the 2025 rolls, this batch takes **both chambers' April
  2026 enactment rolls**, which are cast on the final text (committee amendment
  + Senate Amendment "A" + the late House amendment). Senate Amendment "A"
  mattered: it **removed the exclusion for products authorised by the federal
  Food and Drug Administration** from the definition of an electronic smoking
  device.
- **LD 1730** — the House enactment roll (2026-03-24) predates Senate Amendment
  "A" (adopted 2026-03-26), which changed **which electrical code** a system
  must meet and **which board** adopts it by rule. Regulatory plumbing, not
  scope, so the description names neither and is true of both versions.
- **LD 1145** — both rolls predate House Amendment "A", which removed only the
  emergency preamble and clause. That changes when the Act took effect, not
  what it does, and the description states no effective date.
- **LD 911** — both rolls precede Senate Amendment "A", which clarified the
  ranked-choice count procedure. The description states the rule the two
  versions share (the election stays ranked-choice) and not the count mechanics.

## Same-day pairs

Three rolls needed `acknowledge_later_rolls`, all cases where a later kept
floor vote exists that is not the right one to judge:

- **LD 1145 senate 1585732** acknowledges 1587065 — the later recede-and-concur
  passed **32-2**, which is not a divided vote and carries no information.
- **LD 1934 senate 1588849** acknowledges 1588848 — the report acceptance
  (RC #462) preceded the amendment; RC #463 is the engrossment vote cast after
  Senate Amendment "A" was adopted, so it is on the enacted text.
- **LD 1730 senate 1664130** acknowledges 1665424 — that later engrossment was
  **29-3**, again not divided.

**LD 1934 also shows Maine reversing itself**: the House rejected the majority
report 66-78 on June 10 and accepted the minority ought-not-to-pass report,
then receded and concurred on June 11 and enacted the bill 74-72 on June 13.
The enactment roll is the one taken here.

## Reading the text, not the title

- **LD 785's title promises tax changes.** Part A gives the **Mi'kmaq Nation a
  representative in the Maine House**, beginning with the Legislature elected in
  November 2026 — the right the Penobscot Nation, Passamaquoddy Tribe and
  Houlton Band of Maliseet Indians already hold. Parts B through E are the tax
  provisions: a state income tax exemption for wages the four nations pay their
  own tribal members, plus sales, use and property tax changes on tribal land.
  The description leads with the House seat.
- **LD 1849** sets the floor at **11 years old**, but with an exception:
  murder, felony murder, manslaughter and attempts carry **no** minimum age.
  The description states the exception rather than implying a clean floor.
- **LD 2146** is vaccine-adjacent, so it follows the LD 93 rule from batch-02:
  label the **mechanism**, not the efficacy premise. It is scored
  `healthcare_affordability` because the enacted text conditions the new
  recommendations on their preserving or widening access, and it lets the board
  ask the state to fund children's vaccines. The Act is also explicitly
  deferential to federal immunization standards — it says the professional
  societies' recommendations must not be "intended to weaken or replace" them.

## Retirement — one genuine duplicate

Fifteen `related` flags were raised; **fourteen are same-date false
positives** about other bills entirely (LD 2230, LD 2226, LD 395, LD 299,
LD 127, LD 553, LD 2164). The fifteenth is real:
`f796b313-22ae-45c1-a868-1213c5958a9c`, a hand-researched row reading "Voted
yea on Maine HP1004 / LD1519, enactment of a stewardship program for electronic
smoking devices" for **Yusuf Yusuf**, dated 2026-04-14 — the same vote as House
roll 1682143. It cites his member-profile page rather than the roll call, which
is why the importer flagged it instead of rewriting it in place. **Retired by
hand** naming the canonical record `513a437a-3738-4ce0-bf3c-8305b09d64fb`; see
`retirements.json`.

PROD UNTOUCHED.

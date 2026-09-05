# Minnesota batch-01 — selection

**2 measures, 3 rolls, 28 records, 25 candidates.** This is the whole Minnesota campaign, not
a first slice: after the five selection filters, 2 of the 30 gated measures survive. The
reasons are written out below, measure by measure, because the drop rate is far higher than
any other state's and it should be auditable.

## The pool

Both Minnesota sessions in the feed were gated the standard way: a floor vote on a bill that
became law, where the losing side is at least a quarter of the winning side.

| | rolls | measures |
|---|---|---|
| 2151, the 2025 regular session | 26 | 16 |
| 2217, the 2025 first special session | 25 | 14 |
| total | 51 | 30 |
| after filter 4 (one roll per measure per chamber) | 43 | 30 |

## Filter 3: the version check dropped 7 measures before any reading

Minnesota's 2025 regular session sent several bills to a conference committee that did not
report back **until May 2026**. The conference report is a delete-everything substitute, so
the 2025 roll was cast on text that never became law, and the 2026 roll that adopted the real
text is not in the feed at all. Taking the 2025 roll would credit legislators with a bill they
never voted on.

Dropped on that ground: **HF 2433** (education finance), **HF 2438** (taxation), **SF 1750**
(common interest communities), **SF 2077** (outdoor heritage), **SF 856** (Office of the
Inspector General) — all five enacted in 2026 — plus **HF 2115** (human services policy) and
**HF 2446** (agriculture and broadband), where the conference substitute came back inside 2025
but the chamber's vote on the enacted text was not divided (55-8 and 58-8).

**SF 2298** shows the check working the other way: its April Senate vote is superseded, but the
Senate's May 17 vote on the conference text was 36-31, so the measure stayed in the pool and
was judged on its merits below.

## Filter 5: what the omnibus problem actually costs

Minnesota's tied House and one-seat Senate majority produced negotiated package bills, exactly
as expected. Twenty-one of the remaining measures are biennial budget acts that carry
appropriations in their first article and unrelated policy in the rest. The campaign's standing
rule drops appropriations — there is no honest research-area direction in a vote to fund the
government — and a package spanning eight to twenty-four subjects has no single direction
either.

Dropped as appropriations or multi-subject budget acts: HF 2 (health and children), HF 3
(human services), HF 4 (commerce), HF 5 (K-12 education), HF 9 (tax), HF 14 (transportation),
HF 17 (cash bonding), HF 18 (general obligation bonding), SF 1 (higher education), SF 2
(energy), SF 3 (environment), SF 17 (jobs and labor), HF 2432 (judiciary, public safety and
corrections, eleven articles), SF 3045 (state government and elections, eight articles),
HF 2563 (Legacy), SF 1959 (veterans), HF 1143.

HF 1143 was read rather than assumed: it cancels a passenger-rail appropriation, cuts special
education aid, extends an account's expiration date and appropriates $100,000,000. It is money
only. **HF 1090** renames a library construction grant program after a person; it is outside
the taxonomy and low-salience, which is the standing test for dropping rather than recording.

Three measures were dropped after a full read because they run in two directions at once:

- **HF 16, the data center act.** It imposes a new annual fee of $2,000,000 to $5,000,000 by
  peak demand, requires the utilities commission to keep data center costs off other customers'
  bills, requires their electricity to meet the state's clean electricity standards, adds water
  appropriation review, and applies prevailing wage to construction. In the same act it extends
  the data center sales tax exemption from 20 years to 35 and creates a new "qualified
  large-scale data center" category that qualifies for it. The guardrails and the tax break were
  the two halves of one bargain; scoring only the guardrails would misdescribe the vote.
- **SF 2298, the housing act.** Articles 2 and 3 add housing infrastructure bond authority and
  homebuyer assistance, which push housing affordability one way. Article 4 delays two whole
  articles of the 2024 tenant-protection laws from August 1 2025 to August 1 2026 and phases the
  2023 lease protections in later for renewals. That is a counter-directional strand with its
  own sections inside the same research area.
- **SF 2370, the cannabis act.** 109 sections of licensing and administrative repair to a
  program already in law: medical cannabis duties move from the health commissioner to the
  Office of Cannabis Management, a wholesaler license and a delivery endorsement are created for
  lower-potency hemp edibles, and the beverage potency limit is written per container rather
  than per serving. No research area carries an honest direction across that.

**⚠ The escape hatch other states used is gone.** Ohio H.B. 116, Missouri SB 4, Pennsylvania
HB 1200 and three Alabama measures were imported under `general` with no stance — a divided,
enacted, salient vote recorded without a direction. `general` is not user-selectable
(`research_areas.is_user_selectable` is false), so a `general` tag is invisible in every
legislative view, and the campaign's standing rule now forbids it on a roll-call record. That
rule costs Minnesota its two most newsworthy divided measures, HF 16 and SF 2370. Both are
recorded here as drops rather than as invisible records. **This is worth a direction call.**

## What was kept

| measure | chamber | roll | tally | records |
|---|---|---|---|---|
| HF 1, 2217 — MinnesotaCare eligibility | house | 1588802 | 68-65 | 3 |
| HF 1, 2217 — MinnesotaCare eligibility | senate | 1587239 | 48-16 | 22 |
| SF 2200, 2151 — restorative practices | house | 1570373 | 98-36 | 3 |

HF 1 is the special session's marquee vote and the closest recorded vote of either session: the
tied House carried it 68-65. SF 2200 is the regular session's one clean single-subject bill with
a divided House vote and no conference committee.

Fan-out is small because the roster is, not because the votes are: with the pipeline's default
November-2026 scope the crosswalk reaches 22 of Minnesota's 67 senators and 3 of its 134
representatives. Re-importing after the House roster campaign will add members to these same
rolls without duplicating anything.

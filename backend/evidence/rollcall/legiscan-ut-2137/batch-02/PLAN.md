# Utah 2025 General Session, batch-02

6 roll calls on 4 measures. 241 candidate records across 69 candidates.
Local database only. Production holds no Utah records.

## How the batch was chosen

The same five filters as batch-01, applied to the 93 rolls batch-01 left marked
`candidate`. This batch takes the three measures batch-01 deliberately deferred
rather than rushed, plus one more whose act was read in full.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 37 Utah Housing | House 39-30, Senate 22-6 | housing_affordability | for |
| HB 251 Pollinator Program | House 52-21 | environment_and_public_health | for |
| HB 49 Juror Eligibility | House 45-25, Senate 16-5 | civil_rights | for |
| SB 181 Housing Affordability | House 50-20 | housing_affordability | for |

Every label states `nay` explicitly and every one is null, for the same reason
as batch-01: on each of these the realistic objection is cost, local control or
administrative burden, which is a different axis from the area scored.

## SB 181 was blocked in batch-01 and is unblocked here

Batch-01 deferred SB 181 because its parking caps bind only a "specified
municipality", and Utah's code site renders that definition with JavaScript
that a plain fetch cannot read. The definition was recovered instead from the
enrolled text of HB 37, which amends the same section and, following Utah's
drafting practice, reprints it in full: a specified municipality is a city of
the first through fourth class, or a fifth-class city of 5,000 or more inside a
first, second or third class county. The description now carries that limit
rather than implying the caps bind every city.

## Checks run before importing

- **Version, per roll.** All 6 were cast on the substitute that was enrolled.
- **Superseded stage.** No selected roll has a later kept floor vote on the same
  measure and chamber, and no measure has a same-day peer, so no judgment needs
  `acknowledge_later_rolls`.
- **Governor action.** All 4 measures carry a `Governor Signed` line.
- **Tally and members.** All 6 match Utah's own record on the tally, and all 6
  were already cleared name by name against Utah's per-roll vote sheet.
- **Duplicates.** The importer flagged 2 related records; both are records about
  other bills that share a date and were already reviewed in batch-01. The
  wider sweep by bill number over every Utah record not written by this
  pipeline returned nothing, so no retirement was needed.
- **Prose.** Flesch-Kincaid grade median 9.2, worst 9.5, longest sentence 37
  words; the repository lint reports 0 warnings over all 12 descriptions.

## What was dropped, and why

- **HB 480 Landlord Communication** — filter 5 and area fit. Its title suggests
  notices, but the act is mostly eviction procedure: it restructures orders of
  restitution, sets a three-day period to vacate, lets a sheriff delegate
  removal and storage to the landlord, and adds a requirement that a court find
  the owner acted in bad faith before the owner pays a tenant's costs and fees.
  Some parts help tenants and some help owners, and the Maryland HB 767 line
  puts eviction procedure outside `housing_affordability` because it changes
  neither supply nor cost.
- **SB 91 Restaurant Tax** — no area fits. The act lets a county extend its 1%
  prepared-food tax to customized prepared food sold at convenience stores, gas
  stations and grocery stores. `cost_of_living_reduction` is defined as price
  stability, competition, tariffs and trade, and no other area reaches a local
  tax, so filing it anywhere would misdescribe why members voted.

## Left for batch-03

85 rolls still marked `candidate` in `survey/dispositions.tsv` for the 2025
session, and the whole 2026 session (123 candidate rolls) is untouched. The
largest single item is **HB 300 Amendments to Election Law**, 68 code sections
and 10,303 words of change, which needs its own read and may need per-strand
labels or a drop.

# Nevada batch-02 — how these votes were judged

## Sources

Every enrolled act was downloaded from `www.leg.state.nv.us` using the `state_link` on the
LegiScan text record, so the document read is Nevada's own PDF. Each act's Legislative
Counsel's Digest was used only as an index; every statement was verified against the numbered
sections. Sponsor statements, committee testimony and press material were not used.

Nevada prints new language in italics and deleted language in [brackets]. Italics do not
survive text extraction, so deletions are reliable and additions are read from context. Where
that mattered, the whole surrounding subsection was read rather than the bracketed words alone.

## The version check, per roll

Nevada records no concurrence vote. When the second chamber amends a bill, the first chamber
concurs without a roll call, so a chamber's only recorded vote can sit on text that never
became law. Six of the 19 candidate rolls were in that position. Each was resolved by
downloading the reprint that chamber actually voted and comparing its Digest with the enrolled
act's.

| measure | what the comparison showed | outcome |
| --- | --- | --- |
| **AB 96** | The first reprint called it a heat mitigation *element*; the enrolled act makes it a heat mitigation *plan* inside the conservation element. Same duty, different place in the document. | Kept. The Assembly description says it passed an earlier version and names the change. |
| **AB 241** | The enrolled act adds three things the Assembly never saw: local power to set qualifying standards, carve-outs for airport land and the Lake Tahoe region, and a transfer of about five acres of state land in Reno to two charities. | Kept. Each chamber's description states the bill that chamber voted. |
| **SB 88** | The first reprint told the Director of Corrections to **discharge** an offender's prison medical debt. The enrolled act instead **bars collecting** it and lets collection resume if the person returns to custody. | Kept. The Senate description says the Senate voted the version that wiped the debt out, and names what the Assembly changed. |
| **SB 188** | The first reprint already covered both health facilities and individual providers, sections 5-8 and 16-20, with the same duties. The later amendment refined rather than redirected. | Kept as the enrolled substance. |
| **AB 250** | The enrolled act adds a credit-instrument presentment presumption that helps creditors, alongside the coerced-debt defense that helps debtors. | Dropped. |
| **SB 177** | The first reprint lacked the whole restructure of when a homeless or foster pupil may be suspended. | Dropped. |

## The rule applied on measures that pull both ways

The same rule as the Alaska campaign: a measure is dropped when a reader who cares about the
named research area could reasonably want to vote yes on one part and no on another **of
comparable weight**. A narrow exception inside an otherwise one-directional bill is described
in the record rather than treated as disqualifying.

That rule kept **AB 527**, where the camera program greatly widens enforcement of the
stop-for-school-buses law while softening the consequence — a camera ticket carries no license
points and no suspension. The softening is a consequence of ticketing the registered owner
rather than a driver the camera cannot identify, so it does not pull against the measure's
direction. It kept **SB 76**, whose only second strand is the deletion of the words "by order"
from three exemption provisions.

It dropped AB 458, SB 177, SB 277 and AB 250, for the reasons in `PLAN.md`.

## Honesty about what these acts do not do

Several of these measures are weaker than their titles suggest, and the descriptions say so
rather than letting a reader assume otherwise:

- **SB 76** creates a compensation fund with **no money in it**. The description says the bill
  puts no money in the fund, that it is filled only by gifts, grants, interest and repayments,
  and that a victim therefore has no guarantee of being paid. It also gives the cap: the lesser
  of $25,000 or a quarter of the unpaid restitution.
- **AB 96** requires a plan to be written, not a cooling center to be built. The description
  says exactly that.
- **AB 527** is permissive. The description says a district may choose to install cameras and
  that nothing in the bill makes it.
- **SB 442** requires numbers to be published. The description says it does not itself change
  when a utility may shut off service.
- **SB 183** counts only children in the agency's care, so children who are only under
  investigation are not counted. The description says so.

## Checks run before importing

| check | result |
| --- | --- |
| Repository plain-language lint, 45-word sentence cap | 28 descriptions, **0 warnings** |
| `nv_check.py` — comma splices, British spellings, sentence length, reading level | **0 problems** |
| Flesch-Kincaid grade | median **7.6**, worst **8.9** |
| Longest sentence | 29 words |
| Banned areas (`general`, `impartiality`, `legal_competence`) | 0 used |
| Every stated tally against the stored vote row | **14 of 14 match** on chamber, measure, date and tally |

## Reconciliation

Predicted independently from the crosswalk and the roll evidence before touching the database:
**265 records and 189 area tags**.

| source | records | tags |
| --- | --- | --- |
| independent prediction | 265 | 189 |
| importer dry run | 265 insert | — |
| importer real run | 265 insert, 0 errors, 0 notified | — |
| database, this run's stamp | 265 | 189 |

The dry run's stamp `2026-09-05T03:42:54.183Z` matched zero rows. The real run's stamp is
`2026-09-05T03:42:57.034Z`. The convergence re-run reported all 265 unchanged. Nevada now holds
652 records across 32 rolls.

## Related records

Four candidate rows carried a related-record link, and all four point at hand-researched
records about **different bills** that share a vote date — AB 480 and AB 302 on 2025-04-22, and
SB 121 on 2025-05-22. The same pattern as batch-01. None was retired.

## What is left in Nevada

**27 measures carrying 35 rolls** are marked `candidate:batch-03` in the survey worklist. They
survived triage on title and digest and now need their enacted acts read.

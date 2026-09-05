# Colorado batch-01 — plan

**13 rolls / 7 measures / 343 records / 52 candidates.** Imported on the local
`voteapp` database on 2026-09-04. Production holds no Colorado records.

## How these seven measures were chosen

The five selection filters, in order:

1. **Divided.** The losing side is at least a quarter of the winning side. 611
   rolls on enacted bills clear this.
2. **Became law.** Every measure here was signed by the governor.
3. **One roll per measure per chamber**, and it has to be the chamber's last
   floor vote. In Colorado that is the repassage after concurring in the other
   chamber's changes, which is the vote on the text that became law.
4. **A nameable subject** that maps to a research area.
5. **A defensible for-or-against direction.** Anything that would land on
   `general` was dropped rather than imported.

Within that gate the batch was picked for closeness and public salience: these
are among the closest divided votes of the session, and each fills a research
area with a direction nobody disputes.

## The batch

| measure | subject | area | House | Senate |
|---|---|---|---|---|
| SB 25-003 | semiautomatic firearms and rapid-fire devices | gun_control / for | 36-28 | 19-15 |
| HB 25-1133 | requirements for retail ammunition sales | gun_control / for | 38-25 | 19-16 |
| SB 25-001 | Colorado Voting Rights Act | civil_rights / for | 43-22 | 20-12 |
| HB 25-1312 | Kelly Loving Act, protections for transgender people | civil_rights / for | 40-24 | 20-14 |
| HB 25-1249 | tenant security deposit protections | housing_affordability / for | 34-31 | 19-16 |
| HB 25-1090 | deceptive pricing and landlord fees | corporate_accountability / for | 41-21 | 22-12 |
| SB 25-004 | child care program fees | cost_of_living_reduction / for | 38-22 | — |

SB 25-004 carries only its House roll; the reason is in `JUDGING.md`.

## Dropped after a full read of the enacted text

- **HB 25-1236, residential tenant screening.** The act does two small things:
  a screening report need not carry credit history for an applicant using a
  housing subsidy, and it removes a landlord's right to demand the report come
  straight from the reporting agency. Neither is a supply or cost change, so
  `housing_affordability` does not fit, and no other area does either. This is
  the Maryland HB 767 shape.
- **SB 25-030, transportation mode choice.** The title promises emission
  reductions through mode choice targets, but the enacted act requires
  inventories, a report and a set of definitions; adopting mode choice targets
  is something a local government **may** do. Describing it as an
  emissions measure would credit members with a duty the act does not impose.

## What is left

401 rolls on 253 measures cleared filter 4 across the session, so this batch
takes 13 of them. The worklist at `../survey/divided-enacted-worklist.tsv`
carries the rest.

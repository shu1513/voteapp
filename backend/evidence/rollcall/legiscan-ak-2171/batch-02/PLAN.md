# Alaska batch-02 — divided votes on bills the governor vetoed

## Scope

Batch-01 covered measures that became law. This batch covers the other half of Alaska's
divided government: bills that passed both chambers on a split vote and were then vetoed.
None of them is law. Every description says so, and every description is written in the
conditional ("would have"), so a reader is never told the state changed when it did not.

## The pool

After the fetch collapsed identity duplicates, **65 divided floor rolls on 35 measures**
remained outside the enacted set:

| bill status | measures | rolls | where they went |
| --- | --- | --- | --- |
| vetoed | 20 | 40 | this batch |
| passed one chamber only | 12 | 17 | batch-03 |
| procedural concurrent resolutions | 3 | 3 | excluded |

Two of the twenty vetoed measures, HB 10 and HB 93, were still marked "enrolled" in our
2026-08-30 dataset. The state's own bill pages, read on 2026-09-04, show HB 10 vetoed on
2026-08-31 and HB 93 vetoed on 2026-08-10. See `../CODE-FINDINGS.md` §6.

**No veto in this pool was overridden.** Seven measures show an explicit
`GOVERNOR VETO SUSTAINED` line in their history; the other thirteen drew no override motion.
Alaska overrides in joint session, and batch-01 excluded that whole family of rolls from the
config, so no override roll appears here either.

## Selection

The five filters from the campaign playbook, in order:

1. **Divided.** The losing side is at least a quarter of the winning side.
2. **Reached the governor.** Every measure here passed both chambers.
3. **A nameable subject.** One thing a reader can hold in mind.
4. **One roll per measure per chamber**, on the text that was actually enrolled.
5. **A defensible for-or-against direction** in a research area.

Filter 5 removed the most measures, and the reasoning is in `JUDGING.md`.

## Result

**12 measures, 18 rolls.** Twelve House rolls and six Senate rolls.

| measure | chamber(s) | research area |
| --- | --- | --- |
| HB 10 university faculty regent | House | public_education_quality |
| HB 25 polystyrene foam food containers | House, Senate | environment_and_public_health |
| HB 26 statewide transit plan | House, Senate | public_infrastructure |
| HB 52 children in psychiatric hospitals | House | social_programs_and_welfare |
| HB 69 base student allocation | House, Senate | public_education_quality |
| HB 133 deadlines for paying state bills | House, Senate | government_efficiency |
| HB 280 market-based corporate tax sourcing | House | corporate_accountability |
| SB 21 Alaska Work and Save | House | social_programs_and_welfare |
| SB 24 tobacco and vaping age 21, vape tax | House, Senate | environment_and_public_health |
| SB 41 mental health in school health lessons | House | public_education_quality |
| SB 113 digital business tax apportionment | House, Senate | corporate_accountability |
| SB 258 state software license contracts | House | government_efficiency |

## Fan-out

Our Alaska roster still holds six of the sixty legislators: five representatives
(HD-031, HD-032, HD-035, HD-036, HD-039) and one senator (SD-I). A House roll therefore
reaches four or five people and a Senate roll reaches one. That is why 18 rolls produce
65 records rather than several hundred. A roster campaign is the binding constraint on this
whole jurisdiction, and re-importing after it lands adds the missing members without
duplicating anything.

## Ledgers in this directory

- `judgments.json` — the approved judgments, one per roll.
- `judge-report.json` — the run that set each roll to approved.
- `import-dry-run-report.json` — the dry run. Its stamp matches zero rows in the database.
- `import-report.json` — the first real run: 18 rolls, 65 records inserted, 0 errors.
- `import-rerun-report.json` — the convergence run: all 65 unchanged.
- `ls-ak-*.json` — the stored roll evidence for each imported roll.

# Alabama batch-01 — selection plan

This batch covers Alabama's 2025 Regular Session (LegiScan session 2148). The full pool and the
rules that produced it are in `../README.md` and `../survey/divided-worklist.tsv`.

## How the batch was selected

The session has 917 kept floor votes. Five filters narrowed them, in this order:

1. **Divided.** The losing side is at least a quarter the size of the winning side. 34 rolls
   survive; 773 of the 917 kept votes were unanimous.
2. **Consequential.** The measure became law. 17 rolls on 13 measures survive.
3. **A nameable subject.** SB 314 is a Shelby County local act (a county car-tag fee voted mostly
   by that county's delegation), so it has no statewide subject and drops out. 12 measures left.
4. **One roll per measure per chamber, preferring the chamber's vote on the enacted text.** Where a
   chamber had two divided rolls (HB 202: passage in March, concurrence in May), the batch takes the
   later one, which is the vote on the text that became law.
5. **A defensible for-or-against direction, or a deliberate no-stance import.** Four measures were
   dropped here (listed below). Two measures with no honest direction were still imported under
   `general` with no stance, because they were among the most divided votes of the session and the
   Ohio H.B. 116 precedent says a divided vote worth recording should not vanish just because no
   direction is honest.

## What is in the batch

10 rolls on 8 measures, all enacted:

| Measure | Rolls | Label |
|---|---|---|
| SB 116 machine gun conversion devices | House 1507290 (77-23) | gun_control, yes = for |
| SB 53 human smuggling and jail checks | Senate 1488934 (24-8) | immigration, yes = against |
| SB 63 fingerprints and DNA in custody | Senate 1488709 (24-7) | immigration, yes = against |
| HB 165 Juneteenth state holiday | Senate 1567089 (13-5) | civil_rights, yes = for |
| HB 202 police legal shield | House 1573029 (73-28, concurrence) | civil_rights, yes = against |
| HB 8 vaping restrictions | House 1566797 (52-43, concurrence) | environment_and_public_health, yes = for |
| HB 357 heated tobacco tax | House 1543491 (58-37) and Senate 1567082 (24-6) | general, no stance |
| HB 445 hemp THC products | Senate 1566046 (19-13) and House 1566772 (60-27, concurrence) | general, no stance |

Estimated and actual yield: 534 records across 118 candidates — every candidate the crosswalk maps.

## Dropped under filter 5, after reading each enrolled Act in full

- **HB 43 (split sentences).** It lets judges split a sentence of up to 30 years, so a person
  serves part in prison and part on probation — a leniency expansion. But for the new 20-to-30-year
  band it also orders at least 10 years served with no parole and no early-release credit. The two
  halves pull in opposite directions inside public_safety_and_crime_control.
- **HB 288 (athletic trainer loan aid).** Read in full to rule out a vehicle bill; it really is a
  $7,500-a-year student-loan assistance program for licensed athletic trainers who work at K-12
  schools. No research area carries an honest direction for a small one-profession subsidy.
- **SB 67 (veterans affairs board).** Moves the power to pick the veterans commissioner from the
  veterans service organizations' board to the Governor and makes the board advisory. A
  power-shift fight with no honest direction; creating or rearranging an office is not
  "government efficiency" (the Pennsylvania SB 187 rule).
- **SB 330 (regional water boards).** The Birmingham Water Works fight. It ends the sitting
  board's terms at once and hands most appointments to state officials and suburban counties.
  Supporters call it ratepayer representation; opponents call it a state takeover of a majority-Black
  city's utility. No honest single direction.

## Deferred, not judged

- The 17 divided rolls on measures that passed one chamber and died (the Pennsylvania batch-02
  scope) — includes HB 7 (immigration arrests), HB 247 (Gulf renaming), HB 479 (voter database),
  HB 30 (post-election audits).
- The 2026 Regular Session (LegiScan 2218, already downloaded and measured: 18 divided-and-enacted
  rolls on 14 measures, same vocabulary). It needs only an `AL-2218` registry key.

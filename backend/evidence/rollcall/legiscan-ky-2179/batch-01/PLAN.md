# Kentucky 2025 Regular Session — batch-01

23 roll calls on 12 measures. 11 House rolls and 12 Senate rolls, fanning out to
1,151 records across 107 candidates.

## How these 23 rolls were chosen

Five filters, in order.

1. **Divided.** The nay votes are at least 15 percent of the votes cast. Kentucky's
   minority caucus is about 20 percent of the House and 16 percent of the Senate, so
   the campaign's usual gate — the losing side must be at least a quarter of the
   winning side — cuts straight through the middle of Kentucky's party-line votes
   and would drop 39 of the 55 veto-override rolls. The reasoning is in the
   directory README. The gate still drops token dissents such as HB 240 at 93-3.
2. **Consequential.** Every measure became law. Ten of the twelve became law over
   the Governor's veto, one was signed, and one became law without a signature.
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber**, taking the chamber's last divided roll.
5. **A defensible direction**, or no stance at all where the measure's own text
   pulls both ways. Thirteen rolls on four measures were dropped under this filter;
   the reasons are in `../survey/divided-enacted-worklist.tsv`.

## Why the version check was nearly free here

Kentucky's veto override needs only a simple majority of each chamber, and the
legislature used it 28 times in this session. An override vote is taken on the
enrolled Act, and no amendment is possible at that stage. Twenty of the 23 rolls
are the 27 March 2025 override votes, so the text each chamber voted on **is** the
text that became law. Every roll was still checked one by one against Kentucky's
own vote record.

The three rolls that are not overrides are HB 520 in both chambers and SB 100 in
the Senate, neither of which was vetoed, plus the HB 4 Senate roll described below.
For HB 520 both chambers voted the identical text: a Senate committee substitute
existed but was withdrawn on the floor before the vote, and the enrolled document
is stamped 4 March, fifty-one minutes after the House vote and ten days before the
Senate's. SB 100's Senate roll is its concurrence in the House committee substitute,
and the enrolled document is stamped twenty-four minutes later.

## One roll needs explaining: HB 4 in the Senate

**LegiScan's feed does not contain the Senate's veto-override vote on HB 4.** Kentucky's
own record shows it as RSN# 3664 on 27 March, 32-6. The feed carries only the Senate's
passage vote of 12 March, RSN# 3503, which was also 32-6. That roll is what this batch
imports, and its descriptions say "voted to pass" rather than describing an override.
Everything adopted after 12 March was a title amendment, so the operative text is the
same. HB 2 and HB 6 have the same gap and are not in this batch.

## The twelve measures

| Measure | Rolls | Area | Direction |
| --- | --- | --- | --- |
| HB 4 — diversity, equity and inclusion at public universities | 2 | civil_rights | against |
| HB 90 — birth centers and abortion procedures | 2 | general | none |
| HB 136 — corrections reporting and a prisoner phone contract | 2 | public_safety_and_crime_control | for |
| HB 398 — occupational safety and health | 2 | corporate_accountability | against |
| HB 399 — interference with a legislative proceeding | 2 | civil_rights | against |
| HB 520 — law enforcement records | 2 | anti_corruption | against |
| HB 694 — Teachers' Retirement System funding | 2 | government_spending_reduction | for |
| HB 695 — Medicaid | 2 | general | none |
| SB 84 — judicial review of state agency action | 2 | government_efficiency | for |
| SB 89 — waters of the Commonwealth | 2 | environment_and_public_health | against |
| SB 100 — tobacco, nicotine and vapor product licensing | 1 | environment_and_public_health | for |
| SB 183 — proxy voting by public pension advisers | 2 | corporate_accountability | against |

Every stance label states `nay: null`. A no vote on one of these bills is not
evidence that the member opposes the whole research area, and the realistic
objection usually runs on a different axis from the area scored.

## What is left

`../survey/divided-enacted-worklist.tsv` dispositions all 118 divided-and-enacted
rolls on 45 measures: 23 imported here, 20 not selected because filter 4 takes only
one roll per chamber, 13 dropped under filter 5 with written reasons, and 62 left as
batch-02 candidates. Production has no Kentucky roll-call records.

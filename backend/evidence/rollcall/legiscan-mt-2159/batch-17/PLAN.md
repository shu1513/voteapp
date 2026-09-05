# Montana batch-17 — the widest batch, read in parallel

Twenty-four measures, thirty-five roll calls, 1,210 candidate records. All
twenty-four became law. This is the largest batch of the campaign.

**HB 446's House roll passed 50-49**, the closest vote this campaign has
imported from Montana, ahead of SB 214's 50-48 in batch-14.

| Measure | Chapter | Area | Yes vote means |
| --- | --- | --- | --- |
| HB 162 resort tax may pay for worker housing | 425 | housing_affordability | **for** |
| HB 165 no notary needed to sign over a vehicle | 88 | government_efficiency | **for** |
| HB 184 wider legal meaning of a deaf person | 237 | civil_rights | **for** |
| HB 211 licensed third-party beer and wine delivery | 75 | corporate_accountability | **for** |
| HB 215 no duty to seek balance on state boards | 114 | civil_rights | against |
| HB 358 volunteer firefighter pension to $200 | 448 | social_programs_and_welfare | **for** |
| HB 365 legislative conduct barred from fee awards | 450 | anti_corruption | against |
| HB 411 farm land exempt from open space levies | 653 | government_spending_reduction | against |
| HB 442 insurers stop reporting doctors to the board | 286 | corporate_accountability | against |
| HB 443 statewide licensing of doorstep sellers repealed | 287 | corporate_accountability | against |
| HB 446 new indecent exposure offence by birth sex | 657 | civil_rights | against |
| HB 521 no suing a victim over injuries while offending | 315 | public_safety_and_crime_control | **for** |
| HB 588 motorised scooters brought into traffic law | 693 | public_safety_and_crime_control | **for** |
| HB 621 confidential peer support for responders | 699 | environment_and_public_health | **for** |
| HB 667 protection for employees who run for office | 705 | corporate_accountability | **for** |
| HB 710 judge-issued subpoenas for child exploitation cases | 341 | public_safety_and_crime_control | **for** |
| HB 791 products placed beyond public nuisance law | 531 | corporate_accountability | against |
| HB 819 closed list of flags on government property | 731 | civil_rights | against |
| HB 888 health boards may not require vaccination | 746 | environment_and_public_health | against |
| HB 891 health department subpoenas without a judge | 747 | anti_corruption | **for** |
| SB 30 conflicted judges may not lean on necessity | 351 | anti_corruption | **for** |
| SB 38 no fees after a failed veto override poll | 352 | anti_corruption | against |
| SB 97 challenges heard in the sponsor's home district | 357 | anti_corruption | against |
| SB 147 Indian Child Welfare Act stops expiring | 588 | civil_rights | **for** |

## How this batch was read

All 116 remaining bills were read in parallel by six agents working from a
shared brief, at the user's request. **The agents read; this session judged.**
Every area, every direction, every description and every import decision was
made here, because that is where the campaign's accumulated rules live — the
sunset-title trap, the caution that a harsher penalty is not automatically "for"
`public_safety_and_crime_control`, and the rule that a permission must be
described with its conditions.

Every chapter number above was re-fetched from `api.legmt.gov` in this session
rather than taken from a report.

## The vote check earned its keep twice

All thirty-five imported rolls agree with Montana's own record member for
member. Two bills did not, and neither reached an import:

- **HB 888's House roll 1558107** puts Brian Close on the wrong side. It was
  already marked `held:legiscan-vote-defect`. HB 888 therefore carries its
  Senate roll alone.
- **HB 667's roll 1550002** puts Sidney Fitzpatrick on the wrong side. That roll
  went 99-1, so it was never a candidate — the selected Senate roll is clean.

## Two bills that resolve open questions

**SB 147** was deferred in batch-13 because its only content is "Section 55,
Chapter 716, Laws of 2023, is repealed" and nothing in it says what that section
did. It is now settled: SB 248 and SB 249 print that very section in their own
statutory notes as "(Terminates June 30, 2025--sec. 55, Ch. 716, L. 2023.)". So
SB 147 removes the sunset on the Montana Indian Child Welfare Act.

**HB 807** was held as a question for the user on the fluoride precedent. It
needs no decision. The act changes two words, "any vaccine" to "a vaccine",
twice. The ban on requiring an emergency-use vaccine has been law since 2021. It
is dropped as housekeeping.

## Drops

Four housekeeping and thirteen filter 5, all in
`../survey/filter-5-drops.md`. The filter 5 group is mostly the pattern this
campaign settled in batch-11: an act that cuts both ways at once. HB 642 and
HB 940 criminalise camping, HB 312 and SB 428 move speed limits, SB 348 raises
sentences, HB 350 and HB 409 restrict courts.

**109 rolls across 81 bills remain.**

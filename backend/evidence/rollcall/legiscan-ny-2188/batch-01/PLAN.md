# New York batch-01 — plan

11 measures, 22 roll calls, **570 records across 53 candidates**, imported into the local
`voteapp` database on 2026-09-02. Production has none.

## How these 11 were chosen

The pool is the 343 divided floor votes on measures that became law, across 236 measures.
The five filters, in order:

1. **Divided** — the losing side is at least a quarter of the winning side.
2. **Consequential** — the measure became law. All 11 are signed chapters of 2025.
3. **A nameable subject** that maps to one of our research areas.
4. **One roll per measure per chamber**, and it has to be the decisive one. New York votes
   each measure once per chamber, so this bound only on Senate Bill 1985 (below).
5. **A defensible direction.** A measure whose text pushes two ways was dropped, even when
   it was newsworthy.

Every one of the 236 measures carries a disposition in
`../survey/divided-enacted-worklist.tsv`: 11 in this batch, 118 excluded as local (a named
town, village, county or city), 24 excluded as budget or bonding measures, 11 excluded as
plain extensions of an expiry date, 6 dropped under filter 5, 3 dropped as chapter
amendments of a measure already in this batch, 2 held for a direction call from the user,
5 held for batch-02 because their acts are long enough to need their own read, and 56
still to read.

## The batch

| measure | chapter | Senate | Assembly | area | direction |
| --- | --- | --- | --- | --- | --- |
| S 1985-A, police custody of guns after a family violence call | 466 | 43-19 | 93-46 | gun_control | for |
| S 743, gun warnings cover rifles and shotguns | 114 | 43-20 | 98-50 | gun_control | for |
| S 744, pistol converters are rapid-fire devices | 115 | 50-13 | 100-48 | gun_control | for |
| A 4040-A, disparate impact in housing discrimination | 649 | 39-20 | 95-46 | civil_rights | for |
| S 36-A, abortion medication labels may name a practice | 7 | 39-20 | 95-42 | womens_reproductive_rights | for |
| S 3072, credit history barred from hiring decisions | 681 | 40-20 | 98-46 | civil_rights | for |
| S 8416, FAIR Business Practices Act | 708 | 37-22 | 91-50 | corporate_accountability | for |
| S 7882, algorithmic rent setting banned | 437 | 40-22 | 95-51 | housing_affordability | for |
| S 952-B, security deposits in rent stabilized apartments | 436 | 38-23 | 95-50 | housing_affordability | for |
| S 801, electric vehicle charging in the building code | 111 | 44-17 | 88-57 | environment_and_public_health | for |
| S 8417, end of the gas hookup cost break | 709 | 34-25 | 83-62 | environment_and_public_health | for |

Five areas, none of which had any New York coverage before. All eleven score `for`, which
is what a one-party trifecta produces: the set of measures that divide the chamber and
still become law is the majority's own agenda. The same shape appeared in Illinois,
Connecticut, Maryland and Maine.

## Version check

Every roll was checked against New York's own action history on the Assembly bill page,
which prints each amendment and each passage with its date. Ten of the eleven measures were
voted in the same print by both houses, and that print is the chapter.

**Senate Bill 1985 is the exception and the reason filter 4 exists here.** The Senate passed
print 1985 on 2025-05-13 by 37-20, then reconsidered, amended the bill to 1985-A on 05-27,
and passed it again on 06-09 by 43-19. The Assembly voted 1985-A on 06-11. Only the two
votes on 1985-A are judged; the 05-13 vote is on text the Senate itself replaced and is
recorded as superseded in the worklist.

All 22 roll dates match the dates New York's own history prints. There is no date skew here
and no `official_vote_date` override was needed.

## Dropped under filter 5, after reading the act

- **A 136, the Medical Aid in Dying Act**, and **A 9515**, its provider rules. No area
  carries an honest direction on assisted death: organized disability-rights groups and
  patient-autonomy groups both claim the civil rights reading, and calling it care whose
  access should improve assumes the contested question. This is the Illinois SB 1950 call.
  It is the most newsworthy divided-and-enacted measure of the session, so expect to be
  asked about it.
- **A 1241**, which repeals the crime of furnishing items of nominal value to induce
  attendance at the polls. It reads as protecting people who hand out food and water in
  line, and as removing an anti-inducement crime. Those point opposite ways.
- **S 818, S 9155, S 10113**, all cannabis market rules. Ohio H.B. 116 precedent.

## Held, not dropped

- **A 10710 and A 10711**, childhood vaccine schedules — escalated to the user for a
  direction call, following the standing rule on vaccine-adjacent measures.
- **S 824** (Climate Change Superfund), **S 4914** (shield law for reproductive and
  gender-affirming care), **A 9516** (video after a death in custody), **A 387** (hospital
  language services) and **S 752** (ballot drop boxes) are held for batch-02. Each needs a
  longer read than this batch had room for; none was dropped on its merits.

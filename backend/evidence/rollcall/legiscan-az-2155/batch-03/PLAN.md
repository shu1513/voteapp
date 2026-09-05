# Arizona batch-03 — the vetoed scope: rights, elections, immigration, firearms

32 measures, 58 rolls, 1,463 records across 54 candidates. This is the first Arizona batch of
measures that **did not become law**: the legislature passed them and the Governor vetoed them.

## Why this scope exists

Arizona has a Republican legislature and a Democratic governor who vetoed 174 measures with a
divided floor vote in one session. Under the divided-and-enacted gate used for batches 01 and
02, none of that is visible — what passes into law in a divided state is the bipartisan
remainder. Pennsylvania's batch-02 opened this scope and established the wording rules; this is
Arizona's version of it, and it is where the two parties actually differ.

## How the pool narrowed

| step | rolls | measures |
| --- | --- | --- |
| divided third reading on a bill the Governor vetoed | 374 | 174 |
| after filter 4, one roll per measure per chamber | 328 | 174 |
| after the version rule and the passed-flag check | 287 | 173 |
| judged across batches 03 and 04 | 108 | 64 |
| **in this batch** | **58** | **32** |

The version rule is the same one batch-01 established: Arizona publishes no member list for a
concurrence vote, so where the second chamber amended a bill, the originating chamber's third
reading is a vote on a superseded draft. That removed 41 rows here.

## Wording, and why it differs from batches 01 and 02

Every body is **conditional** — "would have required", never "requires". The builder asserts it:
a body without the words "would have" fails, and so does one containing "became law", "takes
effect" or similar.

The tail states a completed fact — "The Arizona House passed it 32-27, and the Governor vetoed
it." Pennsylvania had to use a time-stamped hedge because its session was still sitting.
Arizona's 2025 regular session has adjourned and no veto was overridden, so nothing here can
change.

## What is in

| area | direction | measures |
| --- | --- | --- |
| civil_rights | against | HB 2017, HB 2062, HB 2438, HB 2868, SB 1002, SB 1003, SB 1052, SB 1256, SB 1584, SB 1586, SB 1694 |
| civil_rights | for | SB 1097 |
| election_integrity | for | HB 2004, HB 2006, HB 2007, HB 2046, HB 2154, SB 1001, SB 1064, SB 1098, SB 1123, SB 1280 |
| election_integrity | against | HB 2440 |
| immigration | against | HB 2099, SB 1088, SB 1164, SB 1268, SB 1610 |
| gun_control | against | SB 1014, SB 1020, SB 1143, SB 1705 |

`gun_control` and `immigration` are new to Arizona; batches 01 and 02 had neither, because
neither subject produced a divided vote on a bill that became law.

**Both directions appear in two areas on purpose.** In elections, ten measures tighten ballot
handling, machine security or list accuracy and score `election_integrity`/for, while HB 2440
would have shielded a county supervisor from prosecution for refusing to certify a result and
scores against. In civil rights, eleven measures restrict and one — SB 1097, which would have
opened more public buildings as polling places — expands.

**The line between the two election areas.** A measure about the accuracy, security or
auditability of the count is `election_integrity`. A measure that changes who can vote or how
easily is voter access, which the campaign files under `civil_rights` — so HB 2017 (closing
voting centers) and SB 1052 (removing the vote from citizens who have never lived in the United
States) sit there rather than in election_integrity.

Every nay is stated and every nay is `null`. On these measures the realistic objection usually
runs on a different axis from the area being scored — cost, local control, or federal
preemption — so a no vote is not evidence of a position on the area's own goal.

## Vote checks

**SB 1001 hit Arizona's reconsidered-vote trap, with a twist.** Its House third reading failed
29-26, the House reconsidered, and it passed 31-25 the same day. The failed roll is stored with
`passed = 1`, because 29-26 is a majority of the votes cast — but Arizona needs a majority of
the whole 60-seat chamber, which is 31. The passing roll is judged and the failed one is named
in `acknowledge_later_rolls` with a note. This is written up as finding 6 in `../CODE-FINDINGS.md`.

A single query over all 58 rolls found that pair and nothing else.

## What was dropped

The full reasons are on the worklist rows in `../survey/divided-vetoed-worklist.tsv`. Across
both vetoed batches, 105 measures were dropped and 8 rows deferred. The patterns in this
subject area:

- **Two directions inside one area** — HB 2867, the Antisemitism in Education Act, protects
  students from discrimination while restricting what may be taught, and both readings sit
  inside `civil_rights`. HB 2206 would have pulled Arizona out of a multistate voter list
  maintenance body: cleaner rolls against protecting voter data. HB 2703 mixes repealing
  emergency voting centers with allowing tabulation during early voting.
- **The China and foreign-adversary cluster** — SB 1082, SB 1109, SB 1221, HB 2542, SB 1027,
  SB 1066, HB 2693. All aim at security, `foreign_trade` is about trade rather than investment
  or land, and splitting the cluster would make members' records disagree about one question.
- **Expressive measures** — HB 2649 supporting the electoral college, HB 2700 requiring Gulf of
  America instruction.

**Four measures are deferred as direction calls rather than dropped**, all vaccine-adjacent:
HB 2012 (barring employers and government requiring an emergency use product), HB 2058
(university immunization exemptions), HB 2063 (exemption information in school notices) and
HB 2257 (foster placement and vaccination status). The campaign's standing instruction after
the Florida fluoride decision is to escalate a contested-evidence direction rather than assume
one. They are marked `deferred:direction-call` on the worklist.

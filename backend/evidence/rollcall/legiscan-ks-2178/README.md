# Kansas roll-call votes — LegiScan session 2178

Kansas Legislature, 2025-2026 Regular Session. Kansas files **both years of the
biennium in one LegiScan dataset**, so this single session covers the whole
current term. There is no separate 2025 or 2026 dataset to register.

- LegiScan session id: **2178** (`2025-2026 Regular Session`)
- Kansas LegiScan `state_id`: 16
- Dataset cut: **2026-06-28**, hash `d756f09a6267fcae9ee6c9eaaf8dac79`, 4.3 MB
- Contents: 1,483 bills, 1,435 roll calls, 218 people
- Dataset kept outside the repository at `/Users/shu/legiscan-data/ks-2178/`

## Who this reaches

**Only the Kansas House is on the November 2026 ballot.** All 125 House seats
run on two-year terms; Kansas Senate terms run to 2028. Our database holds
**129 candidates across 81 of the 125 House districts** and no Senate
candidates at all. Senate roll calls therefore fan out to nobody, and the House
roll calls carry the whole campaign. That is a fact about the Kansas calendar,
not a gap in our rosters, so no Senate roster work would change it.

## Divided government is the story of this feed

Kansas pairs a Republican supermajority legislature with a Democratic governor.
A veto override needs two thirds of each chamber, and the legislature used it
**69 times** in this biennium. An override vote is divided by definition (it
takes a two-thirds margin against an opposing minority), so overrides are a
first-class part of the pool rather than an oddity. The config keeps them as the
`veto_override` question class.

**A prevailing override in one chamber is not enactment.** SB 79's Senate
override prevailed 29-11 on 2025-04-10; the House never took it up (`No motion
to reconsider vetoed bill; Veto sustained`) and the veto stood. Before a
description says a measure became law over the veto, check the bill's own
history for both chambers' overrides and LegiScan `status` 4 (`Passed`). A
one-chamber override is described as that chamber's vote to override, nothing
more.

## What the survey established

- **Every description ends with its own tally**, spelled ` - Yea: <n> Nay: <n>`.
  That suffix makes almost every description unique: 729 distinct descriptions
  fold to **130 families** once it is removed. The raw histogram is useless
  until you fold it, and no config pattern may be anchored at the end.
- **The embedded tally is a free per-roll checksum**, which no other state in
  this campaign offers. See CODE-FINDINGS.md finding 2 for the one roll where it
  disagrees with the structured fields.
- Kansas takes **two recorded floor votes** on a bill. `Committee of the Whole`
  is the amend-and-debate stage — the second-reading analog Texas, California
  and Missouri also exclude — and `Final Action` is passage. `Emergency Final
  Action` is Final Action taken the same day a bill is reported, not a different
  question, so it is kept under the same rule.
- **No committee votes exist in the dataset.** Every roll's `total` is the whole
  chamber (125 House, 40 Senate), so nothing lands in the small-tally bucket.
- Feed health is the cleanest tier: 0 repeated `roll_call_id`s, 0 summary-only
  rolls, 0 internal tally mismatches, 1 identity-duplicate extra.
- **Eleven rolls disagree with the tally Kansas prints in its own bill history**,
  one of them (SB 63, roll 1491886) with a member on the wrong side. They are
  listed in the config's `heldRollCallIds`, so the fetcher stores them with
  `is_floor_vote` null and the judge refuses to approve them. See
  CODE-FINDINGS.md finding 5.
- **Failed final questions are kept**, not excluded, the way Montana keeps
  `3rd Reading Failed`: a chamber's later rejection (HB 2527's conference report,
  46-75, after its 109-13 passage) must be visible to the judge's superseded-stage
  gate. Nothing fans out from them; no failed vote is ever approved.

## Classification result

Every description in the session classifies; the only surfaced rolls are the
11 held ones:

| bucket | rolls |
| --- | --- |
| kept floor votes | 1,270 |
| — passage | 836 |
| — conference report | 297 |
| — concurrence | 69 |
| — veto override | 68 |
| excluded questions | 100 |
| held (tally disagrees with Kansas's record) | 11 |
| surfaced (unmatched) | **0** |

The 100 excluded are Committee of the Whole amendments and rulings, line-item
veto overrides, and procedural motions (withdraw from committee, strike the
enacting clause, previous question, germaneness).

## The pool

Under the campaign's standard divided gate — the losing side is at least a
quarter of the winning side — and the enacted gate:

| measure | count |
| --- | --- |
| divided kept floor votes | 387 (House 215 / Senate 172) |
| **divided and enacted** | **280 rolls on 105 measures** (House 158 / Senate 122) |
| — of those, veto-override rolls | 67 |
| divided on measures that did not become law | 65 |
| divided on measures vetoed and not overridden | 42 |

The standard gate fits Kansas without adjustment. A party-line House vote runs
about 88-37 and a party-line Senate vote about 31-9; both clear the
quarter-of-the-winner threshold comfortably, so unlike Kentucky no recalibration
was needed. That was measured before it was assumed.

Because only the House is on the ballot, the **158 divided-and-enacted House
rolls** are the ones that write records.

## Crosswalk

171 entries: **75 mapped, 96 explicit null**. Validation over all 1,380 stored rolls:
matched 51,843, unmatched but reviewed 61,345, **no_crosswalk 0, out_of_scope 0**, no
file errors.

The proposer offered 72 and all 72 were accepted (70 exact first-and-last matches, 2
first-name prefixes, every one with the seat agreeing). Three were added by hand, all
of them the class the proposer structurally cannot reach — it reads `first_name` plus
`last_name` and never LegiScan's own `name` or `nickname` fields:

- **Stephanie Clayton, HD-019** — LegiScan's `name` is byte-identical to our candidate
  but its `last_name` is `Sawyer-Clayton`.
- **Bill Sutton, HD-043** — `first_name` is the legal `William` with `Bill` in
  `nickname`; again `name` is byte-identical to ours.
- **Abi Boatman, HD-086** — our row is `Elle Abigail Boatman`, so neither first name
  is a prefix of the other. Same seat, same party.

**48 of the 57 unmatched House members are a roster gap, not a crosswalk miss**: their
seat has no November 2026 candidate row at all. Our pool covers 81 of 125 districts.

## Layout

- `survey/` — the fetch survey report the config was written from, plus
  `house-divided-enacted-worklist.tsv`, which gives all 158 House divided-and-enacted
  rolls a disposition
- `crosswalk.json` and `legiscan-people-ks-2178.json` — the committed identity layer
- `batch-01/` — the first batch: PLAN.md, JUDGING.md, judgments.json, 12 roll
  evidence files and the import ledgers
- `CODE-FINDINGS.md` — defects and gaps recorded, not fixed

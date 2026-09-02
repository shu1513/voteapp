# Kentucky roll-call import — 2025 Regular Session (LegiScan session 2179)

Kentucky is a LegiScan phase-4 state. This directory holds the evidence for its
2025 Regular Session, the 30-day short session that adjourned on 28 March 2025.
The 2026 Regular Session is a separate LegiScan session and has its own
directory, `legiscan-ky-2247/`.

## What the dataset holds

1,441 bills, 701 roll calls and 138 people — exactly the 100 House seats and 38
Senate seats. Every tally is a whole-chamber tally, and the dataset contains no
committee votes at all.

Feed health is in the cleanest tier: no repeated roll call ids, no duplicate
roll identities, no summary-only rolls, and no tally mismatches. Every one of
the 701 descriptions matches a kept or an excluded pattern, so nothing was left
for a human to sort out after the fact.

## How Kentucky words its roll calls

Every description ends in a sequence number, and **the two chambers spell it
differently**: the House prints ` RCS# <n>` and the Senate ` RSN# <n>`. Folding
that suffix collapses 701 raw descriptions into 36 families, listed in
`survey/desc-families.tsv`.

The House uses a single wording, `Third Reading`, for every substantive floor
vote — passage, concurrence in the Senate's changes, and veto override alike.
The Senate uses `Third Reading` too, and **writes the version check into the
description**: `Senate: Third Reading W/scs1 sfa1 scta1` names the committee
substitute and the floor amendments folded into the text being voted on. Among
the states worked so far, only Pennsylvania's printer's numbers do this.

A joint resolution is adopted rather than passed, so the House words it `Adopt`.
Simple and concurrent resolutions carry the same word, but the pipeline drops
those measure types before this configuration is consulted.

## The description is not the question

**LegiScan's `desc` is its own claim about what a roll decided, and in Kentucky
it is wrong.** The label `House: Veto Override` appears seven times in this
session and not one of them is a veto override. Checked one by one against
Kentucky's official vote record, they are:

| Bill | Roll | What LegiScan says | What Kentucky says |
| --- | --- | --- | --- |
| SB 2 | RCS# 308 | Veto Override | Previous Question |
| HB 495 | RCS# 304 | Veto Override | Previous Question |
| HB 695 | RCS# 306 | Veto Override | Previous Question |
| SB 120 | RCS# 283 | Veto Override | Reconsider |
| SB 65 | RCS# 333 | Veto Override | Reconsider |
| HB 398 | RCS# 85 | Veto Override | Strike enacting clause |
| HJR 15 | RCS# 36 | Veto Override | a floor amendment |

The real override votes are worded `Third Reading`, exactly like an ordinary
passage. So the configuration excludes `veto override` by rule, which has the
useful side effect of removing seven procedural votes that would otherwise have
been stored as passages. The `questionClass` recorded on a kept Kentucky roll is
report metadata only. It must never be shown to a reader or trusted in a batch.

## Ground truth: Kentucky's own vote record

`https://apps.legislature.ky.gov/record/25rs/<bill>/vote_history.pdf`

One PDF per bill. For every roll call it gives the sequence number, the question
in plain words (`Final Passage`, `Reconsider`, `Previous Question`, `Override
Veto Final Passage`, `Strike enacting clause`), the date and time, and the full
list of how each member voted. Question and version in one document, which is
better than Florida's equivalent. **Check every selected roll against it.**

The bill page at `https://apps.legislature.ky.gov/record/25rs/<bill>.html` is
server-rendered and carries the full action history, the amendments, and the
Governor's veto message where there is one. Both need a browser user agent.

## Two hazards worth carrying forward

**The bill history's `chamber` label is inverted on the override lines.** HB 4's
House override, 79-19, is filed under `S`; its Senate override, 32-6, under `H`.
Read a roll's chamber from the roll itself, or infer it from the size of the
tally. Never read it from the history.

**Not every roll's tally appears in the history.** 672 of 701 do. The 29 that do
not are all procedural votes — amendments, motions to table, motions to suspend
the rules — for which Kentucky prints no tally. This is not a data defect, and a
reconciliation script should expect it.

## The divided gate for Kentucky

Kentucky's minority caucus is about 20 percent of the House and 16 percent of
the Senate. The campaign's usual gate — the losing side must be at least a
quarter of the winning side — cuts straight through the middle of Kentucky's
party-line votes, and would drop 39 of the 55 veto-override rolls, including
HB 4 at 79-19 and HB 2 at 80-19.

**On the user's decision, this campaign instead keeps a roll when the nay votes
are at least 15 percent of the votes cast** — that is, when most of the minority
caucus voted together against the measure. It expresses the same idea the
standard gate encodes elsewhere, calibrated to Kentucky's chamber arithmetic. It
still drops token dissents such as HB 240 at 93-3 and HB 552 at 85-10.

Under that gate the 2025 session yields **118 divided-and-enacted rolls on 45
measures**, of which 48 are veto-override rolls.

## Veto overrides

Kentucky's override needs only a simple majority of each chamber, and the
legislature uses it: **28 bills were vetoed and overridden in this session**. The
feed carries both chambers' override rolls for 25 of them; HB 2, HB 4 and HB 6
are House-only. A description of a bill enacted this way must say so — that a
bill became law over the Governor's veto is part of what happened.

## Batches

This session is complete. Three batches were taken from it, for **2,437
records across 107 candidates** in total.

- `batch-01/` — 12 measures, 23 rolls, 1,151 records.
- `batch-02/` — 11 measures, 18 rolls, 916 records. It also re-read the two
  measures batch-01 dropped that were worth a second look: HB 424 is now
  imported with no stance, and HB 684 stays dropped.
- `batch-03/` — 7 measures, 9 rolls, 370 records. It worked the 13 rolls the
  first two batches left open, and closed the session.

Every one of the 118 pool rolls carries a final disposition in
`survey/divided-enacted-worklist.tsv`: 23 batch-01, 20 not selected in batch-01,
18 batch-02, 10 not selected in batch-02, 9 batch-03, 3 not selected in
batch-03, and 35 dropped under filter 5. No roll is left open.

## Layout

- `survey/` — the fetch survey report, `desc-families.tsv` (the folded
  description histogram the configuration was written from), and
  `divided-enacted-worklist.tsv`.
- `crosswalk.json` — the reviewed people_id to candidate map. It serves **both**
  Kentucky sessions; see the 2247 README for the one seat that changed hands.
- `CODE-FINDINGS.md` — defects recorded but deliberately not fixed.

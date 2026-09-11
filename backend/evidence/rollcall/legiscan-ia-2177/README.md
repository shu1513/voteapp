# Iowa — LegiScan session 2177 (91st General Assembly, 2025 and 2026)

Roll-call votes imported as candidate records for the November 3, 2026 ballot. Local
database only; production holds zero Iowa roll-call records.

## The dataset
4,224 bills, 3,156 roll calls, 193 people, every bill on session 2177. Iowa has no special
sessions in LegiScan's list, so this one dataset is the whole state. Dataset ZIP and the full
fetch evidence (1,405 roll files) live outside the repo under `/Users/shu/legiscan-data/`.

## What the survey found (see `survey/`)
- The floor vocabulary is tiny: `Shall the bill pass?` on 1,184 of 1,192 floor rolls. The
  committee cut works by tally here (committee reports total 6 to 25 against chambers of 100
  and 50).
- Iowa splices the journal's page banner into a few descriptions. Every pattern in the config
  is anchored at the start only. One affected roll (HF 1003, 57-28) is a divided, enacted
  concurrence vote.
- Iowa prints the tally on its own history lines, so all 1,192 floor rolls were audited
  against the state's record with no network. 1,189 match. Three are held in the config: one
  member list a yea short (HF 2253), and two House votes filed under HF 639 and HF 593 that
  the journal shows belong to SF 639 and SF 593 (an HF/SF prefix confusion; the real bills
  carry no House roll in the feed).
- Six Senate rolls are stamped 2026-05-02 by LegiScan and 2026-05-03 by the journal. None is
  in batch-01. Any that reaches a batch needs the `official_vote_date` override.
- Concurrence in Iowa is a voice vote (108 concurrence actions, one with a tally), and no
  conference report drew a recorded vote. So when the second chamber amends a bill, the first
  chamber's only recorded vote is on the earlier text. That removed 50 of the 205 divided and
  enacted rolls (`survey/not-final-text-rolls.tsv`).
- The dataset re-issues ids for nine votes; the fetcher keeps the lowest id. Batches are
  selected from the stored rolls, never the raw dataset.

## Crosswalk (`crosswalk.json`)
155 members: 102 mapped, 53 null. Only the odd-numbered Senate districts are on the 2026
ballot (four-year staggered terms), so 25 of the nulls are structural. Five hand-adds: a
middle-name form, a legal name against a nickname, and three members leaving for federal or
county races that the ballot holds for them. Fan-out: about 78 candidates per House roll,
about 17 per Senate roll.

## Pool
205 divided and enacted rolls on 101 measures; 150 stored rolls on final text; **144 slots on
91 measures** after one roll per measure per chamber. The filter 4 correction below removed
HF 1003, whose final roll in each chamber was unanimous (Senate 43-0, House 85-0).
Dispositions are in `survey/divided-enacted-worklist.tsv`.

## Batches

| batch | measures | rolls | records | run stamp |
|---|---|---|---|---|
| 01 | 10 | 18 | 769 | 2026-09-09T23:47:36.357Z |
| 02 | 10 | 17 | 777 | 2026-09-10T04:35:37.176Z |
| 03 | 8 | 12 | 634 | 2026-09-10T04:40:03.722Z |
| 04 | 5 | 8 | 374 | 2026-09-10T04:45:48.520Z |
| 05 | 4 | 5 | 263 | 2026-09-10T04:47:41.463Z |
| 06 | 3 | 4 | 187 | 2026-09-10T06:34:25.892Z |

Review fixes re-ran the importer in place; rewritten rows carry the re-run stamp instead:
batch 02 `2026-09-10T05:44:42.259Z` (112 rows), batch 03 `2026-09-10T05:44:47.601Z` (176),
batch 04 `2026-09-10T05:44:51.277Z` (92), batch 05 `2026-09-10T04:48:47.712Z` (15),
batch 06 `2026-09-11T02:18:16.187Z` (114).

**Local total: 3,004 candidate records, 102 candidates, 64 approved roll calls over 40
measures. Production holds zero Iowa roll-call records.**

## Every slot is dispositioned

`survey/divided-enacted-worklist.tsv` accounts for all 144 measure-chamber slots on 91
measures, with nothing deferred:

| disposition | slots |
|---|---|
| imported in batches 01 to 06 | 64 |
| appropriations, set aside under filter 3 | 32 |
| dropped under filter 5 or filter 3, each with a written reason | 48 |

The divided-and-enacted pool is finished. Iowa's remaining roll-call work is the
not-enacted scope, which has not been opened, and production promotion.

## Two corrections worth carrying forward

**Filter 4 must take the chamber's FINAL roll, not its final DIVIDED roll.** The first
worklist builder kept the last divided roll in each chamber. On HF 1003 the House adopted the
Senate amendment 57-28 and then passed the bill 85-0 the same day, so the divided roll was not
the vote on the text that became law. The judge's supersession gate refused the batch, which is
what it is for. The rule was corrected and re-run over the whole session: exactly one slot
changed and every roll already imported stayed valid.

**The date skew is real and the audit must be a hard gate.** Six Senate rolls are stamped
2026-05-02 by LegiScan and recorded on 2026-05-03 in Iowa's journal, out of 28 Senate rolls
carrying that date. One of them, HF 2694, reached batch-05 and was imported before the audit
warning was acted on. It was re-judged with an `official_vote_date` override and re-imported
for real; the ledger is `batch-05/import-date-override-report.json`. The audit script now
stops the run instead of printing a warning.

## Not-enacted scope (batches 07-09, 2026-09-11)

Bills that did not become law. The 91st General Assembly adjourned for good on 2026-05-03, so
each description is conditional ("would have") and ends with a fate sentence derived from the
dataset: the other chamber never voted, the chambers never agreed on the same text, or the
Governor vetoed it. Pool and dispositions: `survey/not-enacted-worklist.json` (90 slots on 85
measures, each chamber's final kept roll, kept if divided). 25 measures imported on 26 rolls;
60 dropped with written reasons, including three whose substance became law through another bill
(HF 2336, HF 2716, SF 507). Tools outside the repo: `ia_notenacted.py` (pool), `ia_build_ne.py`
(conditional builder, refuses enacted bills), `ia_finish_ne.py` (audit gate).

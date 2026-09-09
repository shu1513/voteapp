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
205 divided and enacted rolls on 101 measures; 150 stored rolls on final text; **145 slots on
92 measures** after one roll per measure per chamber. Dispositions are in
`survey/divided-enacted-worklist.tsv`.

## Batches
| batch | measures | rolls | records | run stamp |
|---|---|---|---|---|
| 01 | 10 | 18 | 769 | 2026-09-09T23:47:36.357Z |

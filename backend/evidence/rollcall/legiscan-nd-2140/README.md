# North Dakota roll-call import — 69th Legislative Assembly (LegiScan session 2140)

North Dakota's legislature meets only in odd-numbered years, so the 2025 regular
session is the whole dataset for this campaign. The session sat from 2025-01-07
to 2025-05-02.

Local database only. Production holds no North Dakota roll-call records.

## Layout

- `crosswalk.json` — 142 entries mapping a LegiScan `people_id` to a VoteApp candidate.
- `crosswalk-proposals-report.json` — the proposer's output and the unmatched lists.
  The full resolve report carries per-roll member resolution for all 2,131 rolls and is
  9.5 MB, so it is deliberately not committed.
- `legiscan-people-nd-2140.json` — the people snapshot the crosswalk is keyed against.
- `CODE-FINDINGS.md` — findings recorded but not fixed.
- `survey/fetch-report.json` — the run that stored the rolls.
- `survey/divided-enacted-worklist.tsv` — one row per roll in the pool, every one
  carrying a disposition.
- `batch-01/` — the first batch: plan, judging notes, judgments, the 14 roll evidence
  files, and the import ledgers.

## What the session looks like

2,184 roll calls fold to 23 descriptions, the smallest vocabulary of any state in this
campaign. `House Second reading` and `Senate Second reading` alone account for 2,100
of them.

**North Dakota takes its recorded vote on passage at the second reading.** There is no
third reading. This was confirmed rather than assumed: the state prints the tally in its
own bill-history action lines, so every roll in the dataset was matched to a history line
on the same date with the same yea-nay count.

Every roll reports the whole chamber — 93 or 94 in the House, 47 in the Senate.

## Three things that make North Dakota different

**The committee cut by tally does not work here.** 44 rolls are worded
`Reported back, do pass, place on calendar 14 0 0`. The trailing three numbers are the
committee's own vote, but the roll itself carries a full-chamber member list and a
full-chamber tally, so the shared floor-versus-committee check would classify every one
of them as a floor vote. The config excludes them on the literal phrase instead.

**The emergency clause is not a separate question.** North Dakota carries it on the
passage vote itself, so 120 rolls read `Second reading, passed, yeas 63 nays 30,
Emergency clause carried`. Those are passage votes and are kept. Arkansas votes the
clause separately and excludes it; copying that would have discarded real passage votes.

**A failed vote is spelled exactly like a passing one.** 395 rolls are
`Second reading, failed to pass` wearing the same bare description as a passage vote.
Selection has to read the `passed` flag and the bill history, never the caption.

## No conference-report or concurrence roll calls exist

This is the finding that shapes the whole campaign here. The session records 191
`Conference committee report adopted` actions, 189 `Concurred`, 100 `Refused to concur`
and 9 `Conference committee report rejected` — and **not one of them prints a tally**,
because North Dakota takes no recorded vote on any of them.

So for a bill that went to conference, or that the second chamber amended, the only
recorded votes are second readings on pre-conference text. There is no conference-report
roll to prefer, and none to go looking for. Under the campaign rule that a divided roll
is imported only when it is that chamber's vote on the text that became law, this removes
a large slice of the pool.

## The pool

| step | rolls | measures |
|---|---|---|
| floor votes stored | 2,076 | |
| closely divided | 448 | |
| divided and enacted | 194 | 127 |
| on kept bill types (drops concurrent resolutions) | 185 | 122 |
| `passed` is true | 176 | |
| the chamber's vote on the text that became law | 99 | 90 |
| after one roll per measure per chamber, and one held roll | 98 | 89 |

Every one of those 98 carries a disposition in
`survey/divided-enacted-worklist.tsv`: 14 imported in batch-01, 7 excluded as agency
appropriation acts, 7 dropped as study-only, 3 dropped under filter 5, and 67 left as
candidates for a later batch.

## Roster coverage is structural, not a gap

North Dakota serves four-year staggered terms and elects its **odd-numbered** districts
in 2026: 25 Senate districts (the 24 odd-numbered ones plus district 10) and 27 House
districts (the 24 odd-numbered ones plus 20, 26 and 42). The crosswalk maps 48 of 142 serving members and leaves 94 null. **62 of
those nulls sit in a district that is not on the ballot at all.** That is expected. The
other 32 hold a seat that is up but are not seeking it.

Fan-out is a median of 27 candidates per House roll and 20 per Senate roll.

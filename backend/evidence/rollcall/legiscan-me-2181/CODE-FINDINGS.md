# Maine (LegiScan 2181) — findings recorded, not fixed

## 1. Ought-not-to-pass acceptances are unreachable by design

Maine's second-largest floor family is the vote to **accept an "Ought Not To
Pass" report** (House `Acc Maj Ought Not To Pass Rep` 163, Senate `Accept
Majority Ought Not To Pass Report` 165, plus minority and lettered-report
spellings — 352 rolls). A yea there KILLS the bill, so the fan-out's yea/nay
sentences would have to be inverted relative to every other kept question. The
config excludes them by rule.

Measured cost: **8 divided rolls on measures that became law anyway** (a motion
to kill that failed). That is the whole exposure — the rest sit on bills that
died, which the became-law gate drops regardless. A future change could keep
them, but only if the judgment file's yea sentence is written as a vote to kill
the bill; nothing in the importer inverts a sentence for you.

## 2. Failed veto overrides sit outside the gate

All 8 veto-question rolls in the session **failed**, so none of the 7 vetoed
bills became law (LD 1228 automotive right-to-repair, LD 1328 recovery housing,
LD 1731 ferry advisory board, LD 1911 automatic record sealing, LD 307 data
center council, LD 588 agricultural employees' concerted activity, LD 958
eminent domain on tribal trust lands). They are real, contested votes on
consequential bills, and several are marquee — but a record about them must say
the bill did NOT become law, which no current description template does. Left
for a deliberate decision rather than folded into a batch.

## 3. LegiScan's `amendments[].adopted` flag is unreliable for Maine

LD 1126 carries three amendments, all flagged `adopted: 1`; the history shows
Senate Amendment "A" (S-403) FAILED ADOPTION. Nothing in our pipeline reads the
flag, so this is a judging hazard, not a defect — but it is the reason the
recipe pins versions off `history[]` and never off `amendments[]`.

# Alabama 2026 First Special Session batch-01 — judging notes

## Sources

Both measures were judged from their **enrolled Act**, read top to bottom, at
`https://alison.legislature.state.al.us/files/pdf/SearchableInstruments/2026SS1/<BILL>-enr.pdf`
(browser User-Agent required, `pdftotext -layout`). The introduced versions were downloaded too but
carry no extractable text layer, so nothing was read from them. Nothing was judged from a title.

This batch was blocked for a day: the Alabama site stopped serving documents partway through the
earlier session's work and would not return these two files. It came back on 2026-09-02 and both
Acts were read then.

## What the two Acts do

They are the same Act written twice, once for districts in Congress and once for the State Senate.

Each one sets up a contingency. If a federal court, by ruling or by lifting an injunction, lets the
state go back to the map the Legislature itself enacted — Act 2023-563 for Congress, Act 2021-558
for the State Senate — and the ruling lands too late for the normal primary calendar, then the
Governor must call a new special primary in the affected districts, so long as it can be certified
by August 26 2026. In that special primary the candidate with the most votes wins the nomination
outright, with no runoff. Any certification of the earlier regular primary for those offices is void
for the purpose of naming a nominee, though candidates who qualified for the regular primary stay
eligible. Neither Act changes the date of the November 2026 general election.

The two differ in one place. HB 1 §1(e)(1) simply preserves a party's existing right to nominate by
some means other than a primary. SB 1 §1(e)(1) goes further and lets a party do so notwithstanding
any law to the contrary, provided it certifies its nominees by August 26 2026.

## Roll-attribution check

Each roll's printed roll call number was checked against its own bill's history, the test the 2026
regular session made necessary. All three pass.

## Date audit

All three rolls match the bill history line recording the same action: 3 of 3 exact. No
`official_vote_date` override is needed.

## Version checks and supersession

Each roll is the only kept floor vote on its measure in its chamber, so no judgment is superseded
and none needs `acknowledge_later_rolls`. Both bills passed both chambers in the form printed in the
enrolled Act; no conference report, concurrence or executive amendment appears in either history.

## Label reasoning: both measures are `general` with no stance

The obvious candidate area was `election_integrity` — "Ensure elections are secure, accurate,
auditable, and trusted by the public" — and it was rejected after reading the Acts, because the text
runs both ways inside that one area.

A yes vote can honestly be called pro-voter: without this machinery, voters in redrawn districts
would be stuck with nominees chosen under a map that no longer applies. A no vote can just as
honestly be called pro-integrity: the Act voids primary results already cast and certified, drops
the runoff so a nominee can win with well under half the vote, lets the Governor override the
statutory election calendar, and exists to smooth the path back to maps a federal court had blocked.
Neither reading is a distortion of the text. That is the same shape as SB 254 in the 2026 regular
batch, where the enacted text ran both ways inside public safety.

Alabama's own rule from that batch decides the rest: import without a stance when the vote is
divided, enacted and of clear public salience but no research area carries an honest direction; drop
only when the measure is both outside the taxonomy and low salience. A special session called for
redistricting, decided on party-line votes, is not low salience. Missouri reached the same place for
the same reason and imported its 2025 redistricting special session under `general` with no stance.

## The votes were exactly party-line

Every Republican present voted yes and every Democrat present voted no, on all three rolls:

| Roll | Yes | No | Not voting or absent |
|---|---|---|---|
| HB 1 House 1694623 | 75 Republicans | 29 Democrats | 1 Republican not voting |
| HB 1 Senate 1695918 | 27 Republicans | 8 Democrats | none |
| SB 1 Senate 1694713 | 26 Republicans | 7 Democrats | 1 Democrat and 1 Republican absent |

That split is itself part of why no stance was assigned. The division here tracks party position on
a redistricting fight, not a disagreement about whether elections should be secure and accurate.

## Duplicates

The dry run raised 4 related flags. A precise query — same event date, description naming the same
bill or a special primary election — confirmed all 4 as true duplicates: hand-written records for
Chip Brown, Heath Allbright, Margie Wilcox and Shane Stringer describing the same House vote on
HB 1. All four were retired before the import (`duplicate-retirements.json`, to re-run at production
promotion). One further hand-written record on 2026-05-08 says its candidate authored HB 1. That is
a sponsorship record, not a vote record, so it was correctly left alone.

## Import and reconciliation

- Dry run: 3 files, 0 errors, 150 planned inserts.
- Real run (stamp `2026-09-02T15:42:54.932Z`): **150 inserts, 0 errors, 0 notified.**
- Reconciled three ways: the report totals (150); the run-stamp predicate (150 rows, 122 distinct
  candidates — every candidate the crosswalk maps); and the Alabama roll-call total (1,825 before,
  1,975 after, of which 150 carry a 2262 run id).
- Tags: 150, all `general` with no stance — both sides of all three rolls, as a no-stance import
  should produce.
- Convergence: a follow-up dry run reports all 150 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 6 descriptions. Each description is 4 sentences
with no sentence over 45 words, and a British-spelling scan is clean. Reading grade was measured:
a first draft came in at a median Flesch-Kincaid 13.0, which was too heavy, so the descriptions were
rewritten with shorter clauses and plainer words. Final median 9.0, worst 9.3 — the plainest set in
the Alabama campaign so far.

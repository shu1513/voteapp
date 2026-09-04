# Alabama 2023 second special session batch-02 — judging notes

## Source

Judged from HB 5's **introduced** print, read in full, fetched through the LegiScan bulk API and
verified against the recorded byte length and MD5 hash. The bill was never engrossed. Its synopsis
states its own purpose plainly: to redraw the state's congressional districts, last drawn in 2021,
following the Supreme Court's decision in *Allen v. Milligan*.

The Senate amended the bill before passing it 23-7, and the amended text was never printed as a
separate document. The Senate description therefore says the Senate passed its own amended version,
and does not describe that version's contents.

## Roll-attribution and date audit

No roll call numbers appear in this session's descriptions, so the attribution check is vacuous.
Both rolls match their bill history lines exactly: 2 of 2.

## Label reasoning: `general`, no stance

The same reasoning as SB 5 in `../batch-01`, and the same as Missouri's 2025 redistricting session
and Alabama's 2026 special session: no research area in this taxonomy describes drawing legislative
boundaries, and scoring a map fight under election integrity would put a partisan question on an
administrative axis.

The description says this was one of two competing maps, names the decision that prompted the
session, and says the Senate's plan became law instead. It never says HB 5 became law.

## Duplicates

The precise sweep found **3 true duplicates**, all retired before the import
(`duplicate-retirements.json`, to re-run at production promotion): hand-written records for Chip
Brown, Margie Wilcox and Shane Stringer on the House vote of 2023-07-19.

## Import and reconciliation

- Dry run: 2 files, 0 errors, 109 planned inserts.
- Real run (stamp `2026-09-02T16:44:03.990Z`): **109 inserts, 0 errors, 0 notified.**
- Reconciled three ways: report totals (109); run-stamp predicate (109 rows, 109 distinct
  candidates); and the session total, 133 records carrying a 2060 run id, matching 24 + 109.
- Convergence: a follow-up dry run reports all 109 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 4 descriptions, 4 sentences each, no sentence
over 45 words, British-spelling scan clean. Median Flesch-Kincaid 8.0, worst 8.8.

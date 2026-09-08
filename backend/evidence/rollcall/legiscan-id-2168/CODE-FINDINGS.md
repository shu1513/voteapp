# Idaho code findings

Recorded, not fixed. Both apply to any Idaho batch, in either session.

## 1. Idaho prints struck and underlined text, and `pdftotext` flattens both

Idaho prints new language **underlined** and deleted language **struck
through**. `pdftotext` renders them identically, so a plain extract shows
repealed law as live and can invert an act. This is the Arkansas hazard, in the
same family as Kentucky's bold runs, Missouri's bold new matter, Oregon's bold
additions and Colorado's struck deletions.

**It is not a theoretical risk. It changes what an act says.** H0294 of 2025
extracts as one run-on sentence carrying both `two thousand dollars ($2,000)`
and `two hundred thousand dollars ($200,000)` and also `pursuant to 49 CFR
190.223`. Read flat, the act appears to cap pipeline-safety penalties at $2,000
a day with a $200,000 ceiling. Read with the marks:

```
shall be subject to a civil penalty [[of not to exceed two thousand dollars
($2,000) for each violation for each day that the violation persists. However,
the maximum civil penalty shall not exceed two hundred thousand dollars
($200,000) for any related series of violation]] <<pursuant to 49 CFR 190.223 at
the time the violation occurred.>>
```

The act **deletes** the dollar caps and substitutes the federal maximums, which
are far higher. A description written from the flat text would have said the
opposite of what the legislature did, on a measure that reaches roughly 87
candidates.

`tools/id_text.py` resolves the marks from the PDF's own drawn lines, by where a
rule sits relative to the glyph baseline: a rule at or just below the baseline
is an underline, a rule crossing the middle of the glyph box is a strike. It
prints `<<added>>` and `[[deleted]]`.

Two things to know when using it:

- **Unmarked text inside an amended section is existing law being reprinted, not
  a change.** Do not describe it as something the act did.
- **A section the act adds whole is printed plain, not underlined.** So a bill
  whose caption says "Adds to existing law" will show very few marks and still
  be entirely new law. S1180 of 2025 marks only 102 characters in a 6,915
  character act for exactly this reason. Read the section headers: `be, and the
  same is hereby amended by the addition thereto of a NEW SECTION` means the
  whole section is new.

The tool was validated on five acts before it was trusted, per the campaign rule
that a checker which never fires is indistinguishable from one that passes:
H0294 (81 added, 285 deleted), S1069 (673 / 359), H0187 (1,278 / 5), S1180
(102 / 0), H0245 (202 / 247).

## 2. LegiScan's description never names the question, so the bill history is the oracle

Covered in the README because it decides the config, and repeated here because
it is a code-shaped limitation rather than a data quirk: nothing in the fetch,
the classifier or `rollcall:judge` can see which question a roll answers. Only
the bill history line can, and it prints the question and the tally together.
The match runs at selection time and a roll it cannot place is left unselected.

This is the same limitation Delaware carries, and it is recorded the same way:
as a step the batch recipe owes, not as something a pattern can fix.

## 3. LegiScan's `passed` flag reads 0 on every Idaho resolution adoption

The flag is derived from the word PASSED or FAILED in the state's action line.
Idaho adopts resolutions, and an ADOPTED line maps to `passed: 0`. Measured:
20 rolls in 2025 and 23 in 2026 carry a 0 where Idaho's own page says ADOPTED.

Most are CR, JM and R measures that `LEGISCAN_KEPT_BILL_TYPES` rejects before
the queue, so nothing is stored for them. Joint resolutions are a kept type,
and `fetchLegiscanRollCallVotes.ts` writes `result` straight from the flag
(`rollCall.passed ? "Passed" : "Failed"`), so the four adopted joint-resolution
rolls in 2025 — HJR004 House 1506354 and Senate 1515095, HJR006 House 1517486
and Senate 1526800 — are stored as "Failed". Both resolutions go to Idaho
voters in November 2026.

Recorded in the config and deliberately **not** held, following the rule North
Dakota's config review settled for the same defect (#1236, 23 rolls there).
`heldRollCallIds` is for rolls whose tally or member list the state
contradicts; these four have correct tallies and correct member lists. `result`
is only read by the store's change detection, never by the judge or the
importer, so the defect changes no decision today, and none of the four is
closely divided, so none can enter a batch. Holding real passing votes to
correct a string nothing consumes would be the wrong fix. A first draft of the
config did hold them; it was reversed in the merge with main so that two states
do not treat one defect class two ways.

Not fixed in code either: the honest fix (taking `result` from the bill history
when the state prints it) touches every LegiScan state and is out of scope for
a config pull request. The rule for a batch is on selection: read a joint
resolution's outcome from Idaho's bill page, never from `passed`.

The flag is **right** on the three failed joint resolutions (HJR001 in 2025,
HJR007 and HJR009 in 2026), but only because their lines say FAILED. Idaho's
constitution requires two-thirds of all members for a constitutional amendment
(47 of 70, 24 of 35), so 46-23 is a defeat. Nothing in LegiScan knows that
threshold; do not infer a joint resolution's outcome from the tally.

## 4. LegiScan truncates Idaho's `Rules Suspended:` action at the colon

On a day the House suspends the rules to read a bill in full, Idaho's action
line carries the whole event: `Rules Suspended: Ayes 65 Nays 0 Abs/Excd 5, read
in full as required – ADOPTED - 58-10-2`. LegiScan's copy stops at
`Rules Suspended:`, so the vote's tally never reaches the feed. That is why the
tally audit could place only 707 of 854 rolls in 2025 and 643 of 852 in 2026,
and why the House rolls on HJR004 and HJR006 had to be confirmed against the
bill page rather than the dataset. The rule for a batch: when a roll has no
tallied history line on its date, fetch the bill page and read the tally there
before the roll is selected; never treat a missing line as a missing vote.

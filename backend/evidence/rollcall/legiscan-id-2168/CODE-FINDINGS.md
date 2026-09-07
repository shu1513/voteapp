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

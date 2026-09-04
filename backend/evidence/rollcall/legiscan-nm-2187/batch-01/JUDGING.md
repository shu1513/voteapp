# New Mexico batch-01 — judging notes

## Result

Imported on the local `voteapp` database on 2026-09-02. **14 roll calls, 14 measures, 818 records,
63 candidates, 602 area tags, 0 errors, 0 notifications.**

- Run stamp `2026-09-03T01:59:21.967Z`; the ledger is `import-report.json`.
- The dry run planned exactly 818 inserts, and its own stamp `2026-09-03T01:58:50.833Z` matches zero
  rows in the database, which is positive proof the dry run wrote nothing.
- Reconciled three ways: 818 rows carry the run stamp, 818 is the count of every live New Mexico
  roll-call record, and 818 is what the dry run planned.
- The convergence dry run reports all 818 unchanged, written to
  `import-dry-run-rerun-report.json`. `import-report.json` is still the original insert ledger.
- 63 candidates is every candidate the crosswalk maps. **New Mexico's Speaker votes**, so there is no
  gap of the kind Texas and Georgia have.
- **Production has zero New Mexico roll-call records.**

Tag arithmetic was predicted before it was checked and agreed exactly. Every label sets the nay side
to null, so a tag is written only on the yes side: the sum over the 14 measures of yes voters times
labels is 602, and the database holds 602.

## Where the judgments come from

Each description was written from the **enrolled act**, read in full. The Legislative Finance
Committee's fiscal impact report was used only as an index to find the right sections.

That distinction earned its keep three times.

1. **Senate Bill 120.** The report's synopsis describes a version that lists emergency department
   and urgent care visits among the services that carry no cost sharing. A later committee amendment
   struck them, and the enrolled act does not list them. The description does not claim them.
2. **Senate Bill 124.** The report's synopsis of an earlier version carried a sentence letting the
   superintendent delegate subpoena power to staff. The Senate Rules Committee removed it; the
   enrolled act has no such sentence.
3. **Senate Bill 16.** The report says the act lets both unaffiliated and minor-party voters into
   primaries. **The enrolled act opens primaries only to voters who declined to designate a party.**
   The phrase covering minor-party registrants appears nowhere in it. The description says only what
   the act does.

A fourth: **House Bill 12's own fiscal impact report contains a typo of the kind Florida's analyses
have** — in the middle of the House Bill 12 analysis it says "HB 2 could generate significant
long-term savings". Nothing turned on it, but it is one more reason the report is an index only.

## The underscore check

New Mexico prints an amended statute in full and underscores only the bill's own new language, and
plain text extraction loses the underscore. Each selected measure's new language was extracted by
matching characters to the underline rule drawn beneath them, so the description describes what the
act changes rather than law the act merely reprints. See CODE-FINDINGS.md, finding 5.

This is what kept two descriptions honest.

- **House Bill 6** reprints all of Section 13-4-11, seven pages of it. Its only new language is the
  section heading and new Subsection J, which is the prevailing wage requirement for projects backed
  by an industrial revenue bond. The description covers Subsection J and nothing else.
- **House Bill 89's** removal of the citizenship requirement is visible only as bracketed text in the
  introduced print: `[A. is a citizen of the United States or who`. The enrolled act simply does not
  contain the word "citizen", which on its own proves nothing.

## Wording choices worth recording

- **House Bill 12.** The description states the new relinquishment rule and does not say what the old
  deadline was. The fiscal report says the old law gave 48 hours; the act's own text says only
  "immediately upon service of the order or as directed by the court", so the old figure is not
  quoted.
- **House Bill 128.** The act creates the local solar access fund, but the version the House passed
  had its 60 million dollar appropriation stripped by committee. The description says the fund was
  set up with no money from the state's main account, so no reader takes it for a spending vote.
- **House Bill 586.** The act is mostly a widening of the state's review of hospital mergers, and its
  single largest effect is repealing the July 1, 2025 date on which that review would have ended. It
  also makes most of the material the state gathers in a review confidential, which points the other
  way, so the description says so.
- **Senate Bill 9.** The act sets no dollar figure of its own; it ties the state's ceiling to the
  federal maximum. The description says that rather than inventing a number.
- **Senate Bill 21.** The description leads with what the act does that a reader can act on: it makes
  it unlawful to release a pollutant from a single source into a federally covered waterway without a
  state permit, which is how New Mexico takes over running the federal program.

## Reading level

The repository's plain-language lint checks sentence length only, so reading grade was measured
separately.

**Median Flesch-Kincaid grade 8.7, worst 10.2, longest sentence 36 words, mean sentence 18.8 words;
lint warnings 0 over all 28 descriptions.** A first draft measured a median of 9.4 and a worst of
13.9, and six measures were rewritten before importing, not after.

Grade 9 is the honest floor here. Getting below it means dropping the terms these acts are built on —
"superintendent of insurance", "prevailing wage", "Immigration and Nationality Act" — and dropping
exactly those terms is what has caused correction rounds in earlier states. The user's two-to-four
sentence guidance and the seventh-grade target pull against completeness, and completeness won:
descriptions run five to seven short sentences.

Checks run before the judgments file was written: every sentence at most 45 words, the phrase
", The " absent from every description, both sentences of every pair carrying the roll's own tally,
and a scan for British spellings across the descriptions and these notes.

## Related records

One flag, and it is a false positive. A hand-written record for Andrea Romero describes her
co-sponsoring House Joint Memorial 4, dated 2025-03-20, the same day as the Senate Bill 267 roll.
Different measure, distinct claim. Nothing retired.

A wider sweep for any pre-existing record mentioning one of the 14 bill numbers among New Mexico
candidates returned nothing at all.

## Gates

The superseded-stage gate never fired and no roll needed `acknowledge_later_rolls`. New Mexico's feed
holds exactly one roll per bill per chamber, so no later or same-day peer exists anywhere in the
session.

No roll needed an `official_vote_date` override. All 14 dates match the state's own roll call sheets.

# Alaska batch-01 — how it was judged

Every description was written from the **enrolled Act**, the text that became law. Alaska
publishes no neutral legislative-agency analysis, and the sponsor statement and sectional
analysis on each bill page are prepared by the sponsor, so neither was used as a source.

## Checks that ran before the import

**Member lists audited against Alaska's own journal, all eight rolls.** The bill page history
gives the journal page for each action; the journal prints the roll call by name. Every member
LegiScan places on a side is on that side in the journal. **Zero members on the wrong side.**
Three names came back unmatched by the parser and were confirmed by eye — Will Stapp in
HB 48's nays, Jubilee Underwood in HB 35's nays, Sarah Vance in HB 184's nays — each the last
name on a list that the journal's page-break footer splits.

This audit is not optional in Alaska. Indiana's feed puts members on the wrong side, and the
only way to know is to read the state's own record.

**Dates.** Every roll's date matches the tallied action line in Alaska's bill history. No
`official_vote_date` override was needed.

**Version check.** Every roll is on the text that became law. Six of the eight are the
chamber's vote on the enrolled text directly. HB 33 and HB 48 were passed unamended, and the
enrolled Act was diffed against the introduced bill for each: the only differences are page
headers and footers.

**Superseded-stage check, run before judging rather than waiting for the gate.** One SQL pass
over all eight rolls for a later vote by the same chamber on the same measure, and a second for
same-day peers. Both returned zero, so no judgment needed `acknowledge_later_rolls`. That check
is also what removed HB 13 and HB 28 from the batch: each chamber's final vote on those bills
is not divided, so the divided roll is superseded and attributing it would misstate the
members' final position.

**Bold-font read.** Alaska prints new statutory language in bold and deletions in bracketed
capitals, and `pdftotext` throws the bold away, so a reprinted statute reads as if it were all
new. `ak_bold.py` prints the share of bold characters per line. It settled HB 33: the only new
words in its first section are `except as provided in AS 39.52.220(c)`, and the long list of
things a public officer may not do is existing law carried along. One nuance worth keeping — a
section the Act adds whole is printed plain, so the bold read finds insertions into reprinted
statute, not new sections.

**Language.** The plain-language lint reported 0 warnings over all 16 descriptions. Measured
separately: Flesch-Kincaid median grade **8.9**, worst **9.3**, longest sentence 39 words. The
body and the closing tally sentence are joined with a period, and the builder asserts that
`", The "` appears nowhere. A British-spelling scan ran over the descriptions and these notes.

**⚠ Sentence count deviates from the 2-to-4 target.** These run 6 to 9 short sentences. Getting
to grade 9 meant breaking long sentences rather than dropping content, and the content that
would have to go is the statute's own limits — the exemptions, the "may" versus "must", the
effective dates. Dropping exactly those is what caused most of this campaign's correction
rounds in other states, so reading level was treated as binding and sentence count was not.

## Facts worth recording

**HB 57 became law over a veto.** The governor vetoed it and the legislature overrode the veto
46-14 in a joint session of both chambers. The batch imports the House concurrence of
2025-04-30, not the override, because the override is a joint-session vote the data model
cannot hold honestly (see `../CODE-FINDINGS.md`). The descriptions name the override so a
reader is not left thinking the bill simply passed.

**HB 57's conditional sections are deliberately not described.** Sections 10, 11, 13 and 15 —
the vocational funding factor, its spending mandate, the reading grants, and the digital-business
tax earmark — take effect only if SB 113 or a similar bill passes. SB 113 was vetoed and the
veto was sustained, so those sections never took effect. Describing them as things the law did
would be false, so the descriptions cover the unconditional core only.

**Four measures became law without the governor's signature** — HB 27, HB 35, HB 48 and
HB 184. Their tail sentences say so rather than saying the bill was signed.

**HB 184's second subject is stated, not hidden.** Besides letting the state's development bank
lend for workforce housing, the Act repeals a 2018 law that was due to end a municipal tax
limit on private interests in bank-owned shipyards and ports on 30 November 2027. Repealing it
keeps the limit. The 2018 chapter had to be fetched and read to establish that; the Act itself
only cites it by section number.

**SB 183's crime carries a defense.** Blocking the audit committee is a crime only if the
person did not reasonably believe the refusal was legally justified. The description says so.

## Related-record flags

Five flags, all reviewed, none a duplicate, nothing retired. Each is a pre-existing
hand-researched record about the HB 57 or HB 78 **veto override** — a different vote from the
concurrence this batch imports. A wider sweep over every Alaska candidate record naming any of
the batch's bills found more of the same shape, plus sponsorship rows and records about HB 57's
earlier 16 April passage vote and SB 183's 10 May vote. Becky Schwanke's existing SB 183 record
describes the 10 May vote where this batch imports the 12 May reconsideration; she voted no on
both, so the two records agree.

The flags themselves are noise rather than signal: the importer's related check only
understands federal bill spellings, so on a state measure it falls back to "this candidate has
another record containing the word vote".

## Ledgers

- `import-dry-run-report.json` — the plan: 36 planned inserts, 0 errors.
- `import-report.json` — the real run: **36 inserts, 0 errors, 0 notified**, stamp
  `2026-09-03T02:04:43.208Z`.
- `import-dry-run-rerun-report.json` — the convergence run: all 36 unchanged.

Reconciled three ways. The database holds 36 live records across 6 candidates for that stamp;
28 area tags, which is exactly the yea-side record count plus HB 33's nay-side records, the
only measure with a stated nay; and the dry run's own stamp matches zero rows, which is proof
it wrote nothing.

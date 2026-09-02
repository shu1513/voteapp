# Kentucky 2025 batch-02 — how these judgments were made

## Sources

Every judgment was written from Kentucky's own documents. No AI provider was
called, and no research was delegated — batch-01's reading was fanned out to
research agents, and one of them reported sections it had not actually read, so
this batch was read here from end to end.

- The **enrolled Act** at
  `apps.legislature.ky.gov/recorddocuments/bill/25RS/<bill>/bill.pdf`.
- The **official vote record** at
  `apps.legislature.ky.gov/record/25rs/<bill>/vote_history.pdf`.
- The **bill page** at `apps.legislature.ky.gov/record/25rs/<bill>.html`, used
  for the action history and, in one case, for the Legislative Research
  Commission's index headings.

All three need a browser user agent.

## Reading the Acts

Kentucky reprints a whole statute when it amends any part of it, and marks the
**new language in bold** and deletions in square brackets. Plain text extraction
throws the bold away. Every Act here was read by extracting the fonts and
printing only the bold runs, which is exactly the language the Act adds; the
bracketed deletions were then read alongside, because on HB 196 and HB 424 the
deleted text is what tells you the direction of the change.

**The batch-01 judging source was not used.** The Legislative Research
Commission's Summary of Enacted Version misstated the Act in six of batch-01's
twelve measures, so every claim below was read out of the Act itself. The one
place the bill page was still relied on is HB 495: the Act cancels Executive
Order 2024-632 without saying what the order did, and the Commission's own index
headings for the House floor amendment name the subject — "Executive order,
conversion therapy, no force or effect".

## Version checks, roll by roll

Seven of the eleven measures were vetoed and overridden, and Kentucky takes an
override vote on the enrolled Act with no amendment possible, so those rolls
need no further check. The other rolls were each traced through the bill
history:

- **HB 2 Senate, 14 March, 30-6.** The Senate receded from its own committee
  substitute and passed the House text, which was enrolled the same day. The
  Senate's own override roll is not in the LegiScan feed.
- **HB 6 Senate, 13 March, 29-6.** The Senate passed the House bill unamended
  and it was enrolled the same day. Its override roll is likewise not in the feed.
- **HB 45 Senate, 11 March, 29-6.** Passage with Senate Committee Substitute 1,
  which the House then concurred in without further change.
- **HB 137, both chambers.** The Senate passed the House text unamended.
- **HB 455 House, 14 March, 71-17.** Concurrence in the Senate committee
  substitute, enrolled the same day.
- **HB 662 House, 28 March, 77-17.** Concurrence in the Senate committee
  substitute, enrolled the same day.
- **SB 2 Senate, 18 February, 31-6.** The House passed it 73-12 with no
  amendment adopted, so the Senate's text is the enacted text.

Every roll was also matched to Kentucky's own vote record by sequence number,
and all eighteen tallies agree.

## One acknowledged later roll

SB 65 in the House is the session's reconsider-and-revote sequence. Kentucky's
record shows RCS# 331 passing 72-15, RCS# 333 reconsidering that vote 68-12, and
RCS# 334 passing 75-18. RCS# 334 is the vote that stands, but the pipeline's
superseded-stage gate cannot order two kept rolls on the same day, so roll
1529597 (RCS# 331) is listed in `acknowledge_later_rolls` on the judgment for
roll 1529599 (RCS# 334). This is the only acknowledgment in the batch.

## Direction calls worth recording

**HB 6 and SB 65 are both `government_efficiency` / for**, following the SB 84
call from batch-01: when a measure's subject is the machinery of regulation
itself rather than an outcome, that is the area it belongs in. HB 6 freezes new
agency regulations behind a certification; SB 65 voids five specific proposed
regulation changes and bars re-proposing them until June 2026. The descriptions
name the regulations SB 65 voids, because two of them are about Medicaid
behavioral health coverage and coal mine bonding and a reader should know that.

**HB 137 is `environment_and_public_health` / against.** The Act does not change
any pollution limit. It changes what evidence may support an enforcement action,
and shuts out data gathered by any method the federal EPA has not approved or
accepted — including in a citizen's own case. Narrowing the evidence narrows the
enforcement.

**HB 216 is `anti_corruption` / against.** Kentucky's ethics law bars a public
servant from holding or bidding on a contract from the agency that employs them.
The Act writes Department of Agriculture employees outside the Office of
Agricultural Policy out of that bar for farm funds and contracts, and makes the
exemption retroactive to March 2021. That is the same call batch-01 made on
HB 520.

**HB 495 and SB 2 are both `civil_rights` / against**, following the batch-01
HB 4 call and the Ohio, Georgia and Tennessee precedents behind it. HB 495 bars
Medicaid spending on gender-related care and cancels the conversion therapy
executive order. SB 2 bars public money for the same care for people in
correctional facilities. Each names its own limiting provision — SB 2 lets a
doctor taper a treatment already started.

**HB 45 and HB 455 are both `election_integrity` / for.** HB 45 bans foreign
national money in ballot-question campaigns. HB 455 creates an election
investigations unit in the Attorney General's office with subpoena power and a
public annual report.

**HB 662 is `data_privacy` / for.** The Act lets a judge or a judge's immediate
family have personal details removed from any government agency's public view
within 72 hours. It also carries a small unrelated change to insurance appeal
letters, which is not scored.

**Two measures carry no stance at all.** HB 2 pairs a sales-tax exemption for
currency and bullion with a damages remedy against the state, including a waiver
of sovereign immunity and $1,000 a day payable out of the Governor's office
budget. HB 424 broadens the grounds for removing a tenured faculty member while
lengthening the notice period before removal. Labeling only one strand of either
would mislead by omission, so `general` tags both sides topically with no stance.

**Every stance label sets `nay: null`.**

## Writing

Descriptions are one short paragraph, two to four sentences, no sentence over 45
words, written for a reader with no legal background. The builder at
`/Users/shu/legiscan-data/ky25b2_measures.py` asserts those limits and asserts
that the comma-splice string `", The "` appears nowhere. The repo's own
`listPlainLanguageWarnings` was run over all 36 descriptions **before**
importing: **0 warnings**, longest sentence 42 words, mean 19.7. A
British-spelling scan was run over the descriptions and this directory.

## The run

Judge: 18 judgments, all `updated`. The tally-in-sentence gate passed with no
edits. The superseded-stage gate stopped the run once, on SB 65 in the House,
and was answered with the acknowledgment described above rather than by changing
the selection.

The import dry run planned 916 inserts across 18 files with 0 errors and 0
`ambiguous`. The real run inserted exactly **916 records across 107 candidates**,
0 errors, 0 notified, stamp `2026-09-02T06:32:15.133Z`.

Reconciled three ways:

- rows carrying the run stamp: **916 records / 107 candidates**
- all Kentucky 2025 roll-call records in the database: **2,067 / 107** — 1,151
  from batch-01 plus these 916
- the dry run's own stamp `2026-09-02T06:32:04.387Z` matches **zero** rows, which
  is positive proof `--dry-run` wrote nothing

Tags: **804**, predicted independently from the crosswalk and the evidence files
before checking, and the database agrees exactly — 211 `general`, 172
`government_efficiency`, 103 `civil_rights`, 88 `anti_corruption`, 83
`environment_and_public_health`, 78 `election_integrity`, 69 `data_privacy`.

A convergence dry run reports all 916 `unchanged`. The insert ledger is preserved
in `import-report.json`.

## One rewrite after review

Review of the pull request caught that the HB 45 description said a donor may not
have taken more than $100,000 "from one" foreign national. The Act's four
certification clauses do say "from a foreign national", but the clause that makes
the threshold enforceable — the presumption of a violation — aggregates funds
"from one (1) or more foreign nationals", so $60,000 from each of two foreign
nationals crosses it. Both descriptions now say "in total from foreign
nationals". The judgment was re-applied and a real re-run rewrote the 16 HB 45
records (`import-rerun-report.json`, stamp `2026-09-02T14:48:03.652Z`, 900
`unchanged` and 16 `rewrite`, insert ledger untouched). The database reads back
16 records with the new wording and none with the old, and a fresh convergence
dry run again reports all 916 `unchanged` (`import-dry-run-rerun-report.json`).

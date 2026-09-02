# Kentucky 2026 batch-01 — how these judgments were made

## Sources

Every judgment was written from Kentucky's own documents. No AI provider was
called, and no research was delegated.

- The **enrolled Act** at
  `apps.legislature.ky.gov/recorddocuments/bill/26RS/<bill>/bill.pdf`. This is
  the ground truth for what each measure did.
- The **official vote record** at
  `apps.legislature.ky.gov/record/26rs/<bill>/vote_history.pdf`, which gives each
  roll's sequence number, the question in plain words, the date and the full
  member lists.
- The **bill page** at `apps.legislature.ky.gov/record/26rs/<bill>.html` for the
  action history.

All three need a browser user agent.

## Reading the Acts

Kentucky reprints a whole statute when it amends any part of it, and marks the
**new language in bold** and deletions in square brackets. Plain text extraction
throws the bold away, so a long Act can look like a large change when almost
every word is existing law copied out.

Every Act here was read by extracting the fonts and printing only the bold runs,
which is exactly the language the Act adds. Senate Bill 251 is the clearest
case: four pages of statute, of which the Act changes two short subsections.
The helper is `/Users/shu/legiscan-data/ky_bold.py`.

**The 2025 batch's judging source was not used.** Kentucky's Legislative
Research Commission publishes a Summary of Enacted Version on the bill page, and
in the 2025 batch it misstated the Act in six of twelve measures. Every claim
below was read out of the Act itself instead.

## Every roll was checked against Kentucky's record

All nineteen rolls were matched to Kentucky's own vote history by sequence
number, and the tallies agree in every case. The check matters because
**LegiScan's description is its own claim about the question, not Kentucky's**,
and in this session the House files nearly every substantive floor vote under
the single label `House: Veto Override`.

Two rolls show how far the two can drift apart. Kentucky's record calls the
House's 1 April vote on SB 199 `Final Passage` and its 31 March vote on HB 58
`Final Passage`, while LegiScan labels both `House: Veto Override`. The bill
histories settle it: SB 199 was vetoed and the House overrode the veto on 1
April, and HB 58 was never vetoed — the House concurred in the Senate's changes
and the Governor signed the bill on 10 April. The descriptions say what actually
happened in each case.

## Direction calls worth recording

**Three liability shields, all `corporate_accountability` or `gun_control` /
against.** SB 199 deems a federally approved pesticide label a sufficient
warning, which ends state failure-to-warn suits. HB 78 bars most suits against
gun makers and sellers over criminal misuse of a firearm. Both remove a route to
holding a company liable, which is the same call Kentucky's 2025 HB 398 drew.

**HB 312 is `gun_control` / against, not `public_safety_and_crime_control`.**
Extending concealed carry licenses to 18, 19 and 20 year olds is a firearms
policy question, and scoring it as public safety would beg the question the vote
was about.

**SB 251 keeps a stance despite a counter-provision**, following the 2025 SB 89
precedent. The Act's dominant thrust is that execution protocols no longer go
through the public regulation process — no notice, no public comment, no
legislative committee review. The website posting requirement runs the other way
and is named in the description, and `nay` is null. This matches the 2025 HB 520
call, where narrowing the open-records test was `anti_corruption` / against.

**SB 173 is `government_efficiency` / for**, following the 2025 SB 84 call: when
a measure's subject is the machinery of oversight itself rather than an outcome,
that is the area it belongs in. The Act requires the Medicaid agency to publish
its plans and submit every requested federal change with a cost estimate.

**SB 77 is `environment_and_public_health` / for.** The Act funds clinical
trials of a possible opioid-addiction treatment and requires the partner company
to plan for access for uninsured and low-income Kentuckians. That is a public
health measure, the same area the 2025 SB 100 took.

**SB 59 is `election_integrity` / for.** The Act bars public money, buildings,
websites and staff time from campaigning on ballot questions, while explicitly
preserving a public worker's right to speak on personal time with personal
resources. The free-speech reading is real, but the Act's own carve-out is what
answers it.

**SB 100 carries no stance at all**, following the 2025 HB 90 and HB 695 calls.
The Act moves two of five executive committee seats from the Governor to the
Attorney General, gives the executive director full authority with the board
advising, frees the commission from Finance Cabinet approval, and closes
utility-marked confidential information to open records requests. Naming only
one of those strands would mislead by omission. `general` tags both sides
topically with no stance.

**Every stance label sets `nay: null`.** A no vote on one bill is not evidence a
member opposes the area's whole goal.

## Filter 5 set-asides

Four measures were read and set aside rather than dropped, because each is a
reasonable batch-02 candidate once the two-strand question is settled: SB 195
(road-contractor liability shields plus address confidentiality for prosecutors
and public defenders), SB 104 (a 25-foot buffer around first responders plus
rescue-squad benefits), SB 29 (out-of-county waste facilities) and HB 652 (a
program moved between agencies). Their rolls stay `candidate:batch-02` in the
worklist.

## Writing

Descriptions are one short paragraph, two to four sentences, no sentence over 45
words, written for a reader with no legal background. The builder at
`/Users/shu/legiscan-data/ky26_measures.py` asserts those limits and asserts
that the comma-splice string `", The "` appears nowhere. The repo's own
`listPlainLanguageWarnings` was run over all 38 descriptions **before**
importing: **0 warnings**, longest sentence 37 words, mean 19.0. A
British-spelling scan was run over the descriptions and this directory.

## The run

Judge: 19 judgments, all `updated`, no gate failures. The tally-in-sentence gate
and the superseded-stage gate both passed with no edits and no
`acknowledge_later_rolls`.

The import dry run planned 997 inserts across 19 files with 0 errors and 0
`ambiguous`. The real run inserted exactly **997 records across 106 candidates**,
0 errors, 0 notified, stamp `2026-09-02T06:17:37.254Z`.

Reconciled three ways:

- rows carrying the run stamp: **997 records / 106 candidates**
- all Kentucky roll-call records in the database: **2,148** — 1,151 from the 2025
  session and 997 from this one
- the dry run's own stamp `2026-09-02T06:16:55.222Z` matches **zero** rows, which
  is positive proof `--dry-run` wrote nothing

Tags: **833**, predicted independently from the crosswalk and the evidence files
before checking, and the database agrees exactly — 173 `gun_control`, 151
`corporate_accountability`, 103 `general`, 88 `election_integrity`, 88
`government_efficiency`, 85 `environment_and_public_health`, 84
`anti_corruption`, 61 `data_privacy`.

A convergence dry run reports all 997 `unchanged`. The insert ledger is preserved
in `import-report.json`; the convergence run wrote
`import-dry-run-rerun-report.json`.

## One report-only flag

Seven of the nineteen rolls carry `related: 1`. That flag does not block or
change a write — all 997 rows were plain inserts. It fires because the importer
looks for an existing uncited record that might be the same vote told off a press
release, and its fallback test is the single word "vote" appearing anywhere in a
description. On a state measure the precise test cannot run, because it only
understands federal measure spellings. See `../CODE-FINDINGS.md`.

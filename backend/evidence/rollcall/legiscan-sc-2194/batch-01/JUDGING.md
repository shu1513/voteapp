# South Carolina batch-01 — judging and import

## Source

Every description was written from the **ratified act text**, which South Carolina publishes in
full at the foot of the bill page (`https://www.scstatehouse.gov/sess126_2025-2026/bills/<n>.htm`).
South Carolina has no legislative-analysis office writing a neutral summary, so there is no
second document to check the act against; there is also no sponsor statement of intent anywhere
on the page, so the Texas advocacy hazard does not arise. Each act was read from the enacting
clause to the effective date, not in excerpts.

The version each roll actually voted was read separately, from the dated printed version, and
compared with the act. See PLAN.md for the per-roll result.

## What each act does, and why the label

**H 3276, the Hands-Free and Distracted Driving Act** — `public_safety_and_crime_control`, yes =
for. The act bars holding a phone, texting and watching video while driving; fines are $100 then
$200 with two points on the driving record; and it bars an officer from searching the phone
or the car on this violation alone. The road-safety strand and the policing-limits strand both point the same way,
which is the area's own description ("effective policing, prevention, accountability").

**H 4216, income tax** — `personal_income_tax_reduction`, yes = for. A rate cut is the area's
literal subject. The act also caps the state earned income tax credit at $200, where it had been
125 percent of the federal credit with no cap, which is a tax increase for some low-paid workers.
That counter-strand sits inside the same area, so it is stated in the description rather than
hidden, and the stance follows the dominant thrust.

**H 4756, the Student Physical Privacy Act** — `civil_rights`, yes = against. Restroom, locker
room and sleeping-quarters rules keyed to sex at birth follow the line already drawn for Ohio
SB 1, Texas SB 12, Pennsylvania SB 9 and Indiana HB 1041.

**S 171, waste tires** — `environment_and_public_health`, yes = for. A tracking system for waste
tires, a ban on installing unsafe used tires, and money aimed at tire recycling. The new $2 fee on
used tires is a household cost, which is a different area, so it does not cut against the
environmental direction.

**S 214, the Commission for Minority Affairs** — `civil_rights`, yes = against. The act removes
the statutory requirement that most of the commission's members be African American, rewrites its
studies and duties from named minority groups to "communities" and "rural and under-resourced"
areas, and ends its duty to help the minority community with Voting Rights Act filings.

**S 287, electronic nicotine delivery systems** — `environment_and_public_health`, yes = for. A
product directory keyed to federal authorization, with kid-appealing branding banned. Follows
Pennsylvania HB 1425, Texas SB 2024 and Alabama HB 8.

**Every label states `nay: null`.** In each case there is a plausible no vote that does not amount
to opposing the area's goal: an officer-discretion objection on H 3276, a revenue objection on
H 4216, a school-funding-penalty objection on H 4756, a fee objection on S 171, a
consultation objection on S 214, and an adult harm-reduction objection on S 287. The last of those
sits on contested evidence, which the fluoride rule keeps out of a stance.

## A duty that looked substantive and was not

S 214 also deletes the commission's duty to run "a twenty-four hour toll free telephone number and
electronic website in accordance with Section 8-30-10". **Section 8-30-10 was repealed in
February 2024**, so that duty pointed at a statute that no longer exists and removing it is
housekeeping. The first draft described it as a service the act ended. It was cut. Check the
cross-reference before describing a deleted duty as a loss.

## Two corrections after review

**H 4756, §§59-23-520 and 550.** The first draft said "a school" loses a quarter of its state
operating money; the act withholds funds from the school **district** (or, for higher ed, the
institution), not the individual school. It also said finding someone of the other sex in a
restroom is enough to sue; §59-23-550(A) actually requires the school to have granted permission
for the violation or failed to take reasonable preventive steps (e.g., signage). Both sentences
were rewritten and the 100 fanned-out records were rewritten in place via `rollcall:judge` +
`rollcall:legiscan:import`.

**S 287, §44-95-65(B) and (N).** The first draft said only FDA-cleared or timely-pending products
qualify; §44-95-65(B) also qualifies a product whose FDA marketing denial is stayed by the agency
or a federal court. It also said sales of an unlisted product stop the moment the directory
publishes; §44-95-65(N) actually gives retailers 60 days after publication to sell off existing
unlisted inventory before it becomes seizable. Both sentences were rewritten and the 94 fanned-out
records were rewritten in place the same way.

## Writing checks, all run before the import

- The repo's `listPlainLanguageWarnings` over all 12 descriptions: **0 warnings**.
- Body and closing tally sentence joined with a period; the builder asserts `", The "` appears in
  no description.
- Both the yes and no description of every roll quote that roll's own tally.
- A British-spelling scan over 26 spellings, run over the descriptions and over these
  documents: none in the descriptions, one in an early draft of this file ("licence points"),
  fixed before the commit.
- **Reading level measured separately, because the 45-word lint is not a readability check.**
  Flesch-Kincaid grade: median **7.3**, worst **11.9** (S 214), mean sentence 12 to 17 words,
  longest sentence 25 words.

A first draft measured a median grade of 12.3 and a worst of 15.6 with 4 to 5 long sentences per
description. It was rewritten into 8 to 10 short sentences before anything was imported. That
breaks the 2-to-4-sentence guidance, and the trade was made deliberately: the alternative was to
drop the statutory limits, which is what has caused most of this campaign's correction rounds.

S 214 sits at grade 11.9 because of words that cannot be swapped out — "State Commission for
Community Advancement and Engagement", "under-resourced", "ethnic and racial diversity" and the
named minority groups the old law listed. Its sentences average 15 words.

## Import

Local `voteapp` only. Production has zero South Carolina roll-call records.

- Dry run: 6 files, **601 planned inserts**, 0 errors, 0 notified, 0 related flags.
- Real run: 6 files all `imported`, **601 inserts**, 0 errors, 0 notified. Stamp
  `2026-09-03T01:56:40.975Z`.
- Convergence dry re-run: all **601 unchanged**.

Reconciled three ways:

1. The ledger reports 601 inserts.
2. `candidate_records` rows whose `origin_run_id` ends in the run stamp: **601**, across **116
   distinct candidates** — every candidate the crosswalk maps. South Carolina's Speaker votes, so
   there is no shortfall of the kind Texas and Georgia have.
3. The dry run's own stamp `2026-09-03T01:56:16.953Z` matches **zero** rows, which is positive
   proof that `--dry-run` writes nothing.

Tags: **413**, predicted from the ledger before the count was read. Every label states
`nay: null`, so only yes-side records carry a tag, and the yes-side records across the six rolls
number exactly 413.

A wider duplicate sweep was run beyond the importer's own `related` scan, because that scan only
looks at records sharing the roll's date: a search over every hand-written record belonging to a
South Carolina state-legislative candidate, for the six bill numbers and for the phrases
"Hands-Free", "Student Physical Privacy", "Minority Affairs", "waste tire" and "electronic
nicotine", returned nothing. No records were retired.

## Ledgers

- `import-dry-run-report.json` — the plan, 601 planned inserts
- `import-report.json` — the real run, 601 inserts
- `import-dry-run-rerun-report.json` — the convergence run, 601 unchanged

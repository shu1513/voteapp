# Missouri batch-01 — judging and import

**Result on local `voteapp` 2026-08-29: 10 files all `imported`, 0 errors, 583 inserts,
0 notified, 92 candidates, 1,166 tags. PRODUCTION UNTOUCHED.**

Reconciled three ways:

- the dry run planned **583 inserts** (`import-dry-run-report.json`) and the real run inserted 583
  (`import-report.json`);
- `candidate_records` with `origin = 'rollcall_import'` went 36,303 → **36,886**, exactly +583;
- the provenance predicate
  `origin_run_id LIKE 'rollcall:MO:%:2169:%:2026-08-29T05:33:20.744Z'` returns **583 rows / 92
  candidates**, and the dry run's own stamp `2026-08-29T05:33:03.574Z` matches **zero** rows —
  positive proof `--dry-run` is inert.

A dry re-run then reported all 583 `unchanged` (`import-dry-run-rerun-report.json`), leaving the
insert ledger intact. (A *real* re-run would have overwritten `import-report.json`; the Tennessee
lesson.)

**92 candidates, not the 94 the crosswalk maps.** Brian Williams and Doug Clemens are sitting
legislators whose only 2026 candidacy is the 4 August county *primary*; with no November row yet
they fall outside the pipeline's `--scope-from 2026-11-01` and resolve as `out_of_scope`. Their
identities are reviewed and correct, so a later November roster plus a re-import adds their records
idempotently.

## Sources

Every judgment was written from the Missouri House's official **Truly Agreed** bill summary
(`documents.house.mo.gov/billtracking/bills251/sumpdf/<PADDED>T.pdf`) and then confirmed clause by
clause against the **enrolled bill text**. Where the two disagreed, the enrolled text won — see the
summary errors below. Version identity per roll came from the LR number stamped on the official
House roll-call PDF, checked against the enrolled printing.

**Advocacy is quarantined in Missouri, and not where Texas puts it.** The *Committee* (`C`) summaries
carry labelled PROPONENTS / OPPONENTS testimony digests, and HB 495's carries an explicit sponsor
statement of intent. The Introduced, Perfected and Truly Agreed summaries are neutral third-person
description. Judging from `T` avoids it entirely — the Texas advocacy-preamble hazard does not recur
as long as you never read from `C`.

## Labels

Seven measures, **fourteen labels** — every measure carries one label per strand rather than a single
flattened stance (the Florida SB 700 pattern).

| measure | labels |
|---|---|
| HJR 73 | `womens_reproductive_rights`/**against** · `civil_rights`/**against** |
| HB 594 | `personal_income_tax_reduction`/**for** · `cost_of_living_reduction`/**for** |
| HB 567 | `reduce_wealth_gap`/**against** · `corporate_accountability`/**against** |
| HB 145 | `anti_corruption`/**against** · `data_privacy`/**for** |
| SB 152 | `election_integrity`/**for** · `data_privacy`/**for** |
| HB 595 | `housing_affordability`/**against** · `civil_rights`/**against** |
| SB 71 | `public_safety_and_crime_control`/**for** · `anti_corruption`/**against** |

Directions follow the **area description**, not the bill's politics — `immigration` reads "welcome
immigration…", so enforcement-tightening scores against it; `personal_income_tax_reduction` is
literally personal income tax.

`HB 145` and `SB 71` are the clearest multi-label cases: HB 145 closes four categories of government
record (against `anti_corruption`) while the records it closes are personal — minors' information,
utility billing, park reservations (for `data_privacy`). SB 71 creates new car-break-in offences and
fingerprint background checks (for `public_safety_and_crime_control`) while letting public retirement
systems close records tied to their investments and dropping the SEC-registration requirement for
the LAGERS investment counsel (against `anti_corruption`).

## Traps handled

- **HJR 73 is a joint resolution.** It needs no governor's signature and is not law: it takes effect
  only if Missouri voters approve it. Every description says so; none says "became law".
  Its own § B ballot summary — written into the resolution — never tells voters it *repeals* the
  reproductive-freedom right they added in 2024, so the descriptions are written from § 36(a), not
  from the ballot language.
- **HB 567 is the batch's gut-and-replace.** As introduced it merely *delayed* the earned paid sick
  time law to 1 January 2026 and kept it; HCS #2 repeals the law outright. The bill number, the
  sponsor and even the roll-call PDF's own subject line ("PAID SICK LEAVE FOR CERTAIN EMPLOYEES")
  describe the introduced bill. The description is written from the enacted text.
  It also does **not** repeal the $13.75 (2025) or $15.00 (2026) minimum wage — only the automatic
  inflation adjustment from 2027 on — and it *extends* the minimum wage law to public employers,
  which the description states.
- **Emergency clauses are separate questions wearing the passage desc.** HB 567's 84-62 roll the same
  day was its emergency clause and it FAILED; SB 71's 144-6 roll was its emergency clause and passed.
  Neither is in this batch.
- **HB 594's corporate capital-gains deduction is not in force.** It is contingent on Missouri's top
  individual rate first falling to 4.5 % or less, so no description says the Act exempts corporate
  capital gains. The Act also lets counties and ambulance and fire districts ask *their own voters*
  for higher local sales taxes, which the description states alongside the cuts.
- **HB 595's veterans carve-out is stated.** § 441.043.2 forecloses local source-of-income
  anti-discrimination ordinances, but § 441.043.4 expressly preserves them for recipients of
  veterans' benefits, and § 441.043.3 preserves local authority over publicly owned property,
  voluntary subsidised-rent agreements and CDBG-assisted homes. A ban qualified by the statute is
  described with its qualification (the Texas SB 2972 rule).
- **HB 145's roll-call LR is the pre-amendment draft.** 0310S.04F has no § 610.026 at all; the roll is
  the Truly Agreed vote on the SS SCS *as amended*, and the enrolled 0310S.04T carries the whole
  fee-and-automatic-withdrawal package. Same shape on SB 71, whose 1178H.08C predates HA 1-6.
  When the roll-call header says "A.A.", the stamped LR is not what was voted.

## Official summaries that contradict the enrolled Act

Three measured cases — Missouri's summaries are neutral but not infallible, the Georgia HBRO pattern:

1. **SB 71** — the Truly Agreed summary calls the new § 569.175 offence a **class E felony**; the
   enrolled text says **class A misdemeanor**. The description uses the enrolled text.
2. **HB 594** — the summary names Ozark County, Ste. Genevieve, Perry, Sunrise Beach, Hannibal,
   Moberly, Sikeston, Nevada and Joplin. **None of those place names is in the enrolled text**, which
   uses population brackets. No description quotes them.
3. **HB 495** (dropped, recorded for the next batch) — the $1 million Legal Expense Fund cap comes
   from the *introduced* summary; the enrolled § 105.726 reads two million. Its § 566.210 phrasing
   also implies two different minimum terms where the enrolled text makes one change.

Not used but worth recording: SB 160's Truly Agreed summary says the CROWN Act covers charter
schools; the word "charter" does not appear in the enrolled text.

## Wording checks run before importing

- Every body-tail join is built **with a period**, and the builder asserts the string `", The "`
  appears in no description (the comma-splice defect that hit Illinois batches 01 and 02).
- The plain-language lint's 45-word sentence rule (`candidateRecordPlainLanguageLint`) was run over
  all 20 sentences of all 10 judgments **before** the import: 0 warnings. The importer does not run
  it — the California lesson.

## Roll dates

No Missouri roll in this batch falls on a session-end day, and the 2025 Regular Session adjourned
16 May. Every roll's LegiScan date matches the date printed on its official House roll-call PDF, so
no `official_vote_date` override (migration 257, local only) was needed.

# Washington batch-11

Eleven measures, twenty roll calls, 1,058 records across 108 candidates. Local database
only. Production holds no Washington records. This batch closes the last of the screened
candidates, including the three long-deferred measures (House Bills 1409 and 1903, and
Senate Bill 5232).

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1345 — rural backyard homes | 231 L 26 | housing_affordability | for |
| House Bill 1409 — clean fuels schedule | 319 L 25 | environment_and_public_health | for |
| House Bill 1494 — apartment tax break widened | 164 L 25 | housing_affordability | for |
| House Bill 1903 — low-income energy aid (partial veto) | 252 L 26 | social_programs_and_welfare | for |
| House Bill 2156 — Attorney General financial crime investigators | 242 L 26 | public_safety_and_crime_control | for |
| House Bill 2215 — fuel suppliers in cap-and-invest (partial veto) | 251 L 26 | environment_and_public_health | for |
| Senate Bill 5232 — Housing and Essential Needs (partial veto) | 408 L 25 | social_programs_and_welfare | for |
| Senate Bill 5520 — compensation for the wrongly convicted | 224 L 26 | civil_rights | for |
| Senate Bill 6309 — transit permitting | 166 L 26 | public_infrastructure | for |
| Senate Bill 6346 — tax on income over $1 million | 238 L 26 | personal_income_tax_reduction | **against** |
| Senate Bill 6355 — state transmission authority | 249 L 26 | public_infrastructure | for |

No measure carries an authored nay. House Bill 1345 is Senate-only (House 86-5). House Bill
2215 is House-only (Senate 41-7 and 49-0).

## Partial vetoes, each read before judging

- House Bill 1903: only the advisory group section was vetoed. Stated in the description.
- House Bill 2215: only the emergency clause was vetoed, so it took effect in June rather
  than at once. Stated.
- Senate Bill 5232: the veto restored current law on referral eligibility, including
  citizenship, residency and Social Security numbers. Stated, because without it a reader
  would think eligibility was loosened.

## Checked against later court action

Senate Bill 6346: the Washington Supreme Court refused to order a referendum on it (May
2026), and constitutional lawsuits were pending. The law is in force, so it is judged, and
its description ends with one sentence saying the lawsuits were pending.

## Roll selection

Filter 4 takes each chamber's last kept floor vote. **House Bill 2156** had two House votes
on the same day to accept the Senate's changes, 54-33 and 54-41 on reconsideration. As with
House Bill 2040 in batch-07, the reconsideration vote is judged and the earlier roll is
listed in `acknowledge_later_rolls` with a note.

## Duplicates — read one by one

The tally sweep proposed 36 records. **30 retired**, all on Senate Bill 6346:
- 27 exact matches: the House voted on the bill once (51-46), and the Senate's 27-21 vote
  was its only vote on accepting the House's changes. Records citing the Senate's first
  passage (27-22) were left alone.
- 3 hand-added records by House members (Mark Klicker, David Stuebe, Hunter Abell) that
  name the House passage without a tally. The House voted once, so they can only describe
  that vote. Jim Walsh's record of leading the floor debate is not a vote record and stays.

**Left alone:**
- **All 8 House Bill 2215 records.** Its two House votes are identical on every count: 57-38
  with three excused, on 12 February and 12 March. A record saying "57-38 with three
  excused" cannot be tied to one vote. This is the House Bill 1710 trap from batch-03
  again.
- Stephanie McClintock's House Bill 1903 record, which describes two different votes in one
  record.

The bill-number sweep also found older acts reusing SHB 2156, SHB 1903 and others, and
records calling House Bill 1409 a cap-and-invest bill. That is a separate program, but
those records describe a different vote and were not touched.

## Reconciliation

- Dry run planned 1,058, the real run inserted 1,058, and the database holds 1,058 under
  the run stamp `2026-09-11T05:57:28.828Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run and second dry run: all 1,058 `unchanged`.
- 108 candidates, 0 errors, 0 notified. 30 of 30 retirements confirmed.

## Writing

Median grade 7.3, worst 10.6, longest sentence 32 words. The pipeline's own
plain-language lint reports **0 warnings** over all 40 descriptions.

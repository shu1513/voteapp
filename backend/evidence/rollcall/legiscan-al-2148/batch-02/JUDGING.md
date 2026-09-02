# Alabama batch-02 (one-chamber scope) — judging notes

## Sources

These measures never became law, so there is no enrolled Act. Each was judged from **the text the
chamber actually passed**: the engrossed print at
`https://alison.legislature.state.al.us/files/pdf/SearchableInstruments/2025RS/<BILL>-eng.pdf` for
the four that were amended on the floor, and the introduced print for HB 360 of 2026, which the
House passed without amendment. Introduced-version synopses were used for triage only.

## Gate verification

Every measure was checked three ways before judging: LegiScan status is not enacted, the bill
history holds no Act number, approval or enacted line, and the second chamber never took a floor
vote. Each bill's final recorded action is a committee or calendar step, and both sessions have
adjourned.

## Roll-attribution and date checks

Each roll's printed roll call number was checked against its own bill's history — the check that
`../../legiscan-al-2218/CODE-FINDINGS.md` §1 exists for. All six pass, and all six dates match the
history line exactly. HB 169 of 2026 failed this check (its roll carries HB 593's number) and was
dropped.

## Label reasoning

All stance labels state `nay` explicitly and all are `null`.

- **HB 30 — election_integrity, yes = for.** A mandatory post-election audit of a randomly chosen
  precinct and race, publicly reported, with observers admitted. The 2026 successor HB 95 carries the
  same label in batch-01.
- **HB 7 — immigration, yes = against.** The Alabama Laken Riley Act would have authorised state and
  local police to sign federal immigration-enforcement agreements, arrest on probable cause of
  unlawful presence, and required jails to check status and honour detainers. The area's own words
  ask for a welcoming, humane system, so enforcement-and-detention measures score against it, the
  same direction as SB 53 and SB 63 in batch-01.
- **HB 29 — social_programs_and_welfare, yes = against.** It would have raised the weekly employer
  contact requirement from three to five for claimants in counties of 20,000 or more, required proof
  of each week's search, barred counting the same employer twice unless it was hiring again, and
  ordered random checks of five percent of proofs. Tightening what a claimant must do to stay
  eligible narrows the safety net (the Texas SB 379 direction).
- **HB 234 — public_safety_and_crime_control, yes = for.** Alyssa's Law: wearable panic devices for
  designated school staff that connect straight to the local emergency dispatch point and report
  location to the room and floor. The description states the two real limits — a 2030 deadline and
  the condition that the state set aside money for it.
- **HB 247 — general, no stance.** The 2025 Gulf of America Act, textually the same bill as HB 2 of
  2026 apart from its effective date. No research area fits a renaming, so it is imported without a
  stance rather than dropped, on the same rule PLAN.md states for HB 2.
- **HB 360 (2026) — gun_control, yes = against.** See `../../legiscan-al-2218/batch-02/PLAN.md`.

## Duplicates

A precise sweep — same candidate, same date, same bill — found **6 hand-written duplicates**, three
on HB 30 and three on HB 7, all retired before the import (`duplicate-retirements.json`). The other
related flags were same-day records about different bills. No record was ambiguous and none needed
rewriting in place.

## Import and reconciliation

- 2025 session (5 rolls): dry run 413 planned, real run **413 inserts**, 0 errors, 0 notified, stamp
  `2026-09-02T06:31:22.135Z`, 89 candidates.
- 2026 session (1 roll): **89 inserts**, stamp `2026-09-02T06:31:23.847Z`, 89 candidates.
- Both converge: follow-up dry runs report 413 and 89 `unchanged`.
- Alabama totals after this batch: **1,825 records across 122 candidates and 1,525 tags** — 947 from
  the 2025 session and 878 from the 2026 session, which sums exactly to the four batch imports
  (534 + 413 + 789 + 89).
- Tags reconcile exactly to the side arithmetic: 380 for this batch (yea side only on the five
  stance rolls, both sides on HB 247), plus 431 and 714 from the two batch-01 imports.

## Writing checks

Plain-language lint: 0 warnings over 12 descriptions. Period joins with a `", The "` assertion.
**The British-spelling scan caught one this time** — a first draft of HB 7 wrote "federal centre";
the checker had to be widened past its original word list to see it, and it now also covers labour,
favour, honour, analyse, recognise, authorise, penalise, utilise, enrol, fulfil and travelling. All
four Alabama batches were re-scanned with the wider list and are clean.

# Alabama 2023 batch-02 — judging notes

## Sources

Each measure was judged from the version its chamber voted, fetched through the LegiScan bulk API
and verified against the recorded byte length and MD5 hash: the **engrossed** print for HB 209 and
HB 229, the **introduced** print for HB 392 and SB 181, neither of which was ever engrossed.

None of these measures became law, so no description says it did.

## Roll-attribution check

Vacuous in this session — no roll call numbers appear in any description. It is precisely this
absence that forced HB 208 out of the batch; see `PLAN.md`.

## Date audit

All 4 rolls match the bill history line recording the same action: 4 of 4 exact.

## Supersession

None. Each imported roll is the only kept floor vote on its measure in that chamber.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`.

- **HB 209 — election_integrity, yes = for.** The bill would have made it a Class D felony to
  distribute, collect, prefill, obtain or deliver another person's absentee application or ballot,
  with exceptions for close relatives, long-term housemates, election officials and court-appointed
  guardians. This follows the direction the campaign has already applied to comparable measures —
  Ohio SB 293, Montana HB 719 and SB 105 — where tightening absentee rules is scored as `for`
  election integrity, and the access objection is treated as a different axis. Alabama enacted
  substantially this policy the following year as SB 1, which is labelled the same way.
- **HB 392 — gun_control, yes = for.** It would have made the federal prohibited-person list a state
  crime, a Class C felony.
- **SB 181 — civil_rights, yes = for.** One statewide set of procedural protections for students
  facing suspension or expulsion, replacing 100-odd local codes. "Fair treatment under law" is the
  area's own wording, and the core of the bill is due process.
- **HB 229 — general, no stance.** Resentencing under the habitual felony offender law sits inside
  public safety and crime control, and the direction there is genuinely contested: the area covers
  both public safety and "justice system performance", and a sentencing-leniency measure can be read
  as serving the second or as working against the first. The campaign has no precedent for scoring
  leniency as against public safety — its one `against` in that area is a measure that removed a
  firefighter radio requirement — and inventing one here would be a judgment about penal policy, not
  a reading of the Act. The same reasoning was applied to HB 63 in the 2024 batch.

## Duplicates

The precise sweep found **6 true duplicates**, all retired before the import
(`duplicate-retirements.json`, to re-run at production promotion): hand-written records for Chip
Brown, Margie Wilcox and Shane Stringer on HB 209 and HB 392.

## Import and reconciliation

- Dry run: 4 files, 0 errors, 276 planned inserts.
- Real run (stamp `2026-09-02T16:43:53.257Z`): **276 inserts, 0 errors, 0 notified.**
- Reconciled three ways: report totals (276); run-stamp predicate (276 rows, 110 distinct
  candidates); and the session total, 1,263 records carrying a 2014 run id, matching
  987 + 276.
- Convergence: a follow-up dry run reports all 276 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 8 descriptions, all 2 to 4 sentences, no sentence
over 45 words, British-spelling scan clean. Median Flesch-Kincaid 10.8, worst 12.2.

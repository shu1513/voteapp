# Alabama 2024 batch-02 — judging notes

## Sources

Each measure was judged from the version its chamber voted, fetched through the LegiScan bulk API
and verified against the recorded byte length and MD5 hash: the **engrossed** print where one
exists, the **introduced** print for HB 16, HB 229 and SB 81, which were never engrossed. None of
these measures became law, and no description says otherwise.

## Roll-attribution check

All 29 of the session's divided rolls were checked against their own bill's history; 29 of 29 pass.

## Date audit

All 13 rolls match the bill history line recording the same action: 13 of 13 exact.

## Supersession

Filter 4 does real work in this batch, because the gaming bills were voted repeatedly:

- **HB 151** — House 70-32 on 2024-02-15 and Senate 22-11 on 2024-03-07 are both superseded by the
  conference report votes of 2024-04-30 (House 72-29, Senate 20-15), which are what is imported.
- **HB 152** — House 67-31 on 2024-02-15 is superseded by the conference report vote of 2024-04-30
  (70-29). The Senate never voted on a conference report for HB 152, so its 22-11 vote of
  2024-03-07 is that chamber's final action and is imported.

Every other imported roll is the only kept floor vote on its measure in that chamber.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`.

- **HB 111 — civil_rights, yes = against.** The bill would have written fixed definitions of male and
  female into every Alabama statute, declared that sex is fixed at birth and that gender identity is
  not the same thing, authorised single-sex spaces, and required birth records to state the sex
  observed at birth. It narrows who is recognised under state law, which is the area's subject.
- **HB 130 — civil_rights, yes = against.** It would have extended the limit on classroom discussion
  of sexual orientation and gender identity from fifth grade to eighth, and barred any school
  employee from displaying a related flag or insignia anywhere on school property.
- **HB 195 — womens_reproductive_rights, yes = against.** The bill's own text would have barred sex
  education from promoting abortion and from demonstrating contraceptives, alongside a sexual risk
  avoidance mandate. Those two prohibitions are the reason for the label, not the abstinence framing.
- **HB 356 — womens_reproductive_rights, yes = against.** The statutory definition of a qualifying
  charity is what settles it: to earn the tax credit an organisation's stated purpose must include
  helping women carry a pregnancy to term, encouraging parenting or adoption, and **preventing
  abortion**. The Act subsidises that purpose with public money. This is the same measure that
  appeared in 2023 as HB 208, which was dropped from that batch on roll-attribution evidence.
- **HB 36 — gun_control, yes = for.** A Class C felony for possessing a pistol fitted with a
  machine gun conversion device, with exceptions for police and for parts on the federal register.
  Alabama enacted the same policy the following year as SB 116, which the 2025 batch labels the same
  way.
- **HB 363 — public_safety_and_crime_control, yes = for.** Deaths caused by drunk driving would have
  become murder or manslaughter in defined circumstances.

## The five no-stance imports

- **HB 151 and HB 152 (the gaming package).** The largest fight of the session and squarely divided,
  but no research area covers gambling, a lottery or sports betting. High salience with no area is
  exactly the case the no-stance rule exists for.
- **HB 385 (material harmful to minors).** Would have declared premises distributing material harmful
  to minors a public nuisance, widened the definition of sexual conduct, and let county and city
  attorneys bring the action. It touches expression rather than equal treatment, and `civil_rights`
  as this taxonomy defines it — equal rights, anti-discrimination enforcement, fair treatment under
  law — does not fit. Nothing else does either.
- **HB 63 (split sentences).** Would have raised the ceiling for a split sentence from 20 years to
  30, with at least 10 years served on the longest tier. Contested direction inside public safety,
  handled the same way as HB 229 in the 2023 batch.
- **SB 10 (library boards).** Would have made county and city library board members removable at will
  by the officials who appoint them, and required each board to report yearly to the Governor and
  legislative leaders on items reviewed or removed from its shelves. Salient — this was Alabama's
  library-content fight — but the axis is political control of collections, and no area in this
  taxonomy carries it.

## Duplicates

The precise sweep found **6 true duplicates**, all retired before the import
(`duplicate-retirements.json`, to re-run at production promotion): hand-written records on HB 130,
HB 36 and SB 186 for Chip Brown, Chris Pringle, Margie Wilcox, Mark Shirey, Marilyn Lands and Shane
Stringer.

Two records naming these bills were left alone because they are not vote records: Barbara Drummond's
authorship of a successful amendment to HB 130, and Chris Blackshear's sponsorship of HB 151.

## Import and reconciliation

- Dry run: 13 files, 0 errors, 930 planned inserts.
- Real run (stamp `2026-09-02T16:45:01.675Z`): **930 inserts, 0 errors, 0 notified.**
- Reconciled three ways: report totals (930); run-stamp predicate (930 rows, 117 distinct
  candidates); and the session total, 1,519 records carrying a 2103 run id, matching 589 + 930.
- Convergence: a follow-up dry run reports all 930 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings over 26 descriptions, all 4 sentences, no sentence
over 45 words, British-spelling scan clean. Median Flesch-Kincaid 10.8, worst 12.6.

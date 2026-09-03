# Alabama 2021 Regular Session batch-01 — judging notes

## Sources

Every measure was judged from the version its chamber actually voted, fetched through the LegiScan bulk
API (`getBillText`, and `getAmendment` for conference substitutes) and verified against the byte length
and MD5 hash the dataset records for that document. The state website was not used: it had been timing
out for hours at a stretch during the previous Alabama batch, and one direct download in that batch
returned HTTP 200 with a truncated, unreadable file.

Alabama prints struck and inserted text together and the conversion flattens both into one run of words.
The convention is struck text first, inserted text second, so `no less than 12 10 months` means the old
law said 12 and the new law says 10. Every changed number in these descriptions was read that way.

## Roll-attribution and date audit

Each imported roll's printed roll call number was checked against its own bill's history, and each
roll's date was checked against the bill history line recording the same action. Results for this
session are in `../survey/divided-worklist.tsv`. The term-level findings, including a case where one
session's dataset carried another session's roll calls, are in
`../../legiscan-al-1756/CODE-FINDINGS.md`.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`: in each measure the realistic
reason for a no vote runs on a different axis than the scored area.

- **HB 385 — civil_rights, yes = for.** The Act deletes the statutory requirement that sex education
  teach that homosexuality is not acceptable to the general public and that homosexual conduct is a crime.
  Removing a mandate to teach that a group's conduct is criminal is squarely the area's subject.
- **HB 391 — civil_rights, yes = against.** School sports limited by the sex on the original birth
  certificate. The two halves are not parallel and the description says so: a girl may join a boys' team
  where no girls' team exists, a boy may never join a girls' team.
- **HB 273 — environment_and_public_health, yes = for.** Tobacco and vaping age raised to 21, with
  advertising limits and a registry of vape-liquid makers.
- **HB 116, HB 285, HB 388 — election_integrity, yes = for.** A one-off post-election audit; a ban on
  curbside voting; and a constitutional amendment requiring six months' notice before new election rules
  bite. The curbside ban restricts access, and it is scored the same way as every comparable tightening
  measure already in this corpus, from Ohio SB 293 to Montana SB 105.
- **HB 154 — anti_corruption, yes = for.** All campaign finance reports filed electronically into a
  public searchable database, with the $5,000 paper exemption removed.
- **HB 631 — environment_and_public_health, yes = for.** A $5 boat fee funding invasive-species control
  and debris removal in reservoirs.
- **Six measures carry no stance.** HB 103 (staying open in a pandemic emergency) grants a right and then
  conditions it on obeying the same officials' safety rules. HB 123 (voter-list privacy for officials)
  trades public transparency for personal safety. HB 246 (yoga) permits something previously banned and
  then constrains it down to the language used for pose names. HB 404 (athlete pay) has no area. SB 107
  (police jurisdiction), SB 117 (expungement), SB 308 (concealed carry permits) and SB 323 (education
  time credit) each tighten and loosen inside the same area.

## Duplicates

A precise sweep found the hand-written records that describe the same votes, and they were retired
before the import. The sweep is restricted to Alabama candidates, an exact vote date, a description
naming the same bill, and a description worded as a vote. It excludes only records whose origin run id
begins `rollcall:`, because hand-written records carry a `manual:candidate-records:...` run id and a
null-check misses them. Sponsorship records naming the same bill were left alone.

## Import and reconciliation

- Real run (stamp `2026-09-03T16:54:56.724Z`): **747 inserts, 0 errors, 0 notified**, across 18 rolls.
- Reconciled three ways: the report totals; the run-stamp predicate (747 rows, 76 distinct
  candidates); and the Alabama roll-call total, which moved from 4,890 to 7,527 across the six batches
  imported together.
- Convergence: a follow-up dry run reports all 747 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings. Every description is 2 to 4 sentences with no sentence
over 45 words, and a British-spelling scan is clean — it caught real slips on a first pass, including
`legalised`, `licence`, `behaviour`, `labour` and `programme`, all corrected. Reading grade was measured
per session; medians run 8.6 to 11.2.

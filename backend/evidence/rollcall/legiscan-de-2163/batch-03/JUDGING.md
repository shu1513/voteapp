# Delaware batch-03 — how each measure was judged

Every description was written from the **enacted text** — the engrossed print where the
bill was amended, the last draft where it was not — read top to bottom. No description
rests on a title, a synopsis, or a summary. No AI provider was called at any point.

## Enactment was verified against the state, not the feed

This batch exists because the dataset's status field is wrong, so the enactment fact was
checked directly. For each of the 20 parked bills the Delaware action log endpoint
`legis.delaware.gov/json/BillDetail/GetRecentReportsByLegislationId` was read with the
`LegislationId` carried in the dataset's own `state_link`. A bill counted as enacted only
if its log carries an action beginning `Signed by Governor`. Nine did.

**A caution worth repeating: the first pass at this used a regex containing `signed`, and
`Assigned to Judiciary Committee` matched it.** Every one of the 20 bills looked signed.
The fix was to anchor the match to the start of the action text.

## The substitute trap, which Delaware sets on most of this batch

Six of the nine measures did not pass in the form they were introduced. HB 150, HB 233,
HB 368 and SB 300 passed as substitutes; HB 94 passed as its **second** substitute; and
HB 310 and HB 380 were amended on the floor. The operative text is the engrossed print
in every one of those cases, and that is what was read. HB 369 and HB 418 have a single
text and no amendments, so their draft is the enacted text.

## The stale synopsis bit again, and it changed a number

**HB 380's synopsis says the law's coverage threshold becomes 15,000 consumers. The
enacted text says 10,000.** The synopsis is written into the introduced bill and is never
updated, the hazard already recorded as CODE-FINDINGS §2 for HB 210 and SB 60. A
description written from the synopsis would have printed a threshold no Delawarean is
subject to. The description uses 10,000, with the companion drop from 10,000 to 5,000
for firms that make more than a fifth of their revenue selling data — also read from the
text, not the synopsis.

## Checks run before importing

- **All 17 rolls verified as their chamber's last kept floor vote**, and each matched to
  its bill-history line on date, yeas and nays. All 17 exact. No judgment needed
  `acknowledge_later_rolls`; the superseded-stage gate fired on nothing because the three
  superseded rolls were dispositioned out of the batch before judging.
- **`candidateRecordPlainLanguageLint`: 0 warnings** over all 34 descriptions.
- **Reading level measured separately.** First pass came in at Flesch-Kincaid median 8.3,
  worst 9.6, with three measures above grade 9. HB 369, HB 310 and HB 368 were rewritten
  into shorter sentences. Final: **median 7.4, worst 8.6, nothing above grade 9, longest
  sentence 22 words.** This is the most readable Delaware batch so far.
- **Scope kept across the rewrite.** The batch-01 lesson was that shortening drops
  qualifiers. Each rewritten description was re-read against the act: HB 368 keeps all
  four detainer exceptions, HB 310 keeps "in or near Delaware" on the clean-power
  condition, and HB 369 keeps the cooperation duty limited to agencies with a role in
  the work.
- **British spellings**: explicit word list, none found.
- **Comma splice**: body and tally sentence joined with a period, `", The "` asserted
  absent everywhere.
- **Row counts reconciled three ways**: the report's 243 inserts, the 243 per-roll
  candidate insert actions inside it, and 243 live `candidate_records` rows for the 17
  rolls. The re-run reported 243 unchanged.

## A process error worth recording

After the real import I renamed `import-report.json` to `import-rerun-report.json` by
hand, to follow the campaign rule about preserving the ledger before a re-run. That was
wrong twice over: the importer already writes `import-rerun-report.json` when a ledger
exists, so the rule is enforced in code, and the manual rename overwrote the genuine
re-run report with the first run's. It was repaired by restoring the name and running the
import again; the three reports in this directory are now the dry run, the first run, and
a real convergence re-run. **Do not rename these files by hand.**

## Labels

| Measure | Areas | Direction |
|---|---|---|
| HB 150 | civil_rights | yes = for |
| HB 233 | corporate_accountability, environment_and_public_health | yes = for |
| HB 310 | corporate_accountability, environment_and_public_health | yes = for |
| HB 368 | immigration, civil_rights | yes = for |
| HB 369 | gun_control | yes = for |
| HB 380 | data_privacy, corporate_accountability | yes = for |
| HB 418 | gun_control | yes = for |
| HB 94  | immigration, civil_rights | yes = for |
| SB 300 | gun_control | yes = for |

`data_privacy` is the first use of that area in Delaware.

**HB 150 carries civil_rights alone, on purpose.** The act is aimed at immigration
arrests at courthouses, and that is why it was filed, but its text never mentions
immigration — it covers any civil arrest not ordered by a judge. Labelling it
`immigration` would claim something the enacted words do not say.

**HB 418 is labelled `gun_control` "for" even though it eases the ghost-gun ban.** It
creates a six-month path to comply rather than a repeal, keeps the prohibition intact,
and routes every re-serialized firearm through a background check. A yes vote kept and
shored up the ban.

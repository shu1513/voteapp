# TN batch-01 — judging notes

All 14 judgments were written from the **enrolled public chapter** plus the Fiscal
Review Committee's `SUMMARY OF BILL` for the version each chamber actually voted.
No AI provider call was made at any point.

## Version check — how each roll was pinned

The bill page's history names every amendment as adopted, tabled or withdrawn, and each
amendment PDF prints a six-digit **filing number** at its foot. The `FM####.pdf` fiscal
memos are headed `SUMMARY OF BILL AS AMENDED (<filing numbers>)`, so matching those
numbers proves which memo describes the text a chamber voted:

| measure | amendment adopted | filing | memo |
| --- | --- | --- | --- |
| HB 1093 | HA0122 (House) | 005877 | FM0950 |
| SB 670 | SA0403 (Senate) | 006502 | FM1093 |
| SB 2348 | SA0811 (Senate) | 015775 | FM2665 |
| SB 1915 | HA0929 (House) | 016345 | FM3105 |
| SB 1713 | HA1028 (House) | 015709 | FM2640 |
| SB 1360 | SA0383 (Senate) | 004178 | FM0225 |
| SB 1084 | SA0330 (Senate) | 006418 | — (judged from the enrolled act) |

**SB 2040 is the one case where no memo matches.** The Senate adopted SA0841 (014996),
SA0842 (015297) and SA1052 (018137); the latest memo, FM3234, covers 014996, 015297 and
**017444** — a different amendment. The description is therefore written from the
enrolled act (PC 1111) alone, which the House later passed unchanged. That is also why
the description says the ownership bar starts **July 1, 2028**: the enrolled text says
July, the superseded memo said January.

**SB 336 is the split — the two chambers voted different texts.**

- The Senate voted on 2025-04-10 (24-8) on SA0302 as modified by SA0381: amendments to
  **§ 40-29-202**, the certificate-of-restoration route.
- The House voted on 2026-03-09 (64-24) on HA0602, a delete-all substitute amending
  **§ 40-29-102** instead. (LegiScan dates the roll 03-09; the official history prints
  the passage line under 03/10 — a one-day journal offset. The imported `vote_date`
  follows the roll.) That is what became law (PC 605), and the Senate concurred 31-1
  on March 13 — a lopsided vote that is not in this batch.

The Senate's description says it voted the Senate's version and names what later
changed; the House's description describes the enacted text. Both carry
`civil_rights / for`.

## Stance directions follow the AREA DESCRIPTION, not the bill's framing

- `immigration` = "Welcome immigration through a lawful, orderly, and humane system" →
  enforcement measures are **against** (HB 749, SB 1915). Texas SB 8 precedent.
- `gun_control` = "Regulate firearm access … to reduce gun violence" → SB 1360, which
  widens industry immunity and tightens preemption, is **against**; HB 1093, which
  expands the machine-gun definition to conversion devices and raises penalties, is
  **for**.
- `civil_rights` = "Protect equal rights, anti-discrimination enforcement, and fair
  treatment under law" → the DEI bans (SB 1084, SB 1713) are **against**, and restoring
  voting rights after a sentence (SB 336) is **for**. Ohio SB 1 precedent.
- `environment_and_public_health` → SB 670 (wetland deregulation) and HB 2070 (barring
  emissions liability) are both **against**.
- `election_integrity` = "Ensure elections are secure, accurate, auditable, and trusted"
  → HB 2185 is **for**: it only permits the existing verification portal to read federal
  SAVE data when that data is offered through a secure web service.

## Qualifications carried into the descriptions

Following the Texas SB 2972 lesson — when a statute qualifies a ban, the description
must carry the qualification:

- **SB 1084** names the comptroller's federal-funding exemption and the state-level
  carve-outs for demographic-based public-health outreach and neutral equal-access
  outreach. Only the state-level definition has those carve-outs; the county and
  municipal definitions do not.
- **SB 670** names the three pollution conditions attached to the no-permit tier and
  the 1:1 / 2:1 mitigation caps rather than saying "wetlands deregulated".
- **SB 2040** names the hospital, mail-order and orphan-drug exceptions and the
  wind-down through 2028-12-31.
- **SB 2348** names the "or designate a teacher as library information coordinator"
  alternative for schools under 750 students — which the fiscal note's summary omits and
  only the enrolled act states.
- **SB 1915** names that a benefit may not be delayed while verification is pending.

One claim rests on the fiscal note rather than on text I could derive myself: **HB 1093**
raising machine-gun offences from a Class E to a Class C felony. The enrolled act does it
through cross-reference edits inside § 39-17-1302; the Fiscal Review Committee's memo
(FM0950, matching the enacted filing number) states the reclassification directly.

**HB 1093's drive-by consequence is two provisions, and the description names both**
(review fix — the first pass named only the first): § 40-35-303 as amended makes a
defendant convicted of aggravated assault with a firearm fired from inside a motor
vehicle ineligible for **probation** (Section 8), and the new § 40-35-501(gg) removes
**release eligibility** for the same offense — 100% of the sentence served, credits
usable only for privileges and classification (Sections 9-10). The bill caption words
the second provision as "ineligible for parole". The 14 records were rewritten in
place after import.

## Import

Dry run: 14 files, 0 errors, **199 planned inserts**, 0 notified.
Real run 2026-08-27 on local `voteapp`: 14 `imported`, 0 errors, **199 inserts**, 0
notified, **31 distinct candidates** (every mapped crosswalk entry).

Reconciled three ways:

- initial-run report `actions` = `{"insert": 199}` (see the report-provenance note below)
- `candidate_records` 67,973 → 68,172 = +199
- grouped `origin_run_id` stamps (a rewrite re-stamps the rewriting run's `startedAt`,
  the batch-02 mechanic): **185 records @ `2026-08-27T01:49:40.074Z`** (the initial
  import) **+ 14 @ `2026-08-27T20:44:47.015Z`** (the HB 1093 description rewrite)
  = 199 records / 31 candidates. A single-stamp predicate no longer covers the batch.

The dry run's own stamp (`2026-08-27T01:48:33.021Z`) matches **zero** rows, which is
positive proof `--dry-run` is inert. Production was never touched.

**Report provenance:** `import-report.json` uses a fixed filename, so the idempotency
re-run **overwrote** the initial run's report (`insert: 199`) with its own
(`unchanged: 199`) before the trimmed copy was committed. The committed files are
therefore: `import-dry-run-report.json` (the pre-import plan, `insert: 199`),
`import-rerun-report.json` (the idempotency re-run, `unchanged: 199` — previously
mislabeled `import-report.json`), and `import-rewrite-report.json` (the HB 1093
description fix, `rewrite: 14, unchanged: 185`). The initial run's full report was
not preserved; its `insert: 199` result is evidenced by the dry-run plan it matched
exactly and by the DB stamp census above.

## Related-record flags reviewed

Three rolls were flagged `related: 1`, all on Senator **Bo Watson** and all Vote Smart
rows about *other* bills — SB 836 (tuition for students unlawfully present), SB 1084 as
paired with HB 622, and SB 714 (state oversight of underperforming districts). None
cites these roll calls and none duplicates a record this batch writes, so nothing was
retired. Note for later: Watson's Vote Smart row does describe the **Senate's** SB 1084
vote, which is not divided and so is not in this batch; a future batch that imported it
would be a genuine duplicate.

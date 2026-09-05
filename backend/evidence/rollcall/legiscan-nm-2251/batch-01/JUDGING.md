# New Mexico 2026 session, batch-01 — judging

## Sources

- **The enrolled act is ground truth.** `nmlegis.gov/Sessions/26%20Regular/final/<PADDED>.pdf`.
- **The Legislative Finance Committee's fiscal impact report is an index only.** Nonpartisan, with
  no sponsor statement of intent, but only its "Synopsis of ..." paragraph is neutral.
- **The official roll call sheet fixes the tally, the date and the version voted.**
  `/Sessions/26%20Regular/votes/<PADDED>HVOTE.pdf`.

## The direction on House Bill 156 is the opposite of what its title suggests

The bill is titled "Repeal Special Session Vaccination Laws" and it would be easy to file as a
rollback of vaccine policy. **The enrolled act is the reverse.** Sections 8 through 13 of the October
2025 special session act were a delayed reversion: on July 1, 2026 they would have repealed that
act's own changes and restored the old law tying New Mexico's school immunization rules to a federal
advisory committee. House Bill 156 repeals that reversion, so the state Department of Health keeps
the authority the special session gave it. The vote confirms the reading — 51-14 in the House and
38-1 in the Senate, which is not the shape of a contested rollback. The direction is `for` on
environment and public health.

**This is why the enrolled act is ground truth and a title is not evidence.**

## Tally audit

All 19 rolls considered for this batch and the special session were checked against New Mexico's
official roll call sheets. **Eighteen are exact** on date, yea, nay, and not-voting versus absent.

**Senate Bill 151 is the one failure and is held, not imported.** LegiScan stores 42-19 with 9
absent; RCS# 171 reads 43-19 with 6 excused and 2 absent; LegiScan's own bill history says 43-19.
See `../CODE-FINDINGS.md`.

## Version check

The feed carries final passage only and has no concurrence rolls, so each bill's history settles
which text the House vote endorsed.

- **Fourteen need nothing.** Nine Senate bills reached the House last and were signed with no
  agreement step. Senate Bills 241 and 151 were amended by the House and the Senate then concurred,
  so the House vote is on the enacted text. Four House bills — 9, 124, 156 and 270 — passed the
  Senate unamended.
- **Two are House bills the Senate amended and the House then agreed to,** so the House's only
  recorded vote predates the law. The amendments-in-context prints for this session are scans with
  no extractable text, so the guillemet trick from the 2025 session does not work; the Senate
  committee amendments were read directly instead, from `bills/house/<PADDED>FC1.pdf`.
  - **House Bill 4.** The Senate Finance Committee carved five percent of the health insurance
    premium surtax to the behavioral health program fund from September 2028, leaving the health
    care affordability fund 95 percent where the House had voted 100. The measure's core, moving the
    surtax away from the state's main account and into health care, is unchanged. The record first
    stated the enacted split; review pointed out that this credits or blames representatives for a
    provision absent from their recorded vote, so it now describes the House version (100 percent
    from September 2028) and then the Senate's change in its own sentence.
  - **House Bill 247.** The Senate Finance Committee struck the House committee amendment and added
    account freezing plus an annual report to the Legislature. That sharpens the same direction. The
    record describes only the reauthorization limits, the infrastructure-plan requirement and the
    reversions, which both versions share.

## Writing the record text

- Plain English, one paragraph, four or five sentences, no sentence over 45 words.
- Every description says what the measure did and what a yes or a no vote meant, and closes with the
  House tally and the fact that it became law.
- Reading level was measured: **median Flesch-Kincaid grade 8.3, worst 9.0**, mean sentence 16.9
  words, longest 33. Ten of the 16 were rewritten plainer after a first pass came back at median 9.1
  with a worst of 12.0.
- `candidateRecordPlainLanguageLint.listPlainLanguageWarnings` was run over all 36 descriptions in
  this batch and the special session's before importing: **0 warnings**.
- A generator assertion blocks comma splices, any sentence over 45 words, and British spellings.

## Import reconciliation

| Check | Records |
|---|---|
| Importer dry run | 938 |
| Independent recount from the evidence files against the crosswalk, counting only Yea and Nay | 938 |
| Rows in the local database under this run stamp | 938 |

Area tags were predicted before checking: **711 expected, 711 written**.

- Run stamp: `rollcall:NM:house:2251:<roll>:2026-09-05T03:26:00.560Z`, one stamp for the batch.
- 16 files, 16 imported, 0 errors, 63 candidates, 0 notifications.

**Review rewrite** (`import-rerun-report.json`): two descriptions changed after PR review and were
re-judged and re-imported. House Bill 4 now describes the text the House voted on, not the enacted
split (see Version check). Senate Bill 264 keeps Section 3(A)(1)'s qualification: the armed people it
bars from polling places are those "in the civil, military or naval service of the United States",
not any armed person. Import: 122 `rewrite` (60 + 62), 816 `unchanged`, 0 inserts, 0 errors.

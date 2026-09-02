# GA batch-01 — judging notes

## Source

Every judgment was written from Georgia's own documents, no AI call:

1. **House Budget & Research Office (HBRO) Session Report** — the official, nonpartisan
   section-by-section "Final Bill Summary" for each enacted bill, the Georgia analog of Ohio's
   LSC Final Analysis. 2025 and 2026 editions, both "with Vetoes":
   `https://www.legis.ga.gov/api/document/docs/default-source/house-budget-and-research-office-document-library/session-reports/<year>_end_of_session_report_with_vetoes.pdf`
   → `pdftotext -layout`.
2. **The bill text versions themselves** — LegiScan's bill record lists every dated version
   (Introduced / Comm Sub / Engrossed / Enrolled) as a legis.ga.gov PDF, plus an `amendments[]`
   list with an `adopted` flag. Matching a chamber's vote date against that list is Georgia's
   equivalent of the Texas TLO Amendments page, and it is how every roll here was
   version-checked.

**Georgia does not publish a sponsor's statement of intent**, so the Texas hazard — an advocacy
preamble whose numbers contradict the statute — does not recur. HBRO writes only what the bill
does.

## Version check, roll by roll

Every one of the 18 rolls was checked by diffing the text in force on the vote date against the
enrolled Act (normalized for line numbers and page furniture):

- **Identical to the enacted text:** HB 111 (both chambers voted the introduced text), SB 185
  (Senate engrossed), SB 144 (both), SB 443 (both), SB 68 House, SB 1 House, SB 212 House,
  SB 472 House, SB 552 House (the enrolled Act only drops a separate effective-date section).
- **Concurrence taken instead of passage**, because the chamber's own passage vote was on a
  different text or was not divided: SB 68 Senate, SB 1 Senate, SB 472 Senate, HB 1247 both.
- **One genuine version split: SB 212 Senate.** The Senate passed its engrossed version, whose
  attestation required the person to state they were "not acting directly or indirectly on
  behalf of" a campaign; the House substitute restructured that attestation and added an
  exception for people on school property to attend an event open to the general public. The
  Senate roll's description says what the Senate passed; the House roll's says what became law.

## The Georgia vehicle-bill trap (SB 33)

`SB 33` is titled "Georgia Hemp Farming Act; total THC concentration of consumable hemp
products; provide limits" in LegiScan, in the bill caption, and in the HBRO report's own
heading — but the HBRO **summary under that heading is about the Local Homestead Option Sales
Tax**, homestead exemptions, and county tax digests. The hemp language was replaced; the
divided votes on it (Senate 32-21 and House 97-72, both 2026-04-02/03 concurrences) are votes
on a property-tax bill. Judging it from its title would have written a false sentence onto
~190 candidates. It was dropped from this batch. Ohio's H.B. 472 taught the same lesson: read
the analysis, never the title.

**Pre-existing defect this surfaced:** a hand-written record already in the database
(`abfc6d8f-011a-47e5-9a98-4a32ef4d81cd`, Jesse Petrea, 2026-04-02) says "Voted yes on SB 33,
the Georgia Hemp Farming Act, which set total-THC limits for consumable hemp products." That
record is wrong for the same reason and is not touched by this import.

## Stance directions

Direction follows the **research area's description**, never the bill's framing:

- `corporate_accountability` = "Hold companies accountable for legal compliance, consumer
  protection, and public impact" → SB 68 (limits tort liability and damages) and SB 144
  (makes an EPA-approved label a sufficient warning) are **against**.
- `civil_rights` = "Protect equal rights, anti-discrimination enforcement, and fair treatment
  under law" → SB 1 and SB 185 are **against** (Ohio SB 1 DEI-ban precedent); SB 552 is **for**,
  because its enacted text grants student political groups the same access other noncurricular
  groups already have.
- `public_education_quality` names accountability explicitly, so SB 472's audit and
  intervention machinery is **for**.
- `government_efficiency` → HB 1247, following the Texas SB 14 precedent (regulatory-efficiency
  office plus the end of judicial deference to agency interpretations) → **for**.
- `personal_income_tax_reduction` is literally the income tax → HB 111 **for**.
- `data_privacy` → SB 212 **for**.
- `public_safety_and_crime_control` → SB 443 **for**. Recorded counter-reading: critics read a
  higher penalty for obstructing a highway as aimed at protest. The enacted text is a penalty
  grade and a civil-liability clause with no protest-specific language, so the direction is
  taken on the text.

Descriptions end **"and became law"**, not "was signed into law" — LegiScan status 4 records
enactment, not whether the governor signed.

## Import result

Real run on local `voteapp` 2026-08-26: **18 files all `imported`, 0 errors, 1,725 inserts, 0
notified, 207 distinct candidates**. Reconciled exactly to the dry run (`candidate_records`
64,480 → 66,205; `origin='rollcall_import'` 20,330 → 22,055). The dry run's own stamp matches
**zero** rows, which is the positive proof `--dry-run` is inert. A re-run dry run reports all
1,725 `unchanged`.

Batch predicate (one `startedAt` per run, shared by all 18 rolls — the only batch key):

```sql
SELECT count(*) FROM candidate_records
WHERE origin_run_id LIKE 'rollcall:GA:%:2026-08-27T01:03:53.893Z';
```

**207, not 208 crosswalk-mapped candidates**: Speaker Jon Burns casts no recorded vote — the
Texas Speaker Burrows pattern, not a fan-out gap.

## Duplicates retired by hand

The dry run flagged 12 related existing records. Ten name different measures voted the same day
and were left alone. Two were true duplicates of votes this batch imported, and were retired
with reasons naming the replacing record:

- `bd4d280c…` Jason Ridley, "Voted yea on Georgia Senate Bill 68, which changed personal-injury
  lawsuit rules" → replaced by `9bf3108e…` (house roll 1523850).
- `93f3731f…` Jason Ridley, SB 1 House passage → replaced by `bf4f6bac…` (house roll 1531583).

PROD UNTOUCHED — promotion is a separate step.

## Plain-language rewrite (2026-08-30)

All 18 yea and nay descriptions were rewritten from committed evidence. Judge
dry and real runs passed. The importer rewrote 1,725 local records with stamp
`2026-08-31T06:58:26.015Z`; convergence reported all 1,725 unchanged. The
original `import-report.json` remains unchanged.
Prod remains untouched.

Current-judge later-roll acknowledgments: 1508337→1535498,
1524305→1524304, and 1531585→1531584.

A final jargon-definition pass rewrote 188 SB 212 records at
`2026-08-31T07:28:27.200Z`; the next dry run reported all 1,725 unchanged.

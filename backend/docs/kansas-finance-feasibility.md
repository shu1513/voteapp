# Kansas campaign finance — feasibility probe (2026-08-26)

Verdict: **BUILDABLE.** All four targets available: totals raised, totals spent, donor occupations (individuals > $150), and outside spending with explicit SUPPORT/OPPOSE stance.

## Who holds the data

- **Kansas Public Disclosure Commission (KPDC)** (renamed from Governmental Ethics Commission), kpdc.kansas.gov — regulator; publishes the complete report archive as PDFs under `kansas.gov/ethics/CFAScanned/<Office>/<Cycle>ElecCycle/...` (statewide, Board of Ed, Senate, House, DAs, PACs, party committees, ballot questions, independent expenditures). Index pages are plain HTML link trees, e.g. `.../House/2026ElecCycle/HLinks2026EC.htm`.
- **Secretary of State CFR viewer** (`sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx`) — the live e-filing database. ASP.NET WebForms postback app, no REST API, but fully curl-scriptable (see Mechanics). Categories: Contribution, Expenditure, Candidate Campaign Filings, PAC/Party, Gubernatorial Inauguration, Individual Entity (= independent expenditures).

## The four targets

1. **Total raised / total spent** — report cover SUMMARY lines, per period:
   1 cash begin, 2 total contributions+receipts (Sch A), 3 = 1+2, 4 total expenditures (Sch C), 5 = 3−4, 6 in-kind (Sch B), 7 other (Sch D).
   Arithmetic self-checks (1+2=3, 3−4=5) → Delaware-style cover reconciliation. Verified cent-exact on Holscher (Governor: raised $412,630.21, spent $527,588.29 for 1/1–7/23/26) and Helwig (State Rep D1, e-filed, native HTML).
2. **Donor occupations** — Schedule A has "Occupation of Individual Giving More Than $150". Statewide Contribution search exposes an Occupation column; Holscher sample: 2,824/4,285 rows non-blank (blanks = PACs/entities/small donors). Free-text, typos present ("Ownere"). An `Industry` field exists in the export but is empty — ours to classify.
3. **Outside spending with stance** — "Statement of Independent Expenditures for a Person Other Than a Candidate..." — line items: date, vendor+address, amount, and `<district> <CANDIDATE> SUPPORT|OPPOSED` (e.g. "HD101 CASEY SLAUGHTER SUPPORT ... $61,259.00 MEDIA PLACEMENT"). **Explicit per-line stance — better than Montana.** Filed over-$500-aggregate; 4 reporting deadlines/cycle. Archive: `CFAScanned/Others/<cycle>ElecCycle/` (27 PDFs so far for 2026); viewer Individual Entity search shows 316 filings total incl. Aug-2026 not yet in the PDF tree. All scanned PDFs with OCR text layer (noisy: "$58.741.00"). Volume small → transcription with checksum care is fine.
4. **PAC direct money** — appears on candidate Schedule A as entity contributions (Kansas Dental PAC etc.); no extra pipeline needed for direct-side.

## Mechanics (proven end-to-end via curl)

- Default curl UA → 403. Browser UA works. Cookie jar + `__VIEWSTATE`/`__EVENTVALIDATION`/`__VIEWSTATEGENERATOR` round-tripping required (HTML-unescape the values).
- Contribution flow: GET entry → POST `ddlViewerOptions=Contribution&btnSubmit=Submit` → lands on `cfr_examiner_contribution.aspx` → POST search fields (`txtCandidateName`, `txtStartDate` MM/DD/YYYY, ...) → results page → POST `btnExport=...` → **`Contributions.xls` = HTML table of the FULL result set** (4,285 rows / 6.9 MB verified, no pagination cap). Row fields: candidate, contributor, address/city/state/zip, occupation, industry(empty), date, tender type, amount, in-kind amount+desc, period. Expenditure category has the same shape (not yet exercised).
- Contribution search covers **statewide candidates only** (mandatory e-file since 2010-01-10). Itemized export ≠ cover totals (unitemized ≤$50 and other receipts missing): Holscher itemized $389,246.47 + in-kind $4,803.67 vs cover $412,630.21 → **totals must come from covers.**
- Candidate Campaign Filings search: filter by office (incl. State Representative/Senator), filing type "Receipts and Expenditures Report", filed-date range → enumerates all filings (354 State Rep R&E filed 7/1–8/26/2026). E-filed rows open native HTML: `reports/exp_report_main.aspx` cover + postbacks `lnkbtnSchedule{A,B,C,D}View` → full itemized schedules incl. occupation and unitemized-total lines. Paper rows = PDF icon → scanned PDF. Report identity lives in server session (no report id in URL) → scraper must walk postbacks per report.
- Legislative e-file rate sample: 9/12 House 2026-07 reports carry "Electronically filed on" (the KPDC PDFs are print-then-scan of the viewer, so even e-filed KPDC PDFs are OCR-noisy — prefer viewer HTML; use KPDC PDFs only for paper filers).

## Gotchas

- K.S.A. 25-4154(d): contributor names may not be used for **commercial** purposes (class A misdemeanor). Voter-information use is non-commercial; keep donor names out of any monetized surface.
- KPDC file-name convention: `<officecode><initials>_<period>.pdf`, e.g. `SW01CH_202607`, `H001DH_202601`; `AT`=appointment of treasurer, `PLF`=last-minute (July), `Aff`=exemption affidavit. Codes are per-district+initials, not stable IDs.
- Statewide 2026 cycle covers 1/1/23–12/31/26 (4-year); House cycles 2-year; Senate 4-year (next 2028 — no regular Senate races Nov 2026).
- Amendments exist both as `amend` PDFs and Amendment Date column in the viewer; take latest.
- `ethics.ks.gov` dead; `ethics.kansas.gov` redirects to kpdc.kansas.gov; PDF links 302 from `kansas.gov` → `www.kansas.gov` (curl needs `-L`).

## Suggested v1 shape

Totals from covers (viewer HTML for e-filers; OCR+reconciliation for the paper minority), occupations from statewide Contribution export + Schedule A HTML for legislators, outside stance from IE statements (small volume, transcribe). Nov-2026 relevant offices: Governor + all statewide, State House, Board of Ed odd districts, DAs (partial).

## Addendum 2026-08-26: verification of external Aug-2 feasibility report

Cross-checked a second feasibility report against primary sources. Results:

**Confirmed (adopted into this doc):**
- K.S.A. 25-4148a: occupation required for individual > $150; **if contributor not employed for compensation, the SPOUSE's occupation is filed** — occupation value may belong to the spouse. Label accordingly. Occupation sometimes filed voluntarily below $150 (Helwig: "Retired" on a $100 cash row).
- K.S.A. 25-4150 (rewritten by HB 2206, 2025): IE statement threshold = **$1,000 aggregate/calendar year per filer**; vendors itemized only over **$500 aggregate**. PAC/party Schedule C candidate identification kicks in over **$300**.
- **Dual-path IE reporting is real and must be deduped**: Kansas Comeback PAC filed dedicated IE statements (Oppose Ty Masterson: $359,633 + $10,810.63 + $8,500 + $5,000) AND repeated the identical vendor/amount transactions on its regular PAC Schedule C (PAC869_202607) with candidate named but NO direction. Never sum the two paths; dedicated statement wins on direction, Schedule C is corroboration.
- **"Total this Period" on IE statements is a running cumulative control total across successive statements** (370,443.63 → 378,943.63 → 383,943.63), despite the label. Sum unique rows; validate against the running total; never add the total lines.
- Blank PAC Schedule C confirmed: single column "If independent or in-kind expenditure in excess of $300... list candidate name & address" — no support/oppose field, and independent vs in-kind is not structurally distinguished.
- **Koch GA case**: $1,544.08 email communication supporting 34 unnamed candidates (~$45.41 each, unitemized per KPDC advice) — unallocatable; exclude from candidate totals, keep as coverage caveat.
- Amendments are full replacement reports (Perry H003DP: in-kind $223.28 → $463.28, +$240 row; whole report re-filed). Take latest, never add.
- Affidavit of exemption exists for candidates under $1,000 per campaign phase — a legitimate no-report status, not zero.
- Local offices (county, first-class cities, Wichita school board, etc.) file with county election officers — no central repository; out of v1 scope.
- KPDC "Campaign Contributor Data" page's structured search = LEGACY app (`kansas.gov/ethics/EthicsSite/`), genuinely marked "Last updated March 15, 2022". Historical only (back to 1993).

**Where that report is wrong (its central architectural conclusion):**
- It concluded "no current structured source; PDF-first, document-heavy connector" because it only found the stale legacy search and dismissed the SOS viewer as "internal report-viewer URLs, not a public API; do not build against a browser session." **The SOS CFR viewer (`sos.ks.gov/elections/cfr_viewer/`) is public, live, and current** — verified: Holscher contribution rows through 7/23/2026, IE filings listed through 8/20/2026, Candidate-filings search enumerating 354 House R&E reports filed Jul–Aug 2026, and native-HTML schedules for e-filers. Its `btnExport` returns the FULL search result set in one file (4,285 rows verified) and the whole postback chain works in curl with a browser UA. PDF-first is only needed for the paper-filing minority and IE statements; everything else has a structured/current path.
- Its "12 IE filers" (Aug 2) is now 27 PDFs on the KPDC index + 316 filings visible in the viewer — the viewer is FRESHER than the scanned index (Aug filings not yet scanned to KPDC).

**Policy adds worth keeping:** spouse-occupation caption; coverage-state enum on outside totals (explicit rows vs unresolved-direction vs unallocated vs none-found); don't infer direction from committee name/ideology; occupation coverage denominators (itemized-individual vs all-direct dollars); K.S.A. 25-4154(d) counsel review before publishing donor NAMES (occupation aggregates are the safe surface).

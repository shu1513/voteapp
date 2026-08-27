# South Carolina campaign finance — feasibility findings (2026-08-26)

Verdict: **BUILDABLE** for candidate-side money (totals, itemized donors with occupation, itemized spending). Outside-group support/oppose stance is **not structured** — committee filings exist and are current, but independent expenditures carry no candidate/stance fields.

## Systems

1. **New system (candidates): `ethicsfiling.sc.gov`** — Angular SPA on a Tyler Technologies backend with an **open JSON API, no auth, no key**. All state/local candidate campaign-disclosure data (House/Senate/statewide/local filers).
2. **Old system (committees): `apps.sc.gov/PublicReporting`** — ASP.NET WebForms, cookie/viewstate postbacks. Non-candidate committees, political parties, caucuses, ballot-measure committees. **Still current** (reports through July 2026 verified). Per-schedule CSV download links exist.

## Candidate API endpoints (all `https://ethicsfiling.sc.gov/api/...`)

| Purpose | Endpoint | Body |
|---|---|---|
| Filer search (last-name prefix) | `POST Ethics/Get/Public/Search/By/Filer/Name/` | bare JSON string, e.g. `"Wilson"` |
| Report list + running totals | `POST Ethics/Get/Public/Candidate/Reports` | `{"candidateFilerId":N}` |
| Itemized contributions | `POST Candidate/Contribution/Search/` | `{"candidate":"...","officeRun":"...","contributionYear":"2026",...}` (all optional) |
| Itemized expenditures | `POST Candidate/Expenditure/Public/Get/All/Campaign/Expenditures` | `{"candidate":"...","expenditureYear":"2026",...}` |
| Advanced expenditure search | `POST Candidate/Expenditure/Public/Get/All/Advanced/Campaign/Expenditures` | city/state/zip, amount + date ranges |
| Public report detail (period + cycle summary, all itemized rows, versions) | `GET Ethics/Get/Public/Candidate/Report/Details/{reportId}` | — (open; POST `Candidate/Report/Get/*` variants are 401) |
| All offices/positions lookup | `Ethics/Get/Public/All/Offices/Positions` | — |

- **Totals**: report list rows carry `contributions` / `expenses` / `balance` **cumulative campaign-to-date** per submitted report (Initial, Quarterly, Pre-Election). Latest report = total raised / total spent. Verified cent-exact: Alan Wilson governor campaign itemized 2025+2026 sum $4,859,328.27 == Q2-2026 report `contributions`.
- **Contribution rows**: `contributionId, officeRunId, candidateId, date, amount, candidateName, officeName, electionDate, contributorName, contributorOccupation, group (Yes/No), contributorAddress, description`. Occupation fill rate 100% on individual rows sampled (blank for `group=Yes` rows, i.e. PAC/entity donors).
- **Expenditure rows**: `expId, campaignId, expDate, vendorName, amount, address, expDesc`.
- `Candidate/Report/Get/Report/Summary` is filer-side only (401). `Candidate/Report/Public/Campaign/Get/Reports` ignores its filter and dumps the full statewide report index (usable for enumeration).

## Gotchas

- **2026-cycle statewide office labels are broken**: `officeName` comes back as the literal string `"4"` for the 2026 governor race, so office-text search (`officeRun:"Governor"`) misses the whole 2026 field. Filter by candidate name / `officeRunId` / `contributionYear` instead.
- **Alan Wilson files as "Wilson, Michael A"** (legal first name Michael). Governor run: `candidateFilerId 54344`, `officeRunId 77574`, election 2026-06-09 (officeRun rows are per election event — primary and general are separate officeRunIds).
- Filer-name search matches last-name prefix only; `"Wilson, Alan"` returns 0.
- Year filters are plain strings (`"contributionYear":"2026"`); object-shaped values 400. Omitting filters returns everything (Governor all-time = 174k rows / 65 MB in one response — API has no paging and no apparent size cap).
- Election dates on multi-cycle candidates repeat June primary dates; a candidate's runs are keyed by `officeRunId`, not office text.

## Outside groups (old portal)

- Path: `IndividualCommittee/Committee.aspx` → type (Ballot Measure / Caucus / Non-Candidate / Political Party, each at State/County/City level) → name search (min 3 chars, begins-with or contains) → committee → report index (quarterly, 2004→current) → tabs: Summary / Contributions / Expenditures / Loans / Loan Payments / Assets, each with a CSV download.
- Summary tab = period + YTD roll-ups (contributions incl. in-kind and loans; expenditures; cash on hand).
- **Expenditure schedule columns: Date / Vendor / Address / Description / Amount only. No support-oppose field, no candidate link.** IEs appear as free-text descriptions ("Independent Expenditures", vendor = media buyer, e.g. Sinclair Public Affairs $270,175 on 2026-05-18 from SC REALTORS PAC) with **no target candidate**.
- PAC→candidate **direct** giving is recoverable two ways: candidate-side (contribution rows with `group:"Yes"`) — preferred, structured; or committee-side vendor names ("X for SC House"), fuzzy.
- Conclusion: SC outside-spending stance would need the Mississippi-style manual route or ad-hoc research, not an adapter. Candidate-side sync does not depend on the old portal.

## What VoteApp gets

- Total raised / total spent / cash on hand: **yes**, per report, cent-exact, quarterly cadence (due the 10th after each quarter + pre-election reports).
- Direct donor occupations: **yes** (individuals; entity donors identified by name + address).
- Itemized spending: **yes**.
- Outside groups for/against a candidate: **no structured source**; committee registry + totals exist on the old portal.

## Statute + audit notes (verified 2026-08-26)

- SC Code §8-13-1302(A) makes campaigns MAINTAIN contributor occupation (no employer anywhere in current law); §8-13-1308(F) certified public reports itemize sources >$100 and require expenditure purpose AND beneficiary — beneficiary is absent from every public schedule (source-contract gap). §8-13-1308(D)(2): immediate IE reports over $10k statewide / $2k other. §8-13-1300(7)/(31)(c): money for communications within 45 days of an election is excluded from "contribution" and sits in a separate account → committee donor lists can understate election-period funding. S.813 (introduced 2026-01-14, in Senate Judiciary) would add occupation+employer to the public-report statutes; not law.
- Report versions are real and REPLACE: e.g. Evette pre-election 2026 = Original 423616 + Amendments 1–4 (current 430061); the report-list endpoint points at the newest version. Details/{id} works for any version id.
- In practice filings itemize everything (rows down to $3); itemized sum == period total to the cent; no unitemized line exists in the detail schema.
- Election-cycle "Cash Contributions" in report detail excludes loans/in-kind (income is broken out by type, each with filingPeriod + electionCycleTotal).
- Constitutional-officer archive ssl.sc.gov/Ethics is currently DOWN (redirects to Maintenance).
- An external feasibility report claimed a legacy "global expenditure search" capped at 6 months / 500 records — no such search exists on either portal today; do not design around it.

## Review-audit addendum (2026-08-26, second probe round)

- **ID spaces are disjoint**: report-detail contribution rows carry `contributorId` (entity id; Wilson Q2: 783 rows, 761 unique); `Contribution/Search` rows carry `contributionId` (transaction id). Zero overlap — a detail<->search join is impossible. Breakdowns must come from search rows (only source of the `group` flag); details are for authoritative summaries.
- **Search contract**: no server-side `candidateId` field — unknown fields are ignored, and a body with only unknown fields returns HTTP 500 ("Please search for something using at least one of the fields"), not a statewide dump. Numeric and string `contributionYear` behave identically (1,813 rows both ways for candidate "Wilson", 2026). Filer-name search is contains/fuzzy (returns `Johnson-Wilson` for "Wilson"), not prefix.
- **Search rows = cash + in-kind**: Wilson search-row sum $4,859,328.27 equals income Total = Cash $4,817,978.10 + In-kind $41,350.17 (loans/personal/credits are $0 for him).
- **Cycle totals RESET per election run**: McMaster 2022 primary run peaks at $5,528,030.35, then `Quarter 3 & Pre-Election (General) Report 2022` (elec 11/8/2022) restarts at $2,103,841.11. Campaign totals = sum of run-final cumulatives; balances carry across. Combined report names exist: `Quarter 3 & Pre-Election (General) Report 2022` (type `Pre-Election Quarterly`), `Quarter 4 & Final 2023 Report`.
- **Amendment ordering trap**: order reports by filing-period end, then newest version. Evette's pre-election was amended 2026-07-14 (cycle $4.58M) after Q2 filed 2026-07-10 (cycle $6.20M); timestamp ordering regresses totals. Amendments consolidate rows: pre-election Original 423616 = 399 rows vs current Amendment 4 (430061) = 356 rows, identical totals.
- **Refunds are positive expenditure rows** (`Refund Excessive`, `CHARGEBACK`), already inside the expenditure Total; no negative contribution rows observed. No donor-side netting.
- Correction to the earlier note: the *candidate* portal does have global contribution/expenditure searches; it is the *committee* portal that lacks any target-aware transaction search.

# Idaho campaign finance — build plan

Status: rev 3 — Phase 0 MERGED (#1025); Phase 1 MERGED (#1041, 2026-09-02) and run locally (migration 268 applied, 101 of 108 candidates auto-linked); Phase 2a BUILT 2026-09-02 (`idahoContributionAggregator.ts`, verified on live rows). Next = Phase 2b (outside money) then Phase 3 (acquisition + sync, rules pinned below).
Source facts: `backend/docs/idaho-campaign-finance.md` (endpoints, quirks, and the two findings that shaped this plan; committed with Phase 0).
Template module: `backend/src/pipeline/newHampshireFinance/` (same Civix CFIS vendor; same JSON envelope, pagination, bulk-export endpoint, client error model).
Writer: `createStandardStateFinanceSnapshotWriter` (`pipeline/finance/standardStateFinanceSnapshotWriter.ts`), as NH/Montana.

## What Idaho gives us (verified live 2026-09-01)

| Feature | Verdict | Source surface |
|---|---|---|
| Raised / spent / cash on hand | YES, official per-registration totals | candidate grid `PublicFilerDetails/GetCandidateDetails` (`totalRaised`, `totalSpent`, `balanceOfFunds`) |
| Contribution rows (size, donor type, in-state) | YES, complete and current | `PublicTransactionDetails/GetContributionsDetails` by filer name, rows filtered by `filerRegistrationGuid` |
| Primary / General split | partial — per-row `electionTypeCode`; no official split totals | search rows |
| Donor occupation / employer | **NO** — Idaho does not collect either (Idaho Code 67-6607) | none; occupation chart stays null, no industry fallback |
| Outside spending target | YES — target resolves to the candidate's registration guid on 99% of rows | IE search `PublicIndependentExpenditureDetails/GetIndependentExpenditureDetails` |
| Outside spending stance | YES, filer-declared Support / Oppose on every IE row | IE search |
| Electioneering communications | excluded by construction (IE search never returns them; stance is N/A) | — |
| Bulk CSV export | contract-checked only; **not a row source** (see finding 2) | `ExportData/GetExportPublicDownloadData` |

2026 registration reality (full 2,048-row grid pull): 729 registrations for election year 2026 (600 Active / 127 Terminated / 2 Inactive); State Representative 207, County Commissioner 156, State Senator 107, Clerk 59, Governor 22, District Judge 23. The grid lists registrations, not a ballot: we link FROM our Nov-2026 roster, never from the grid. US Senate / US House = FEC path, untouched. County / city / district candidates are exempt until $500 raised or spent (67-6608): an absent registration is "no filing", never $0.

## The two findings the design rests on

1. **Grid `totalRaised` = Σ current contribution transactions of that registration, cent-exact.** Proven on every probed registration once rows are attributed by `filerRegistrationGuid` (Achilles 2024: 286 rows = $89,667.61; Achilles 2026, Blad, Blanksma, Bruno, Boyle ×2, Ackerman all exact). Returned contributions are subtracted by the state and are not served by the contribution search, so registrations with returns show grid < search; totals come from the grid, never recomputed from rows. **Revised 2026-09-02** (survey of the 60 top-raised eligible 2026 registrations, single-page fetch): 42 cent-exact; 8 rows > grid, each a "Return Contribution" the state subtracts; 10 rows < grid — rows of a *filed* monthly report the search does not serve (Stegner: 168 of the July monthly's 177 rows, $30,552.85; Hickman: the whole June monthly, $8,622.46; the rest $117–$1,810). `GetFinancialSummaryDetails.totalContributions` equals the grid; loans are separate. The grid stays the headline; rows are a breakdown basis whose coverage is reported.
2. **The bulk CSV export contains exactly the version-1 transactions.** Every contribution edited or added through an amended report (transactionVersionId > 1) is missing from the export while it is present in the search API and in the grid total. Verified row-for-row on four filers (bulk rows == search rows with version 1, same Transaction Ids, same sums). Effect on the whole grid: only 89% of registrations reconcile from bulk sums; the rest are short by the amended-in money. The export also cannot be filtered per registration. Therefore: **bulk CSV is not used for totals or breakdowns.** Phase 0 keeps the contract check (headers, encoding, row shape) and the probe fails if the export ever stops being version-1-only, so the decision gets revisited on evidence.

Consequences: no 20 MB artifact cache; the raw artifact per link is the registration's own JSON (grid row + contribution rows + IE rows), sha256-manifested like Missouri/Montana. Cycle attribution is free: the search row carries the registration guid, and the grid row carries `electionYear` and `filingCycleId`.

## Architecture (Phase 1+)

- `idahoCfsClient.ts` (Phase 0) — anonymous JSON client. Bulk export, candidate grid (one page of 5,000 covers the whole grid), contribution search by name (`sortBy TransactionDate desc`, `filerRegistrationGuid` in the body is ignored server-side), IE search (one page of 10,000 covers all-time today; paginated anyway). Edge rejects library user agents: send `Mozilla/5.0` + SPA Origin/Referer (NH precedent).
- `idahoCfsCsv.ts` (Phase 0) — windows-1252 decode; exact 28/34-column headers (expenditure header has a trailing space on `Filing Entity Name `); records split by raw newlines are re-joined; unrepairable rows quarantined (never thrown); `assertIdahoCsvQuarantineTolerance` fails closed above 1%; `idahoCsvElectionYear`/`idahoCsvElectionStage` absorb the swapped election columns.
- `idahoPhaseZero.ts` (Phase 0) — reconciliation + IE summary, no DB.
- `idahoFinanceEligibleOffices.ts` (Phase 1) — VoteApp `scope::canonical_name` → grid `office` + district kind. 16 keys: 7 statewide (Comptroller → "State Controller"), State Senator, State Lower Chamber Legislator → "State Representative", and 7 county keys (Commissioner, Treasurer, Assessor, Coroner, Sheriff; both "County Clerk" and "Clerk of Court" → "Clerk", the county clerk being clerk of the district court ex officio). Judges, prosecutors, and special districts stay out until the roster carries them.
- `idahoCandidateFilerResolver.ts` (Phase 1) — roster candidate → grid registration, pure: grid `electionYear` == race year, exact grid office, district evidence by kind (legislative district number from the district geoid, House seat kept as a label only; county from the district name; commissioner seat from the ballot title "County Commissioner District N" against grid `seatZone`), then full-name evidence through the shared middle-name gate (`personNamesMatchWithMiddleEvidence`) with one-sided roster→grid nickname expansion; the grid's quoted call name becomes a parenthetical alias; display name tried first, structured "First Last" second. Several registrations of one entity in the race year: exactly one Active links, otherwise ambiguous (manual review). Never links from `committeeName` text. Known misses that stay manual: roster spellings outside the nickname table ("Rod W. Beck" vs grid "Rodney William") and people who go by their middle name ("Eric Myricks" vs grid "William Eric Myricks II").
- `idahoCandidateFinanceAutoLink.ts` (Phase 1) — lists Nov-2026 Idaho candidate elections in eligible offices with no active link, pulls the grid once (one 5,000-row page), resolves, writes links only (`link_source = 'sunshine_grid'`, deep-link `sourceUrl`); `npm run idaho-candidates:finance:auto-link -- --dry-run` reports without writing. Gated by the sync flag (live API).
- `idahoRegistrationArtifactCache.ts` (Phase 1) — one JSON per registration guid (grid row + its contribution rows + the IE rows targeting it) under `scratch/idaho-campaign-finance/registrations/` (gitignored), sha256 manifest, 0700/0600, identity checked on store and read (every row must carry the key's guid).
- `idahoFinanceWriter.ts` (Phase 1) — `createStandardStateFinanceSnapshotWriter` over the `id_*` tables; link identity `registration_guid`, outside identity `filer_key`, signed cash on hand, manual-link protection, `sunshine_grid` supersession.
- `idahoContributionAggregator.ts` (Phase 2a) — pure direct-money aggregator: grid totals in, size and source-type breakdowns plus `rowCoverage` out (rules under Phase 2a).
- `idahoOutsideSpendingAggregator.ts` (Phase 2b) — outside money.
- writer + sync + due-list + scheduler (Phase 2/3) — clone NH/Montana shapes.

## DB (Phase 1)

Migration `268_add_idaho_campaign_finance_tables.sql` (267 is claimed by the open West Virginia PR): five `id_candidate_finance_*` tables cloned from migration 249's `nh_candidate_finance_*` set. Link row identity = `registration_guid` (lowercase uuid **text** with a regex CHECK, so the standard writer/loader compare it like every other state's committee id) + `filer_name`; no extra entity/registration-id columns — the standard writer's link insert is fixed and the sync re-pulls the grid by guid anyway. `link_source IN ('manual','sunshine_grid')`; `election_year` 2026+; direct `category_type IN ('contribution_size','contributor_source_type')` (Vermont precedent — Idaho has no occupation/employer; in-state share has no standard category and is a Phase 2a UI decision); outside identity `filer_key` (filer registration guid when registered, else a name-derived key Phase 2b defines) + `filer_name`; `cash_on_hand` signed (grid `balanceOfFunds` goes negative). Identifiers ≤63 chars; never renumber. DDL validated against the local schema in a rolled-back transaction 2026-09-02; not applied.

Source enum `IDAHO_SUNSHINE` in `ballotLookupFinanceShared.ts`; label `IDAHO_SUNSHINE: "Idaho Secretary of State Sunshine Portal"` in `packages/api-client/src/format.ts` `FINANCE_SOURCE_LABELS` (+ `format.test.ts`); homepage `https://sunshine.voteidaho.gov` in `FINANCE_SOURCE_HOME_URLS` (`packages/api-client/src/finance.ts`, + test). `sourceUrl` on links/summaries = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=<registrationGuid>&tabName=CAN&isLegacy=false` (verified deep link).

## Flags (Phase 1)

Code defaults `false` in `featureFlags.ts` (done); all three in `backend/.env.example` and the read flag in `render.yaml` at `"false"` until Phase 4 (done). Set the read flag ON in the local `backend/.env` of the main checkout per [voteapp-new-state-finance-checklist] once migration 268 is applied (free read-side flags stay on; sync and raw refresh hit the live API and stay off unless a batch run is intended):

```text
IDAHO_CAMPAIGN_FINANCE_ENABLED
IDAHO_CAMPAIGN_FINANCE_SYNC_ENABLED
IDAHO_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED
```

Phase 0 adds no flags: the probe is a read-only anonymous fetch with no persistence (NH precedent).

## Phases

### Phase 0 — acquisition contracts (BUILT 2026-09-01, this PR)

Built: `backend/src/pipeline/idahoFinance/{idahoCfsClient,idahoCfsCsv,idahoPhaseZero,index}.ts`, `backend/src/scripts/probeIdahoCampaignFinance.ts` (`npm run idaho-candidates:finance:phase-zero`), sanitized fixtures under `backend/tests/fixtures/idahoFinance/`, tests under `backend/tests/pipeline/idahoFinance/` and `backend/tests/scripts/`.

Gates the probe enforces (throws on failure):
- bulk TCON files for the requested filing years and the cycle-year TEXP file parse against the exact headers, with quarantine ≤ 1%;
- for the fixture entity (Todd Achilles, entity 257, two registrations) every registration's search-row sum equals grid `totalRaised` to the cent;
- bulk rows for the registration equal exactly its version-1 search rows (same transaction ids with the same amounts) within the downloaded filing years — the export is still version-1-only;
- no bulk contribution row of the fixture entity is missing from the union of its registrations' search rows — the search is still a superset of the export;
- the all-time IE list carries only Support/Oppose stances and only `TIECOM`/`TEXP` transaction codes.

Live results 2026-09-01 (`npm run idaho-candidates:finance:phase-zero`, exit 0, every gate green; corpus figures from the Python spike behind this PR, reproduced by the TypeScript parser on the same downloaded files):
- probe: grid 2,048 registrations (729 for 2026); TCON 2025 = 82,590 rows / 23 quarantined, TCON 2026 = 53,460 / 17, TEXP 2026 = 23,158 / 3; Achilles 2024 = 286 search rows = $89,667.61 = grid; Achilles 2026 = 32 rows = $5,900.00 = grid, bulk 32 rows = version-1 rows exactly; IE 2026 = 2,956 rows (2,919 candidate targets, 2,867 with a registration guid, 52 name-only; 531 from non-registered filers), $3,322,917.76 support / $874,787.88 oppose including the 37 measure rows;
- grid: 1,629 entities, 412 entities with more than one registration;
- TCON rows 2023–2026 (TypeScript parser): 38,813 / 97,428 / 82,590 / 53,446 usable after re-join; quarantined 0 / 0 / 23 / 17 — all the `,Äô` apostrophe corruption (29 cells) or a stray opening quote that swallows fields (24 cells), PAC/central-committee rows; the parser recovers 1–6 rows more per file than the Python `csv` module did;
- `Amended` = `N` on 100% of export rows (consistent with version-1-only);
- IE list: 9,897 rows all-time (2024: 5,497; 2025: 1,421; 2026: 2,956), stance 7,707 Support / 2,190 Oppose, candidate rows 9,522 of which 8,738 carry a registration guid (8,635 match a current grid guid), 1,159 name-only targets (`isCandidateNonRegisteredEntity`), 2,359 rows from non-registered filers (`TEXP` code);
- 2026 candidate-target IE money: $3,403,243.20 support / $849,889.46 oppose (API) vs $3,144,754.32 / $674,202.03 in the bulk TEXP (version-1 subset, registered filers only);
- one IE transaction = one parent row + one allocation row per target; `amountApplied` is the per-target figure; 87 of 735 transaction groups allocate less than the transaction amount (partial allocation is legitimate); identical allocation rows recur (225 all-time) and the state counts them — keep them.

### Phase 1 — schema, flags, source label, filer resolver, artifact cache, writer (BUILT 2026-09-02)

Built: migration 268; `IDAHO_CAMPAIGN_FINANCE_{ENABLED,SYNC_ENABLED,RAW_DATA_REFRESH_ENABLED}` (+ `.env.example`, `render.yaml`); source `IDAHO_SUNSHINE` + label + home URL (+ tests); `backend/src/pipeline/idahoFinance/{idahoFinanceEligibleOffices,idahoCandidateFilerResolver,idahoCandidateFinanceAutoLink,idahoRegistrationArtifactCache,idahoFinanceWriter}.ts`; `backend/src/scripts/autoLinkIdahoCandidateFinance.ts` (`npm run idaho-candidates:finance:auto-link`, `--dry-run`, `--force`, `--max-candidates`, `--lookback-days`, `--lookahead-days`); grid row contract gains `seatZone`; tests under `backend/tests/pipeline/idahoFinance/` (shared factories in `idahoTestFixtures.ts`), `backend/tests/scripts/`, `backend/tests/config/`. No live run, no data written.

Live run (local, 2026-09-02, after migration 268): `auto-link --dry-run` then real — 108 attempted, 101 linked (94 name_exact, 7 name_nickname, all checked: Debbie/Deborah, Ben ×2, Dan, Doug ×2, Pam), 0 ambiguous, 0 errors, 7 unmatched for manual links: Eric Myricks (grid "Myricks II, William Eric"), Pro-Life ("Pro-Life, Pro-Life"), W. Lane Startin ("Startin, Wesley 'Lane'"), Desi Burbank ("Burbank, Desiree Leigh"), C. Scott Grow ("Grow, Cecil Scott"), Jan Brown ("Brown, Janice Marie", one Active of three), Marty Kilhefner ("Rotz-Kilhefner, Martha Louise").

Resolver rules are pinned by tests against the real grid shapes (Senate district, House seat label "16B", commissioner seat "Ada 2", both clerk spellings, statewide null district, call-name alias, nickname expansion, middle-initial corroboration, middle conflict rejection, Active-over-Terminated, two-Active ambiguity, wrong-year exclusion).

### Phase 2a — direct money (BUILT 2026-09-02)

Built: `idahoContributionAggregator.ts` (pure, no I/O) + tests; verified on live rows for 7 registrations including Little (4,725 search rows). Summary = grid `totalRaised` / `totalSpent` / `balanceOfFunds` (signed); `directContributionTotal` = Σ positive direct rows. Rows are selected by registration guid + `TCON`; an unknown subtype, a row of another entity or cycle, a repeated transaction id, or a repeated row guid throws. Breakdowns: `contribution_size` = the NH buckets over `ITMY`/`INKIND` plus one lump "Unitemized small contributions" over `NITMY`/`ANYMS` (digit-free label so the shared bucket sort keeps it last); `contributor_source_type` over every direct row — TIND → individuals, TBSN → business_nonprofit_entities, TPAC → pac_independent, TCENC → party_committee (central committees), TCAN/TSELF → candidate_self, else/null → other (the names NM/NE/VT already store). `ITR` (interest) is in the grid total but is not a contribution. `contributorCount` stays null (NH precedent).

Decisions made here, binding on Phase 3:
- in-state share (`stateType`) is not written: no category type or card slot; the state's `GetContributionsInStateAndOutStateDetail` endpoint exists if a card is ever wanted;
- row coverage is reported (`rowCoverage` exact / rows_exceed_grid / rows_below_grid with `rowTotal`, `gridTotalRaised`), never enforced. Sync rule: the summary is always written (the grid is official); breakdowns are written too, with a coverage note whenever coverage is not exact ("breakdowns cover $rowTotal of the $gridTotal state total"); no quarantine on a shortfall — the gap is the state's search, not our data;
- acquisition rule: fetch the contribution search as ONE page (`pageSize` 10,000; the largest 2026 filer-name result is Little at 4,725) and fail closed when `totalItems > items.length`. Date-sorted paging at 500 is unstable (Little: 30 rows duplicated, 30 dropped) and `sortBy` other than `TransactionDate` returns HTTP 500;
- loader gap: the per-state ballot-lookup loaders surface `occupation`/`industry`/`contribution_size` only, so `contributor_source_type` rows are stored (NM/NE/VT precedent) but not yet shown. Surfacing them is a UI decision for Phase 3.

Original spec (kept for the record):

- summary: raised = grid `totalRaised`, spent = grid `totalSpent`, cash on hand = grid `balanceOfFunds`, as-of = latest `filedDate` among rows (fallback: sync time);
- breakdowns from the registration's contribution rows: size buckets over itemized rows (`ITMY`, `INKIND`), unitemized lump = Σ `NITMY` rows (+ `ANYMS`), donor type from `transactionSourceTypeCode` (TIND/TSELF/TCAN → individual; TBSN/TPAC/TCENC → organization) stored as `contributor_source_type`; in-state share from `stateType` (INST/OTST) has no standard category type or card slot — decide (new category + loader/UI, or a coverage note) before writing it;
- coverage note: itemized share = Σ rows / grid total (returns make grid < rows; show "state total" as the headline and never let a breakdown exceed it);
- occupation chart null with the Idaho footnote (not collected by the state).

### Phase 2b — outside spending

- source = IE search; select rows by `candidateMeasureFilerRegistrationGuid == link.registration_guid`, plus name-only rows (`isCandidateNonRegisteredEntity`) matched against the linked candidate's roster name + office with the standard name matcher, quarantined when ambiguous;
- window = transaction dates within the registration's cycle (grid `electionYear`, previous registration's year exclusive), support/oppose totals over `amountApplied`; keep duplicate allocation rows; ECs never enter;
- outside-group rows keyed by `filerName` (+ `filerRegistrationGuid` when present; `TEXP`-code rows are non-registered filers and get a "not registered in Idaho" note);
- UI: standard supporting/opposing cards, no Idaho-specific footnote needed (stance is filer-declared).

### Phase 3 — local live run

Nov-2026 rosters → `idaho-candidates:finance:auto-link` (dry-run first, then write; ambiguous/unmatched → manual links) → sync all links → validation report. Roster reality 2026-09-02: 212 Idaho Nov-2026 general elections locally, 57 with candidates (120 links) — State Senator 72, federal 12, statewide 26, county ~20, State Representative none — so the roster gap, not the pipeline, caps this phase. (grid/search match rate must be 100% for linked registrations; anything else quarantines the link). Record results in this doc and the findings doc.

### Phase 4 — prod

Migrations, `IDAHO_CAMPAIGN_FINANCE_ENABLED` in render.yaml, `id_*` pg_dump promotion, deploy, re-sync near November (monthly reports due the 10th; 48-hour reports land in the search API immediately).

## Validation gates (import refuses to write on failure)

- grid row present and `electionYear` matches the link's cycle; the auto-link only creates links to Active registrations (a lone Terminated/Inactive one is reported for manual review), but an existing link keeps syncing after its registration terminates — the money stays public;
- row coverage is reported, not enforced: rows > grid = returned contributions the state subtracts; rows < grid = rows of a filed report the search omits (survey 2026-09-02: 10 of 60). The summary still comes from the grid and breakdowns carry a coverage note;
- every IE row selected carries stance Support or Oppose and a date inside the window;
- artifact sha256 matches the manifest before any snapshot write.

## Explicitly rejected (with reasons)

- Bulk CSV as the row or totals source — version-1-only export (finding 2); it also cannot be filtered per registration and needs a 20 MB download per filing year.
- Legacy archive `sunshine.sos.idaho.gov` — data ends 2023; irrelevant for Nov 2026.
- Donor occupation/employer and the employer-industry pipeline — nothing to classify.
- Passing `filerName` to the candidate grid — server-side timeout; we fetch the whole grid (one page) and filter locally.
- Electioneering communication rows — no stance by statute (67-6628); the IE search already excludes them.
- Rebuilding `totalRaised` from rows — returned contributions are not served by the search; the state's figure is the official one.

# Idaho campaign finance — build plan

Status: rev 1 — Phase 0 BUILT 2026-09-01 (client, CSV parser, reconciliation module, probe script, sanitized fixtures; live probe results below). Next = Phase 1.
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

1. **Grid `totalRaised` = Σ current contribution transactions of that registration, cent-exact.** Proven on every probed registration once rows are attributed by `filerRegistrationGuid` (Achilles 2024: 286 rows = $89,667.61; Achilles 2026, Blad, Blanksma, Bruno, Boyle ×2, Ackerman all exact). Returned contributions are subtracted by the state and are not served by the contribution search, so registrations with returns show grid < search; totals come from the grid, never recomputed from rows.
2. **The bulk CSV export contains exactly the version-1 transactions.** Every contribution edited or added through an amended report (transactionVersionId > 1) is missing from the export while it is present in the search API and in the grid total. Verified row-for-row on four filers (bulk rows == search rows with version 1, same Transaction Ids, same sums). Effect on the whole grid: only 89% of registrations reconcile from bulk sums; the rest are short by the amended-in money. The export also cannot be filtered per registration. Therefore: **bulk CSV is not used for totals or breakdowns.** Phase 0 keeps the contract check (headers, encoding, row shape) and the probe fails if the export ever stops being version-1-only, so the decision gets revisited on evidence.

Consequences: no 20 MB artifact cache; the raw artifact per link is the registration's own JSON (grid row + contribution rows + IE rows), sha256-manifested like Missouri/Montana. Cycle attribution is free: the search row carries the registration guid, and the grid row carries `electionYear` and `filingCycleId`.

## Architecture (Phase 1+)

- `idahoCfsClient.ts` (Phase 0) — anonymous JSON client. Bulk export, candidate grid (one page of 5,000 covers the whole grid), contribution search by name (`sortBy TransactionDate desc`, `filerRegistrationGuid` in the body is ignored server-side), IE search (one page of 10,000 covers all-time today; paginated anyway). Edge rejects library user agents: send `Mozilla/5.0` + SPA Origin/Referer (NH precedent).
- `idahoCfsCsv.ts` (Phase 0) — windows-1252 decode; exact 28/34-column headers (expenditure header has a trailing space on `Filing Entity Name `); records split by raw newlines are re-joined; unrepairable rows quarantined (never thrown); `assertIdahoCsvQuarantineTolerance` fails closed above 1%; `idahoCsvElectionYear`/`idahoCsvElectionStage` absorb the swapped election columns.
- `idahoPhaseZero.ts` (Phase 0) — reconciliation + IE summary, no DB.
- `idahoCandidateFilerResolver.ts` (Phase 1) — roster candidate → grid registration for the cycle year: exact last name + first-name token + office family + district/jurisdiction; multiple registrations per entity are normal (one per cycle), pick `electionYear == cycle year`; same-year duplicates (11 entities today, usually a terminated re-registration) → prefer Active, else review queue. Never link from `committeeName` text.
- `idahoContributionAggregator.ts` (Phase 2a) — direct money.
- `idahoOutsideSpendingAggregator.ts` (Phase 2b) — outside money.
- writer + sync + due-list + scheduler (Phase 2/3) — clone NH/Montana shapes.

## DB (Phase 1)

Migration `<next>_add_idaho_campaign_finance_tables.sql`: five `id_candidate_finance_*` tables cloned from migration 249's `nh_candidate_finance_*` set. Link row identity = `registration_guid` (uuid, unique) with `filer_entity_id`, `filer_registration_id`, `election_year` alongside; `link_source IN ('manual','sunshine_grid')`. Identifiers ≤63 chars; never renumber.

Source enum `IDAHO_SUNSHINE` in `ballotLookupFinanceShared.ts`; label `IDAHO_SUNSHINE: "Idaho Secretary of State Sunshine Portal"` in `packages/api-client/src/format.ts` `FINANCE_SOURCE_LABELS` (+ `format.test.ts`); homepage `https://sunshine.voteidaho.gov` in `FINANCE_SOURCE_HOME_URLS` (`packages/api-client/src/finance.ts`, + test). `sourceUrl` on links/summaries = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=<registrationGuid>&tabName=CAN&isLegacy=false` (verified deep link).

## Flags (Phase 1)

Code defaults `false` in `featureFlags.ts`; add all three to `backend/.env.example`, the read flag to `render.yaml`, and set them ON in local `backend/.env` per [voteapp-new-state-finance-checklist] (free read-side flags stay on; raw refresh hits the live API and stays off unless a batch run is intended):

```
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
- bulk rows for the registration equal exactly its version-1 search rows (row count and cents) within the downloaded filing years — the export is still version-1-only;
- the all-time IE list carries only Support/Oppose stances and only `TIECOM`/`TEXP` transaction codes.

Live results 2026-09-01 (`npm run idaho-candidates:finance:phase-zero`, exit 0, every gate green; corpus figures from the Python spike behind this PR, reproduced by the TypeScript parser on the same downloaded files):
- probe: grid 2,048 registrations (729 for 2026); TCON 2025 = 82,590 rows / 23 quarantined, TCON 2026 = 53,460 / 17, TEXP 2026 = 23,158 / 3; Achilles 2024 = 286 search rows = $89,667.61 = grid; Achilles 2026 = 32 rows = $5,900.00 = grid, bulk 32 rows = version-1 rows exactly; IE 2026 = 2,956 rows (2,919 candidate targets, 2,867 with a registration guid, 52 name-only; 531 from non-registered filers), $3,322,917.76 support / $874,787.88 oppose including the 37 measure rows;
- grid: 1,629 entities, 412 entities with more than one registration;
- TCON rows 2023–2026 (TypeScript parser): 38,813 / 97,428 / 82,590 / 53,446 usable after re-join; quarantined 0 / 0 / 23 / 17 — all the `,Äô` apostrophe corruption (29 cells) or a stray opening quote that swallows fields (24 cells), PAC/central-committee rows; the parser recovers 1–6 rows more per file than the Python `csv` module did;
- `Amended` = `N` on 100% of export rows (consistent with version-1-only);
- IE list: 9,897 rows all-time (2024: 5,497; 2025: 1,421; 2026: 2,956), stance 7,707 Support / 2,190 Oppose, candidate rows 9,522 of which 8,738 carry a registration guid (8,635 match a current grid guid), 1,159 name-only targets (`isCandidateNonRegisteredEntity`), 2,359 rows from non-registered filers (`TEXP` code);
- 2026 candidate-target IE money: $3,403,243.20 support / $849,889.46 oppose (API) vs $3,144,754.32 / $674,202.03 in the bulk TEXP (version-1 subset, registered filers only);
- one IE transaction = one parent row + one allocation row per target; `amountApplied` is the per-target figure; 87 of 735 transaction groups allocate less than the transaction amount (partial allocation is legitimate); identical allocation rows recur (225 all-time) and the state counts them — keep them.

### Phase 1 — schema, flags, source label, filer resolver, artifact cache, writer

- migration + flags + label/home-URL entries above (checklist items 1–2);
- resolver against the grid (rules above) with a `--dry-run` report;
- per-registration JSON artifact cache (grid row + contribution rows + IE rows), sha256 + manifest, `scratch/idaho-campaign-finance/` gitignored;
- snapshot writer via `createStandardStateFinanceSnapshotWriter`;
- unit tests on sanitized fixtures; no live run.

### Phase 2a — direct money

- summary: raised = grid `totalRaised`, spent = grid `totalSpent`, cash on hand = grid `balanceOfFunds`, as-of = latest `filedDate` among rows (fallback: sync time);
- breakdowns from the registration's contribution rows: size buckets over itemized rows (`ITMY`, `INKIND`), unitemized lump = Σ `NITMY` rows (+ `ANYMS`), donor type from `transactionSourceTypeCode` (TIND/TSELF/TCAN → individual; TBSN/TPAC/TCENC → organization), in-state share from `stateType` (INST/OTST);
- coverage note: itemized share = Σ rows / grid total (returns make grid < rows; show "state total" as the headline and never let a breakdown exceed it);
- occupation chart null with the Idaho footnote (not collected by the state).

### Phase 2b — outside spending

- source = IE search; select rows by `candidateMeasureFilerRegistrationGuid == link.registration_guid`, plus name-only rows (`isCandidateNonRegisteredEntity`) matched against the linked candidate's roster name + office with the standard name matcher, quarantined when ambiguous;
- window = transaction dates within the registration's cycle (grid `electionYear`, previous registration's year exclusive), support/oppose totals over `amountApplied`; keep duplicate allocation rows; ECs never enter;
- outside-group rows keyed by `filerName` (+ `filerRegistrationGuid` when present; `TEXP`-code rows are non-registered filers and get a "not registered in Idaho" note);
- UI: standard supporting/opposing cards, no Idaho-specific footnote needed (stance is filer-declared).

### Phase 3 — local live run

Nov-2026 rosters → resolver → sync all links → validation report (grid/search match rate must be 100% for linked registrations; anything else quarantines the link). Record results in this doc and the findings doc.

### Phase 4 — prod

Migrations, `IDAHO_CAMPAIGN_FINANCE_ENABLED` in render.yaml, `id_*` pg_dump promotion, deploy, re-sync near November (monthly reports due the 10th; 48-hour reports land in the search API immediately).

## Validation gates (import refuses to write on failure)

- grid row present and `electionYear` matches the link's cycle; status may be Terminated (money stays public);
- Σ contribution rows for the registration guid ≥ grid `totalRaised` only when returned contributions exist in the bulk export for that entity; otherwise must equal to the cent;
- every IE row selected carries stance Support or Oppose and a date inside the window;
- artifact sha256 matches the manifest before any snapshot write.

## Explicitly rejected (with reasons)

- Bulk CSV as the row or totals source — version-1-only export (finding 2); it also cannot be filtered per registration and needs a 20 MB download per filing year.
- Legacy archive `sunshine.sos.idaho.gov` — data ends 2023; irrelevant for Nov 2026.
- Donor occupation/employer and the employer-industry pipeline — nothing to classify.
- Passing `filerName` to the candidate grid — server-side timeout; we fetch the whole grid (one page) and filter locally.
- Electioneering communication rows — no stance by statute (67-6628); the IE search already excludes them.
- Rebuilding `totalRaised` from rows — returned contributions are not served by the search; the state's figure is the official one.

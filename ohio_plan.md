# Ohio campaign finance — implementation plan

Date: 2026-08-03, revised 2026-08-04 after a second-opinion review (valid items
folded in below; the reviewer's stale-checkout claims — "maryland loader is
bespoke", "migrations end at 208" — were verified false and rejected). Governs
the Ohio state-finance build. Read alongside `plan.md` ("Pause point — add new
states") and `docs/finance-module-capability-matrix.md` — their rules apply to
every PR here. Feasibility research: Ohio report dated 2026-08-02; do not
re-hit the portal during code-only PRs (portal work happens only in the
acquisition spike, when authorized).

## Verdict

Ohio is born on the shared factories with **zero migration debt** and **zero new
factory capability**. Canonical identity (`committee_id` = Ohio stable numeric
MASTER_KEY, `committee_name`), all 5 standard tables, standard summary columns,
std direct categories (`occupation` + `contribution_size`), donor/industry
outside breakdowns. Reference sibling: **maryland** (diff against it before
writing each file).

Scope decision (user, 2026-08-04): outside-group funders/industries (**#3** —
who funds the PACs, industry rollups + donor evidence) **stays in scope**,
built as the last feature PR. Ohio v1 UI parity target = Maryland/Texas.

## Shared-piece config (settled)

- **Writer** — `createStandardStateFinanceSnapshotWriter` wrapper, maryland
  pattern (`marylandFinanceWriter.ts`):
  - `label: "Ohio"`, `minElectionYear: 2000`
  - `summaryUpdatePolicy`: replace all summary fields, preserve-when-null the two
    outside totals (R+oC — direct and outside are built from different artifact
    sets; a direct-only refresh must not wipe outside totals)
  - `outsideGroupValidation: "pairing"` — **mandatory**, cascade-FK trap
    (matrix: breakdowns upsert before stale-group delete; `none`/`presence`
    silently cascade-deletes)
  - `supersededLinkSource: "sos_bulk_export"` (decided at PR 1; also the
    link-source CHECK value next to `'manual'` — ME `cfis_bulk` / MD
    `cfs_public_export` precedent, auto-link re-keys need supersession)
  - No `normalizeCommitteeId` (MASTER_KEY numeric; strict numeric validation +
    trim in wrapper mapping)
  - Pool-guard wrapper stub (maryland's `requireMarylandPool` pattern)
- **Due-list** — `createStandardStateFinanceDueListQuery`, canonical config, no
  `linkColumns`, no `mapRow`.
- **Loader** — `loadStandardStateFinanceSummariesByCandidateElection` wrapper
  (~40 lines, maryland shape — maryland IS a shared-loader wrapper since Phase 3
  cohort 0): defaults + `isEligibleElection` + `evidenceLabelTypes: ["donor"]`
  (donor evidence feeds the outside-industry explanations — #3 is in scope, so
  this stays). Characterization test via
  `tests/helpers/stateFinanceLoaderCharacterization.ts` written **up front**
  (born-shared state: the pin tests the wrapper directly).
- **Auto-link** — copy maryland (`marylandCandidateCommitteeAutoLinker.ts`).

## Schema (allocate migration number at PR time — 210 as of 2026-08-03)

`db/migrations/<next>_add_ohio_campaign_finance_tables.sql` — clone migration
127 (maryland), `oh_` prefix. Identifiers ≤63 chars: longest is
`oh_candidate_finance_direct_breakdowns_source_url_check` (55); the
outside-group-breakdowns table uses the short constraint prefix `oh_cff_`
(maryland's `md_cff_` trick). Link-source CHECK lists `'manual'` + the Ohio bulk
source. Amounts CHECK `>= 0` (canonical).

## Settled design decisions

1. **Negative cash-on-hand**: canonical schema rejects negatives; signed cash is
   a Phase-5 capability (IL/KY/LA) and Ohio does not adopt it. If a cover page
   reports a negative balance: write NULL + count in sync diagnostics. Never
   clamp to 0.
2. **Eligible offices v1** (DB-grounded at PR 1 against Ohio 2026 election
   rows): `statewide::Governor`, `statewide::Attorney General`,
   `statewide::Secretary of State`, `statewide::State Auditor`,
   `statewide::State Treasurer`, `state_upper::State Senator`,
   `state_lower::State Lower Chamber Legislator`. **No Lieutenant Governor**
   — Ohio elects it on a joint ticket, no separate election rows exist. **No
   judicial offices** — no existing state's eligible list has any
   (grep-verified 2026-08-03); Ohio judicial rows use canonical name "State
   Level Judge"; Supreme Court / Court of Appeals / Board of Education
   deferred. County/municipal excluded (report: e-filing not uniformly
   required locally).
3. **Support/oppose fail-closed**: only rows with explicit SUPPORT/OPPOSE become
   outside-spending rows (schema CHECK enforces anyway). Blank-direction rows →
   excluded-amount + row-count diagnostics. Never infer direction. No dedup of
   identical-looking expenditures (legitimately repeat).
4. **Form 31-U is two-stage** (review finding, confirm in spike): the annual
   candidate/PAC/party expenditure bulk files carry 31-U rows with spender
   MASTER_KEY, REPORT_KEY, vendor, date, amount, purpose — but reportedly NOT
   target candidate/office/direction. Those live in each report's separate
   Form 31-U detail export (`independent_expenditure.csv`). Pipeline:
   annual files → collect unique REPORT_KEYs → fetch each report's detail CSV
   → validate detail total against annual bulk + cover page (three-way
   reconciliation) → aggregate only explicit SUPPORT/OPPOSE rows.
   **Annual rows are discovery/reconciliation data only — never sum annual and
   detail amounts.** Observed volume (2026 PAC file): 43 rows, 13 report keys,
   $9.8M — detail fetch count is small.
5. **Candidate matching (31-U detail)**: exact normalized name + compatible
   office + cycle, uniquely matching one candidate — else quarantine. **No
   name-only fallback, no fuzzy links** (v1; revisit only with match-rate data
   from the live run).
6. **Occupation — fail closed** (review finding: Ohio's single combined
   `Employer/Occupation or Labor Organization` field has no prescribed
   delimiter; sampled 2024 data ~65% row coverage and contains employers, LLC
   names, even dates): emit an `occupation` breakdown row **only** when the
   whole value or exactly one component matches a versioned high-confidence
   occupation taxonomy. Ambiguous values are OMITTED from the occupation
   category — never shown as occupations; the exact reported string lives only
   in raw artifacts/diagnostics. Sync reports matched rows, matched dollars,
   omitted rows, representative omissions. **Manual precision audit gates
   enabling the feature; if precision is poor, launch Ohio without occupations
   rather than mislabel employers.** Contribution-size buckets are unaffected.
7. **Amendments**: whole-artifact refresh; rebuild each candidate snapshot from
   newest artifact set; replace, never append. Reconcile 31-U totals vs
   cover-page IE totals; quarantine mismatches beyond rounding tolerance.
8. **Flags** (free read flag ON in `backend/.env` per policy; others off):
   - `OHIO_CAMPAIGN_FINANCE_ENABLED`
   - `OHIO_CAMPAIGN_FINANCE_SYNC_ENABLED`
   - `OHIO_SOS_RAW_DATA_REFRESH_ENABLED` (house raw-refresh naming)
9. **Artifact retrieval**: portal blocks unattended HTTP (Cloudflare 403) — a
   real browser session is required; **Florida is NOT a browser precedent**
   (its client is plain fetch), though its artifact-cache/manifest code shape
   (`floridaCampaignFinanceArtifactCache.ts`) remains the template. Acquisition
   mechanism (automated browser job vs Claude-assisted manual download into the
   cache dir) is decided in the acquisition spike. Retrieval stays separate
   from parsing: download → validate → SHA-256 → atomically replace snapshot;
   finance sync reads cache only. Product download IDs must be discovered from
   file labels/years on the file-transfer page — never hardcode `P72_GETID`.
10. **Parser hardening** (review findings from live samples; build in from the
    first parser PR): Windows-1252 (not valid UTF-8), CR-only row separators,
    duplicate headers (active-candidate file has `OFFICE` twice — second is
    actually party), HTML entities (`&AMP;`), header whitespace/spelling
    drift, currency-formatted detail amounts (`"$31,550.42"`). Annual
    contribution files ≈90 MB → stream parsing + streaming SHA-256, never
    whole-file in memory.
11. **Artifact manifest fields**: product label + expected year, original
    filename, file-transfer page URL, retrieval time + portal "date modified",
    SHA-256, byte size + row count, detected encoding + row separator,
    header/schema version, min/max transaction dates, report keys found,
    validation + quarantine counts.
12. **Source label**: `"OHIO_SOS"` (agency convention: `ARIZONA_SOS`,
    `FLORIDA_DOS`, `PENNSYLVANIA_DOS`) — new member in
    `ballotLookupFinanceShared.ts` source union + `FINANCE_SUMMARY_SOURCES`
    array, registry entry in `ballotLookup.ts` state table.
13. **PDF Miscellaneous-Filings path** (nonregistered spenders: AFP etc.):
    separate `ohioSpecialFilingParser`, accept only fully-parsed rows (spender,
    candidate, direction, amount, year, filing id + URL), quarantine the rest.
    Not a v1 blocker; last PR. Electioneering mentions without explicit
    support/oppose stay out. Until it ships, source metadata must say
    registered-committee outside spending only.

## Required artifacts per cycle Y

- Active candidate list + candidate cover pages
- Candidate contributions Y−1, Y; candidate expenditures Y−1, Y
- Active PAC list + PAC cover pages; PAC expenditures Y−1, Y
- Party cover pages; party expenditures Y−1, Y
- One Form 31-U detail CSV per discovered REPORT_KEY
- For #3 (funders/industries, last feature PR): PAC contributions Y−1, Y
  (+ party contributions if needed)
- Later: Miscellaneous Filings index + candidate-related 30-B-2/30-E PDFs

## Module layout

`backend/src/pipeline/ohioFinance/`: artifact cache, rows/parsers, committee
resolver, direct-contribution aggregator, outside-spending aggregator (31-U
two-stage), outside-group contribution aggregator (tennessee
`tennesseeOutsideGroupContributionAggregator.ts` pattern — #3, last),
special-filing parser (late), writer wrapper, ballot-lookup loader wrapper,
eligible offices, auto-link, sync, batchSync, `index.ts`.

## PR sequence (one PR at a time; `npm run typecheck` + `npm test` green in `backend/`; cite matrix row)

Code-only PRs 1–2 are data-independent and proceed under the no-portal rule.
The acquisition spike is the gate before parser/aggregator work — it is the
main feasibility risk and also settles the 31-U two-stage question.

1. Migration (next free number) + writer wrapper + writer test + eligible
   offices (+ test) + feature flags.
2. Loader wrapper + characterization pin + flag-gate test + `ballotLookup.ts`
   registry + `OHIO_SOS` source union entry.
3. **Acquisition spike** (needs user authorization for portal access; no
   migration, no DB writes): prove browser retrieval works, discover + download
   annual artifacts, fetch all 31-U detail CSVs, confirm/refute decision 4,
   produce reconciliation + occupation-quality reports, capture real fixture
   samples for the parser PR.
4. Artifact cache + row parsers (encoding/row-separator/header-drift/currency
   handling per decision 10; manifests per decision 11; fixtures from spike).
5. Committee resolver (+ test) + auto-link copied from maryland (+ test).
6. Aggregators: direct contributions (occupation fail-closed per decision 6) +
   31-U two-stage outside spending with three-way reconciliation (+ tests).
7. Sync + batchSync (due-list builder config) + npm scripts
   (`ohio-candidates:finance:*`, maryland block as template).
8. **#3**: outside-group contribution aggregator (TN pattern) + PAC
   contribution artifacts + committee-label / industry-label queue entries.
9. Later, separately gated: PDF special-filings path; first live run with
   money reconciliation + match-rate report.

## Status

- [x] PR 1 schema + writer (migration 210, `oh_` tables, writer wrapper with
      numeric MASTER_KEY validation, eligible offices, 3 flags; branch
      `claude/ohio-financial-module-55919c`)
- [x] PR 2 loader + wiring (`ohioBallotLookupFinanceLoader.ts` shared-loader
      wrapper, characterization pin with Lieutenant-Governor ineligibility,
      `OHIO_SOS` source union + `FINANCE_SUMMARY_SOURCES`, `ballotLookup.ts`
      registry entry; branch `claude/ohio-finance-loader-pr2`)
- [ ] PR 3 acquisition spike (portal access required)
- [ ] PR 4 artifact cache + parsers
- [ ] PR 5 resolver + auto-link
- [ ] PR 6 aggregators (direct + 31-U)
- [ ] PR 7 sync + batchSync + scripts
- [ ] PR 8 outside-group funders/industries (#3)
- [ ] PR 9+ PDF path / live run

Update the checklist + any changed decision here as PRs land; also update the
finance-consolidation memory at campaign end.

# Illinois Local Campaign Finance Plan

*Written 2026-07-11 against `030d1ea5`. Scope: extend existing Illinois
campaign-finance support to reliable municipal offices across every Illinois
place already modeled by VoteApp. This is not a Chicago-only implementation.*

## Status update — 2026-07-12

The ID-based relation path, jurisdiction-aware local mapping, D-2 aggregation,
and normalized artifact consumer are now present on `main`. The next narrow
slice is implemented on `codex/illinois-normalized-artifact-producer`:

- a read-only producer for the six official SBE bulk files;
- exact header, row-width, and truncated-download validation;
- stable candidate/election/committee/filing joins into
  `illinois_sbe_normalized/v1`;
- official source fixtures covering Chicago Mayor/Clerk/Treasurer, Elgin,
  Inverness Village, Cicero Town, multiple committees, amendments, and a
  rejected ward race.

Still required after this slice: durable acquisition/publishing of the official
files, broader real-source fixture coverage, canonical Illinois place elections
and rosters, statewide dry-sync reconciliation, staged rollout, and monitoring.

## 1. Fresh code and data audit

### Existing path

Illinois finance already has a complete state-module skeleton:

1. `illinoisFinanceEligibleOffices.ts` maps VoteApp offices to Illinois State
   Board of Elections (SBE) labels.
2. `illinoisCandidateFinanceAutoLink.ts` finds eligible candidate/election rows
   missing finance links.
3. `illinoisCandidateCommitteeResolver.ts` searches itemized contributions and
   guesses the candidate's committee from its name.
4. `illinoisCandidateFinanceBatchSync.ts` fetches committee contributions and
   candidate independent expenditures.
5. `illinoisFinanceAggregators.ts` builds direct and outside-spending totals and
   breakdowns.
6. `illinoisFinanceWriter.ts` replaces a candidate/election snapshot.
7. `illinoisBallotLookupFinanceLoader.ts` combines active committee links into
   the shared API/UI finance shape.
8. A BullMQ scheduler runs the batch path behind Illinois read/sync flags.

The writer, shared finance response, UI, feature flags, and most scheduler
plumbing can remain. The source identity and summary logic cannot.

### Current local database

Read-only audit on 2026-07-11:

- 1,445 Illinois `place` districts exist.
- Zero Illinois `place` elections exist.
- Zero `il_candidate_finance_links` and zero Illinois finance summaries exist.
- Place office catalog has Mayor, City Clerk, City Treasurer, Municipal
  Assessor, Alderman, City Council Member, and Town Council Member, but:
  - `Alderman` has no aliases.
  - Village/Town President aliases are absent.
  - Village/Town Trustee has no accurate canonical office.

Finance work therefore does not create coverage by itself. Eligible local
elections and candidate rosters must enter the normal election pipeline first.
Finance sync should consume them, never create parallel elections/candidates.

### Correctness gaps that block local expansion

1. **Committee resolution is a name guess.** The resolver accepts a committee
   only when its name contains candidate-name tokens. This rejects official
   relationships such as slate/party-style committees and can choose the wrong
   same-name committee. SBE candidate detail explicitly links candidates to one
   or more committees; that relation must be authoritative.
2. **No stable external IDs.** `committee_key` is currently a normalized name,
   despite SBE assigning committee IDs. Renames and similarly named committees
   can merge or split incorrectly.
3. **No local jurisdiction identity.** A finance link stores office and a
   generic `district`, but current place flow supplies no City/Village/Town and
   municipality. `Mayor + candidate name` is not unique statewide. Outside
   expenditure artifact matching currently checks only candidate name and an
   office substring.
4. **Displayed receipts are not totals.** `aggregateIllinoisDirectContributions`
   sums searchable itemized rows and calls that `totalReceipts` and
   `directContributionTotal`. SBE says receipts of $150 or less are not
   itemized. D-2 summary reports are required for real receipts, expenditures,
   cash, and debts.
5. **Every receipt type becomes a “contribution.”** Production requests `All
   Types`; transfers, loans, other receipts, and in-kind receipts currently feed
   contribution-size and occupation aggregates. Breakdowns must be limited to
   the applicable itemized category.
6. **Spent/cash are permanently null.** Schema has `total_disbursements` and
   `cash_on_hand`, but sync never supplies them. Schema lacks debts even though
   shared API supports `debts_owed`.
7. **Multiple committees need deliberate behavior.** Schema and loader support
   multiple links, which is good. Resolver currently returns only one “winner.”
   Explicit candidate detail can list several committees. Loader sums committee
   summaries, so transfers between a candidate's own committees can be counted
   twice unless totals semantics are documented or deduplicated.
8. **Live SBE export is not production-reliable.** Client emulates ASP.NET
   postbacks and can fail before the CSV download list. Code itself tells the
   operator to use artifact mode. Scheduler artifact mode accepts static local
   paths, which is not yet a durable daily ingestion strategy.
9. **Cap handling only warns.** Partition helpers exist but the batch loader
   does not execute them; a 25,000-row export can still write a partial snapshot.
10. **Eligibility is duplicated.** Office keys live in both
    `illinoisFinanceEligibleOffices.ts` and
    `illinoisBallotLookupFinanceLoader.ts`, inviting drift.
11. **Election cycle is approximate.** Current filter is January 1 of the prior
    year through December 31 of election year. Keep this initially, but label it
    as the display cycle and test odd-year municipal elections explicitly.

Official basis: SBE's [search documentation](https://www.elections.il.gov/campaigndisclosure/searchoptions.aspx)
states that candidate records lead to formed committees, itemized receipts omit
small receipts, and committee-summary searches carry totals. SBE's
[D-2 documentation](https://www.elections.il.gov/campaigndisclosure/politicalcommittee.aspx)
defines D-2 as the summary for receipts and expenditures. SBE also publishes a
`Downloadable Campaign Disclosure Data` entry from its
[Campaign Disclosure menu](https://www.elections.il.gov/CampaignDisclosure.aspx?MID=rfZ%2BuidMSDY%3D).

## 2. Supported local scope

Do not hardcode Chicago, Aurora, Springfield, or any city list. Eligibility is
the intersection of:

- VoteApp election district is an Illinois Census `place`;
- SBE candidate relation identifies district type `City`, `Village`, or `Town`;
- normalized SBE district name matches the VoteApp place name;
- SBE office maps through this explicit allowlist;
- legislative seats have explicit place-wide/at-large evidence and no
  ward/district marker;
- candidate identity and election year match;
- one or more official committee relations are present.

### Ship after the source/correctness foundation

| SBE labels | VoteApp canonical office | Rule |
|---|---|---|
| Mayor | Mayor | City/Village/Town place-wide |
| President | Mayor | Village/Town only; never bare City `President` |
| Clerk | City Clerk | City/Village/Town place-wide |
| Treasurer | City Treasurer | City/Village/Town place-wide |
| Assessor | Municipal Assessor | City/Village/Town place-wide |
| Alderman, Alderperson | Alderman | City/Village/Town plus explicit at-large/place-wide evidence; never Ward |
| Councilman, Councilperson, City Council | City Council Member | City/Village/Town plus explicit at-large/place-wide evidence |
| Trustee | Municipal Trustee (new) | Village/Town place-wide |

This covers all reliable source-backed cities, villages, and towns automatically
as VoteApp elections arrive. Chicago gets Mayor, Clerk, and Treasurer now;
Chicago alderpersons remain excluded because SBE classifies them by ward.

### Deliberately excluded

- `Ward` candidates, including Chicago alderpersons: address resolver has no
  ward layer. Attaching them to Chicago `place` would show every ward race to
  every resident.
- Board of Education and Police District Council: sub-place geographies.
- Library Board: often a distinct taxing district.
- Collector, Tax Collector, Supervisor, Supervisor of General Assistance:
  township/coterminous-government boundaries are not represented by Census
  incorporated-place resolution.
- Committee Person: party office, not a municipal governing office.
- Commissioner: source label is jurisdiction-dependent and too ambiguous for a
  statewide generic mapping. Add only after a separate title+jurisdiction proof.
- Judges and municipal attorneys/constables: no sufficiently verified SBE
  office mapping from this research pass.

## 3. Source strategy

### Authority order

1. **Canonical:** official SBE downloadable data, candidate detail, committee
   detail, D-2 summaries, contributions, and independent expenditures.
2. **Transport fallback/cross-check:** [Illinois Sunshine](https://www.illinoissunshine.org/api-documentation/),
   which documents candidate, committee, receipt, expenditure, filing, and
   candidate/committee relationship data derived from SBE. Use only after a
   production connectivity probe and review of its attribution/usage terms.
3. Never use Illinois Sunshine to override conflicting official SBE identity or
   totals. Record transport source and official underlying source separately.

### One normalized artifact contract

Keep source acquisition separate from finance computation. Both official bulk
files and an approved mirror adapter should produce one versioned dataset:

- candidates/candidacies: SBE candidate ID, name, district type, district,
  office, party, election year/type/result, source URL;
- candidate↔committee relations: SBE candidate ID, SBE committee ID, committee
  name/status/type, source URL;
- D-2 report summaries: committee ID, reporting period, filed/amended timestamp,
  report identity, receipts, expenditures, ending cash, debts, source URL;
- itemized receipts: stable row/document ID, committee ID, D-2 part/type, date,
  amount, donor fields, occupation/employer, source URL;
- independent expenditures: stable row/document ID, spender committee ID,
  candidate, office, district, support/oppose, date, amount, source URL;
- manifest: schema version, acquired-at, source type, source URLs, file hashes,
  row counts, and whether every required file completed.

This is a small adapter boundary, not a new generic finance framework. Existing
Illinois aggregators/writer consume normalized records.

## 4. Phased implementation

### Phase 0 — prove transport and freeze fixtures (no DB writes, no feature expansion)

1. Add a read-only probe for official downloadable SBE data and Illinois
   Sunshine bulk/API fallback. Run from the deployed backend environment, not
   only locally. Measure HTTP status, freshness, row counts, pagination/caps,
   and latency.
2. Capture official fixtures for at least:
   - Chicago Mayor/Clerk/Treasurer;
   - Aurora Mayor plus an at-large Alderperson/Council member;
   - one Village President, Clerk, and Trustee;
   - one Town Mayor/President, Clerk/Treasurer/Assessor, and Trustee;
   - a candidate with multiple committees;
   - a candidate whose committee name lacks candidate-name tokens;
   - a same-name candidate or same office in different municipalities;
   - a Ward alderperson that must be rejected;
   - an amended D-2 report.
3. Implement and test normalized artifact parsers only. Unknown schema/header,
   incomplete manifest, cap, duplicate stable ID, or conflicting amendment must
   fail closed.

**Gate:** one source transport is repeatable in production and fixtures prove
candidate ID → candidacy → committee ID → filing/transactions. Otherwise stop;
do not widen office allowlists around the old name matcher.

### Phase 1 — fix office catalog and jurisdiction-aware identity

1. Add `place::Municipal Trustee` to `seedOffices.ts`, migration, and curated
   research-area mapping. Add aliases:
   - Mayor: `Village President`, `Town President`;
   - Alderman: `Alderman`, `Alderperson`;
   - City Council Member: `Councilman`, `Councilperson`, `City Council`;
   - Municipal Trustee: `Trustee`, `Village Trustee`, `Town Trustee`.
   Avoid bare `President` in the global office-title alias table; map SBE
   `President` only with Village/Town context.
2. Replace singular office mapping with a jurisdiction-aware mapping containing
   accepted SBE district types and labels. Export one allowlist and import it in
   auto-link, batch sync, and ballot loader; remove duplicate UI allowlist.
3. Migration for finance identity:
   - add nullable `sbe_candidate_id`;
   - add nullable `sbe_district_type`;
   - add nullable `sbe_office`;
   - use existing `district` for exact SBE district name/number;
   - store `SBE:<committee-id>` in `committee_key` for new links and use
     `committee_name` only for display/legacy CSV matching;
   - keep normalized-name legacy links readable during rollout.
4. Extend due-row query with VoteApp district name/type. Normalize municipality
   suffixes (`City`, `Village`, `Town`) conservatively and require exact
   post-normalization match; no fuzzy city matching.
5. Replace `searchAndResolveIllinoisCandidateCommittee` for new links with an
   explicit relation resolver. Return zero, one, or many linked committees.
   Name matching remains only a manual diagnostic/fallback and may not activate
   a link automatically.
6. Add an `unmatched_reason`/structured result for: no official candidacy,
   jurisdiction mismatch, office mismatch, ambiguous candidate, no committee,
   incomplete source. Do not persist speculative inactive links.

**Gate:** fixtures link all intended local examples by IDs, reject Ward and
cross-city examples, and preserve existing statewide/legislative behavior.

### Phase 2 — make money fields truthful

1. Add D-2 normalized records and aggregation:
   - choose latest valid amendment per committee + reporting period;
   - sum period receipts and expenditures within the display cycle;
   - take cash and debt from latest report ending in the cycle;
   - never sum an original report with its amendment.
2. Add `debts_owed` to Illinois summaries and writer/loader. Populate existing
   `total_disbursements` and `cash_on_hand`.
3. API mapping for Illinois:
   - `total_raised` = D-2 total receipts;
   - `total_spent` = D-2 expenditures;
   - `cash_on_hand` = latest D-2 ending cash;
   - `debts_owed` = latest D-2 debts.
4. Itemized rows power breakdowns only:
   - occupation: itemized individual receipts with a reported occupation;
   - contribution-size buckets: itemized individual contributions only;
   - transfers, loans, other receipts, and in-kind receipts do not masquerade
     as individual contribution buckets;
   - zero/negative/amended-away rows are handled by stable filing/row identity,
     not dropped solely because amount is non-positive.
5. For candidates with multiple official committees, preserve per-committee
   summaries and sum only for display. Detect transfers between linked
   committees and subtract them once from combined receipts/spending, or, if
   source fields cannot prove both sides, expose committee totals separately and
   omit a combined `total_raised` rather than publish a known double count.
6. A capped or partial source run must not replace the last complete snapshot.

**Gate:** fixture totals reconcile exactly to amended D-2 reports; itemized
breakdowns can be lower than total receipts without changing totals.

### Phase 3 — local outside spending and all-place batch rollout

1. Build independent-expenditure search/filter from explicit SBE candidacy:
   candidate ID/name + exact SBE office + district type + municipality + cycle.
   Never rely on office substring alone.
2. Deduplicate support/oppose rows by stable SBE expenditure/document ID before
   aggregation. Fetch outside-group funders by committee ID.
3. Update artifact mode to select the newest complete versioned manifest from a
   configured artifact directory/object-store location. Do not put one-off local
   CSV paths into a daily scheduler definition.
4. Keep live ASP.NET client as a probe/manual fallback until it proves reliable;
   production scheduler uses normalized artifacts.
5. Run dry-run against every eligible Illinois candidate/election. Report counts
   by office, municipality, linked/unlinked reason, multiple committees, missing
   D-2, incomplete/capped source, and outside-spending match.
6. Enable in waves with the existing master/read/sync flags:
   - A: Mayor, City Clerk, City Treasurer;
   - B: Village/Town President, Clerk, Treasurer, Municipal Assessor;
   - C: jurisdiction-proven at-large Alderman/Alderperson, Council, Trustee.
   Waves are office gates, not city allowlists; each wave applies statewide.

**Gate:** sampled Chicago, other city, village, and town API responses reconcile
to official candidate/committee/D-2 pages; Ward candidates remain absent.

### Phase 4 — election coverage, operations, and cleanup

1. Run normal election discovery/roster workflow for Illinois place districts
   with upcoming actionable elections. Finance sync follows those canonical
   elections. Do not bulk-create election shells from finance data.
2. Add sync observability:
   source age, artifact version, eligible candidates, relation match rate,
   jurisdiction rejections, missing filings, last complete sync, cap/partial
   rejection, and per-office coverage.
3. Add source attribution in finance response when mirror transport is used,
   while retaining official SBE source links for facts.
4. After migration adoption, convert or retire legacy normalized-name
   `committee_key` rows. Remove automatic name matcher only after no active link
   depends on it.
5. Document operator procedure for artifact acquisition, validation, dry-run,
   activation, rollback (disable flags; retain last complete snapshot), and
   source outage behavior.

## 5. Tests and verification

### Unit/contract

- Office mapping matrix for every included/excluded label and district type.
- Municipality normalization; Springfield must never match a different
  Springfield-like jurisdiction.
- Explicit candidate/committee relation: zero/one/many committees, renames,
  slate committee, same-name candidates.
- D-2 original/amendment/final selection and period aggregation.
- Receipt-category filtering and stable-row deduplication.
- Ward/subdistrict rejection.
- Multiple-linked-committee transfer behavior.
- Artifact schema version, checksum, cap, partial, and stale-source failure.

### DB/integration

- Migration constraints and legacy link compatibility.
- Auto-link and due queries include local jurisdiction fields and one shared
  allowlist.
- Snapshot replacement remains transactional and retains last good data after a
  failed/partial run.
- Ballot loader maps receipts/spending/cash/debt and combines committees under
  the chosen tested rule.
- Feature flags still hide reads and stop scheduled writes independently.

### Live acceptance sample

- Chicago: Mayor, Clerk, Treasurer; no alderperson without ward support.
- One non-Chicago city: executive/admin plus at-large legislative seat.
- One village: President, Clerk, Trustee.
- One town: Mayor/President plus at least one supported admin/Trustee office.
- Candidate with slate-style committee and candidate with multiple committees.
- Candidate with no committee/under filing threshold: finance remains absent,
  not a zero-dollar claim.

## 6. Non-goals

- Municipal ward/address resolution.
- School, library, police, township, or other special-district geography.
- Creating elections or candidate rosters from finance filings.
- Browser automation or anti-bot workarounds in production.
- A cross-state finance rewrite.
- Showing zero when no filing exists. Illinois filing threshold means absence of
  data is not evidence of zero fundraising/spending.

## 7. Recommended PR sequence

1. `probe/fixtures + normalized Illinois artifact contract`
2. `office catalog + jurisdiction-aware eligibility + ID relation resolver`
3. `D-2 totals + receipt correctness + debts`
4. `local outside spending + durable artifact scheduler`
5. `statewide local-office rollout + observability/runbook`

Each PR is independently testable. No local office reaches UI before PRs 2 and
3 land; no scheduler-wide expansion before PR 4 proves complete artifacts.

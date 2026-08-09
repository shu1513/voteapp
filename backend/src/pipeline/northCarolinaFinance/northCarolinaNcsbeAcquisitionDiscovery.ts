import type { Pool, PoolClient } from "pg";

import { northCarolinaCommitteeSearchQueryForCandidateName } from "./northCarolinaCandidateFinanceBatchSync.js";
import { resolveNorthCarolinaCandidateCommittee } from "./northCarolinaCandidateCommitteeResolver.js";
import { NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./northCarolinaFinanceEligibleOffices.js";
import {
  acquireNcsbeCommitteeArtifacts,
  acquireNcsbeCycleArtifacts,
  type NcsbeAcquisitionCommittee,
  type NcsbeAcquisitionResult,
  type NcsbeCommitteeAcquisitionResult,
} from "./northCarolinaNcsbeArtifactAcquisition.js";
import {
  getNcsbeArtifactStatus,
  readNcsbeArtifact,
  storeNcsbeArtifact,
} from "./northCarolinaNcsbeArtifactCache.js";
import { fetchNcsbeCommitteeSearch, requireNcsbeYear, type NcsbeTransport } from "./northCarolinaNcsbeClient.js";
import { parseNcsbeCommitteeSearchPage, parseNcsbeDocumentListPage } from "./northCarolinaNcsbeParsers.js";

// Acquisition-side discovery for the NCSBE portal (north_carolina_plan.md,
// the "owed before PR 9's live run" work): instead of explicit
// `--committee <SBoEID>:<OGID>` args, the acquisition discovers what to fetch
// from the roster and from the cached IE inventories.
//
// - Roster-driven committee searches: one portal committee search per roster
//   candidate name, cached under the SAME key the sync's auto-link loader
//   reads (`northCarolinaCommitteeSearchQueryForCandidateName`), so auto-link
//   always has evidence to resolve against.
// - Committee list: active links' SBoEIDs with OGID derived from the cached
//   searches (exact-SBoEID filter; committee-name search fallback covers
//   manual links whose committee never appears under the candidate's name),
//   plus resolver matches for still-unlinked candidates — the same pure
//   resolver auto-link runs later, so the first sync after a fresh pull can
//   aggregate immediately instead of waiting for a second acquisition.
// - Registered IE spenders (PR 8's funder leg): discovered from the cached IE
//   doc-type inventories (structured rows with a real SBoEID), OGID derived
//   the same way, then their document inventories + cycle reports pulled so
//   funder legs stop being honestly unavailable.
//
// The `NC-OGID:` identity upgrade for unregistered IE filers is deliberately
// NOT implemented here: the `NC-IE-FILER:` hash key already carries stable,
// source-scoped identity, per-candidate outside TOTALS are unaffected by how
// groups split, and rekeying would rewrite the settled PR 6/8 group-identity
// joins for a cosmetic dedup. It stays a recorded future upgrade.
//
// Only the roster query touches the database (read-only). Everything else is
// portal + cache, matching the acquisition module's contract.

type Queryable = Pick<Pool | PoolClient, "query">;

export type NorthCarolinaAcquisitionRosterRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  // Active finance link, when one exists. A candidate election with several
  // active links yields one roster row per link.
  linkedCommitteeId: string | null;
  linkedCommitteeName: string | null;
};

type RosterQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  linked_committee_id: string | null;
  linked_committee_name: string | null;
};

// Every eligible NC candidate election in the cycle window (Y−1..Y by
// election date), linked or not — unlike the auto-linker's due list, linked
// rows are needed too, because their committees are what the acquisition
// must pull. Withdrawn/lost candidacies are excluded: they need no artifact
// pull, and the sync's outside-target UNIVERSE query does its own wider scan.
export async function listNorthCarolinaAcquisitionRoster(
  db: Queryable,
  input: { cycleYear: number }
): Promise<NorthCarolinaAcquisitionRosterRow[]> {
  const cycleYear = requireNcsbeYear(input.cycleYear);
  const result = await db.query<RosterQueryRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        election.id::text AS election_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
        ) AS candidate_name,
        extract(year from election.election_date)::int AS election_year,
        office.scope AS office_scope,
        COALESCE(NULLIF(trim(office.canonical_name), ''), election.official_ballot_title) AS office_name,
        CASE
          WHEN district.district_type IN ('state_upper', 'state_lower') THEN
            NULLIF(
              regexp_replace(
                substring(district.geoid_compact from char_length(district.state_fips) + 1),
                '^0+',
                ''
              ),
              ''
            )
          ELSE NULL
        END AS district,
        link.committee_id AS linked_committee_id,
        link.committee_name AS linked_committee_name
      FROM public.candidate_elections AS candidate_election
      JOIN public.candidates AS candidate
        ON candidate.id = candidate_election.candidate_id
      JOIN public.elections AS election
        ON election.id = candidate_election.election_id
      JOIN public.districts AS district
        ON district.id = election.district_id
      LEFT JOIN public.offices AS office
        ON office.id = election.office_id
      LEFT JOIN public.nc_candidate_finance_links AS link
        ON link.candidate_id = candidate.id
        AND link.election_id = election.id
        AND link.link_status = 'active'
      WHERE candidate.deleted_at IS NULL
        AND district.state = 'NC'
        AND election.race_type = 'office'
        AND election.election_date >= make_date($1::int - 1, 1, 1)
        AND election.election_date < make_date($1::int + 1, 1, 1)
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($2::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
      ORDER BY election.election_date ASC, election.id ASC, candidate.id ASC
    `,
    [cycleYear, [...NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS]]
  );

  return result.rows.map((row) => ({
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    linkedCommitteeId: row.linked_committee_id,
    linkedCommitteeName: row.linked_committee_name,
  }));
}

type CommitteeSearchLoad = {
  rows: ReturnType<typeof parseNcsbeCommitteeSearchPage>;
  url: string;
  fetched: boolean;
};

// Fetch-or-cache for one committee-search query. When `refresh` is set the
// portal is asked again; a fetch failure falls back to a ready cached copy
// (recorded by the caller) rather than dropping evidence that still exists.
async function loadOrFetchCommitteeSearch(input: {
  transport: NcsbeTransport;
  cacheDir: string;
  query: string;
  refresh: boolean;
  retrievedAt?: Date;
}): Promise<CommitteeSearchLoad & { fetchFailure: string | null }> {
  const key = { type: "committee_search", query: input.query } as const;
  const cached = await getNcsbeArtifactStatus({ cacheDir: input.cacheDir, key });
  const cachedLoad: CommitteeSearchLoad | null =
    cached.status === "ready" && cached.body !== null && cached.manifest !== null
      ? { rows: parseNcsbeCommitteeSearchPage(cached.body), url: cached.manifest.url, fetched: false }
      : null;

  if (!input.refresh && cachedLoad !== null) {
    return { ...cachedLoad, fetchFailure: null };
  }

  try {
    const fetched = await fetchNcsbeCommitteeSearch(input.transport, input.query);
    await storeNcsbeArtifact({
      cacheDir: input.cacheDir,
      key,
      url: fetched.url,
      body: fetched.body,
      retrievedAt: input.retrievedAt,
    });
    return { rows: fetched.parsed, url: fetched.url, fetched: true, fetchFailure: null };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (cachedLoad !== null) {
      return { ...cachedLoad, fetchFailure: message };
    }
    throw error;
  }
}

export type NcsbeCommitteeDiscoveryResult = {
  committees: NcsbeAcquisitionCommittee[];
  rosterRowCount: number;
  searchQueryCount: number;
  searchesFetched: number;
  searchesFromCache: number;
  searchFailures: Array<{ query: string; message: string }>;
  resolverMatchedCount: number;
  resolverUnmatchedCount: number;
  resolverAmbiguousCount: number;
  linkedOgidResolvedCount: number;
  // Committees that could not be given an OGID (or whose searches disagreed
  // on it) — skipped from the pull, never guessed.
  ogidFailures: Array<{ sboeId: string; message: string }>;
};

// Search per roster candidate + committee list with OGIDs. Search refresh
// rule: a query is re-fetched when forced, missing from the cache, or any of
// its roster rows still lacks an active link — an unlinked candidate's
// committee may have registered since the last pull, while a linked
// candidate's cached search stays valid evidence.
export async function discoverNcsbeAcquisitionCommittees(input: {
  transport: NcsbeTransport;
  cacheDir: string;
  roster: readonly NorthCarolinaAcquisitionRosterRow[];
  force?: boolean;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<NcsbeCommitteeDiscoveryResult> {
  const result: NcsbeCommitteeDiscoveryResult = {
    committees: [],
    rosterRowCount: input.roster.length,
    searchQueryCount: 0,
    searchesFetched: 0,
    searchesFromCache: 0,
    searchFailures: [],
    resolverMatchedCount: 0,
    resolverUnmatchedCount: 0,
    resolverAmbiguousCount: 0,
    linkedOgidResolvedCount: 0,
    ogidFailures: [],
  };

  // One search per distinct trimmed candidate name.
  const rowsByQuery = new Map<string, NorthCarolinaAcquisitionRosterRow[]>();
  for (const row of input.roster) {
    const query = northCarolinaCommitteeSearchQueryForCandidateName(row.candidateName);
    const rows = rowsByQuery.get(query) ?? [];
    rows.push(row);
    rowsByQuery.set(query, rows);
  }
  result.searchQueryCount = rowsByQuery.size;

  const searchByQuery = new Map<string, CommitteeSearchLoad>();
  for (const [query, rows] of rowsByQuery) {
    const refresh = (input.force ?? false) || rows.some((row) => row.linkedCommitteeId === null);
    try {
      const load = await loadOrFetchCommitteeSearch({
        transport: input.transport,
        cacheDir: input.cacheDir,
        query,
        refresh,
        retrievedAt: input.retrievedAt,
      });
      searchByQuery.set(query, load);
      if (load.fetched) {
        result.searchesFetched += 1;
      } else {
        result.searchesFromCache += 1;
      }
      if (load.fetchFailure !== null) {
        result.searchFailures.push({ query, message: load.fetchFailure });
      }
      input.log?.(
        `Committee search ${JSON.stringify(query)}: ${load.rows.length} rows` +
          (load.fetched ? "" : " (cached)")
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      result.searchFailures.push({ query, message });
      input.log?.(`Committee search ${JSON.stringify(query)}: FAILED — ${message}`);
    }
  }

  // Exact-SBoEID OGID lookup over a search's rows; searches can disagree only
  // by returning nothing, never by returning two OGIDs for one SBoEID row set
  // — a disagreement is treated as no answer plus a recorded failure.
  const ogidBySboeId = new Map<string, number>();
  const conflictedSboeIds = new Set<string>();
  const recordOgid = (sboeId: string, orgGroupId: number): void => {
    const existing = ogidBySboeId.get(sboeId);
    if (existing !== undefined && existing !== orgGroupId) {
      conflictedSboeIds.add(sboeId);
      return;
    }
    ogidBySboeId.set(sboeId, orgGroupId);
  };
  const lookupOgidInRows = (rows: CommitteeSearchLoad["rows"], sboeId: string): void => {
    for (const row of rows) {
      if (row.sboeId !== null && row.sboeId.toUpperCase() === sboeId) {
        recordOgid(sboeId, row.orgGroupId);
      }
    }
  };

  const discovered = new Map<string, NcsbeAcquisitionCommittee>();
  const addCommittee = (sboeId: string, orgGroupId: number): void => {
    if (!discovered.has(sboeId)) {
      discovered.set(sboeId, { sboeId, orgGroupId });
    }
  };

  // Linked rows first: derive OGID from the candidate's search, then fall
  // back to a committee-name search (covers manual links whose committee the
  // candidate-name search never returns).
  const linkedNeedingOgid = new Map<string, { committeeName: string | null }>();
  for (const [query, rows] of rowsByQuery) {
    const search = searchByQuery.get(query);
    for (const row of rows) {
      if (row.linkedCommitteeId === null) {
        continue;
      }
      const sboeId = row.linkedCommitteeId.trim().toUpperCase();
      if (search !== undefined) {
        lookupOgidInRows(search.rows, sboeId);
      }
      if (!ogidBySboeId.has(sboeId) && !linkedNeedingOgid.has(sboeId)) {
        linkedNeedingOgid.set(sboeId, { committeeName: row.linkedCommitteeName });
      }
    }
  }
  for (const [sboeId, { committeeName }] of linkedNeedingOgid) {
    if (ogidBySboeId.has(sboeId) || conflictedSboeIds.has(sboeId)) {
      continue;
    }
    const nameQuery = committeeName?.trim() ?? "";
    if (nameQuery.length === 0) {
      continue;
    }
    try {
      const load = await loadOrFetchCommitteeSearch({
        transport: input.transport,
        cacheDir: input.cacheDir,
        query: nameQuery,
        refresh: input.force ?? false,
        retrievedAt: input.retrievedAt,
      });
      if (load.fetched) {
        result.searchesFetched += 1;
      } else {
        result.searchesFromCache += 1;
      }
      if (load.fetchFailure !== null) {
        result.searchFailures.push({ query: nameQuery, message: load.fetchFailure });
      }
      lookupOgidInRows(load.rows, sboeId);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      result.searchFailures.push({ query: nameQuery, message });
    }
  }
  for (const rows of rowsByQuery.values()) {
    for (const row of rows) {
      if (row.linkedCommitteeId === null) {
        continue;
      }
      const sboeId = row.linkedCommitteeId.trim().toUpperCase();
      if (conflictedSboeIds.has(sboeId)) {
        if (!result.ogidFailures.some((failure) => failure.sboeId === sboeId)) {
          result.ogidFailures.push({
            sboeId,
            message: "committee searches disagree on this SBoEID's OrgGroupID",
          });
        }
        continue;
      }
      const orgGroupId = ogidBySboeId.get(sboeId);
      if (orgGroupId === undefined) {
        if (!result.ogidFailures.some((failure) => failure.sboeId === sboeId)) {
          result.ogidFailures.push({
            sboeId,
            message: "no committee-search row carries this SBoEID, so its OrgGroupID is unknown",
          });
        }
        continue;
      }
      if (!discovered.has(sboeId)) {
        result.linkedOgidResolvedCount += 1;
      }
      addCommittee(sboeId, orgGroupId);
    }
  }

  // Unlinked rows: the same pure resolver auto-link runs later. A match here
  // pre-pulls the committee so the first sync can aggregate immediately.
  for (const [query, rows] of rowsByQuery) {
    const search = searchByQuery.get(query);
    if (search === undefined) {
      continue;
    }
    for (const row of rows) {
      if (row.linkedCommitteeId !== null) {
        continue;
      }
      const resolution = resolveNorthCarolinaCandidateCommittee({
        candidateName: row.candidateName,
        officeScope: row.officeScope,
        officeName: row.officeName,
        electionYear: row.electionYear,
        district: row.district,
        searchRows: search.rows,
        sourceUrl: search.url,
      });
      if (resolution.status === "matched") {
        result.resolverMatchedCount += 1;
        addCommittee(resolution.committeeId, resolution.orgGroupId);
      } else if (resolution.status === "ambiguous") {
        result.resolverAmbiguousCount += 1;
      } else {
        result.resolverUnmatchedCount += 1;
      }
    }
  }

  result.committees = [...discovered.values()];
  return result;
}

export type NcsbeRegisteredSpender = {
  sboeId: string;
  committeeName: string;
};

// Registered IE spenders from the cached IE doc-type inventories: structured
// rows (dataLink present) with a real SBoEID — exactly the rows whose reports
// the outside aggregator can turn into SBoEID-keyed groups. Fail-closed: a
// missing inventory throws, because a silent partial spender list would leave
// funder legs unavailable with no explanation.
export async function discoverNcsbeRegisteredSpenders(input: {
  cacheDir: string;
  cycleYear: number;
}): Promise<{ spenders: NcsbeRegisteredSpender[]; inventoryYears: number[] }> {
  const cycleYear = requireNcsbeYear(input.cycleYear);
  const years = [cycleYear - 1, cycleYear];
  const spendersBySboeId = new Map<string, NcsbeRegisteredSpender>();
  for (const year of years) {
    const { body } = await readNcsbeArtifact({
      cacheDir: input.cacheDir,
      key: { type: "ie_doc_type_inventory", year },
    });
    for (const row of parseNcsbeDocumentListPage(body)) {
      if (row.dataLink === null || row.sboeId === null) {
        continue;
      }
      const sboeId = row.sboeId.toUpperCase();
      if (!spendersBySboeId.has(sboeId)) {
        spendersBySboeId.set(sboeId, { sboeId, committeeName: row.committeeName });
      }
    }
  }
  return { spenders: [...spendersBySboeId.values()], inventoryYears: years };
}

export type NcsbeSpenderAcquisitionResult = {
  discoveredSpenderCount: number;
  skippedAlreadyAcquired: string[];
  committees: NcsbeCommitteeAcquisitionResult[];
  failures: Array<{ sboeId: string; message: string }>;
};

// Pulls each registered spender's document inventory + cycle reports (the
// funder leg's receipt source). OGID comes from a committee-name search with
// the exact-SBoEID filter. Per-spender isolation: one failing spender leaves
// that year's funder leg honestly unavailable, never abandons the rest.
export async function acquireNcsbeSpenderArtifacts(input: {
  transport: NcsbeTransport;
  cacheDir: string;
  cycleYear: number;
  spenders: readonly NcsbeRegisteredSpender[];
  alreadyAcquiredSboeIds?: ReadonlySet<string>;
  force?: boolean;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<NcsbeSpenderAcquisitionResult> {
  const result: NcsbeSpenderAcquisitionResult = {
    discoveredSpenderCount: input.spenders.length,
    skippedAlreadyAcquired: [],
    committees: [],
    failures: [],
  };
  for (const spender of input.spenders) {
    if (input.alreadyAcquiredSboeIds?.has(spender.sboeId)) {
      result.skippedAlreadyAcquired.push(spender.sboeId);
      continue;
    }
    try {
      const nameQuery = spender.committeeName.trim();
      if (nameQuery.length === 0) {
        throw new Error("spender committee name is empty, so no search can derive its OrgGroupID");
      }
      const load = await loadOrFetchCommitteeSearch({
        transport: input.transport,
        cacheDir: input.cacheDir,
        query: nameQuery,
        refresh: input.force ?? false,
        retrievedAt: input.retrievedAt,
      });
      const matches = new Set(
        load.rows
          .filter((row) => row.sboeId !== null && row.sboeId.toUpperCase() === spender.sboeId)
          .map((row) => row.orgGroupId)
      );
      if (matches.size !== 1) {
        throw new Error(
          `committee search ${JSON.stringify(nameQuery)} yields ${matches.size} OrgGroupIDs for ` +
            `SBoEID ${spender.sboeId}`
        );
      }
      const [orgGroupId] = matches;
      result.committees.push(
        await acquireNcsbeCommitteeArtifacts({
          transport: input.transport,
          cacheDir: input.cacheDir,
          cycleYear: input.cycleYear,
          committee: { sboeId: spender.sboeId, orgGroupId: orgGroupId! },
          force: input.force,
          retrievedAt: input.retrievedAt,
          log: input.log,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      result.failures.push({ sboeId: spender.sboeId, message });
      input.log?.(`Spender ${spender.sboeId}: FAILED — ${message}`);
    }
  }
  return result;
}

export type NcsbeRosterAcquisitionResult = {
  discovery: NcsbeCommitteeDiscoveryResult;
  acquisition: NcsbeAcquisitionResult;
  spenders: NcsbeSpenderAcquisitionResult | null;
  // Set when spender DISCOVERY itself failed (an unreadable IE inventory);
  // committee + IE results above are preserved.
  spenderDiscoveryFailure: { message: string } | null;
};

// Full roster-driven cycle pull: searches -> committee list -> committee +
// IE artifacts -> registered-spender artifacts. `extraCommittees` (explicit
// --committee args) union with the discovered set; the spender phase runs
// after IE so it reads inventories this run just stored. Skipping IE skips
// the spender phase with it — spenders only exist because of IE filings, and
// a skip must neither pull spender reports off stale cached inventories nor
// report a "failure" for a phase the caller asked not to run.
export async function acquireNcsbeRosterCycleArtifacts(input: {
  transport: NcsbeTransport;
  cacheDir: string;
  cycleYear: number;
  roster: readonly NorthCarolinaAcquisitionRosterRow[];
  extraCommittees?: readonly NcsbeAcquisitionCommittee[];
  includeIe?: boolean;
  force?: boolean;
  retrievedAt?: Date;
  log?: (message: string) => void;
}): Promise<NcsbeRosterAcquisitionResult> {
  const discovery = await discoverNcsbeAcquisitionCommittees({
    transport: input.transport,
    cacheDir: input.cacheDir,
    roster: input.roster,
    force: input.force,
    retrievedAt: input.retrievedAt,
    log: input.log,
  });

  const committeesBySboeId = new Map<string, NcsbeAcquisitionCommittee>();
  for (const committee of [...discovery.committees, ...(input.extraCommittees ?? [])]) {
    const sboeId = committee.sboeId.trim().toUpperCase();
    if (!committeesBySboeId.has(sboeId)) {
      committeesBySboeId.set(sboeId, { sboeId, orgGroupId: committee.orgGroupId });
    }
  }

  const acquisition = await acquireNcsbeCycleArtifacts({
    transport: input.transport,
    cacheDir: input.cacheDir,
    cycleYear: input.cycleYear,
    committees: [...committeesBySboeId.values()],
    includeIe: input.includeIe,
    force: input.force,
    retrievedAt: input.retrievedAt,
    log: input.log,
  });

  let spenders: NcsbeSpenderAcquisitionResult | null = null;
  let spenderDiscoveryFailure: { message: string } | null = null;
  if (input.includeIe ?? true) {
    try {
      const discoveredSpenders = await discoverNcsbeRegisteredSpenders({
        cacheDir: input.cacheDir,
        cycleYear: input.cycleYear,
      });
      spenders = await acquireNcsbeSpenderArtifacts({
        transport: input.transport,
        cacheDir: input.cacheDir,
        cycleYear: input.cycleYear,
        spenders: discoveredSpenders.spenders,
        alreadyAcquiredSboeIds: new Set(committeesBySboeId.keys()),
        force: input.force,
        retrievedAt: input.retrievedAt,
        log: input.log,
      });
    } catch (error) {
      spenderDiscoveryFailure = { message: error instanceof Error ? error.message : String(error) };
      input.log?.(`Spender discovery: FAILED — ${spenderDiscoveryFailure.message}`);
    }
  }

  return { discovery, acquisition, spenders, spenderDiscoveryFailure };
}

// Alabama per-candidate finance sync: fetch the linked race row live (the
// authority — Phase 0 contract: race totals == Σ filed report covers,
// cent-exact), build contribution-size buckets from the CACHED cash
// extracts, and write one full-replacement snapshot.
//
// Presence semantics (plan Phase 3): the summary always comes from the race
// row; the coverage ratio gates BUCKETS ONLY, never the summary — a lagging
// or permanently incomplete extract must not leave authoritative totals
// stale. A missing race row or fetch failure throws and writes nothing,
// preserving the prior snapshot. Extracts are never fetched live here — the
// refresh CLI (its own flag) populates the cache; an unreadable artifact
// just gates the buckets off with a diagnostic.

import { resolve } from "node:path";

import type { Pool, PoolClient } from "pg";

import {
  DEFAULT_ALABAMA_FCPA_CACHE_DIR,
  readAlabamaFcpaArtifact,
} from "./alabamaFcpaArtifactCache.js";
import type { AlabamaFcpaClientOptions, AlabamaRaceRow } from "./alabamaFcpaClient.js";
import { parseAlabamaCashExtract, type AlabamaCashRow } from "./alabamaFcpaCsv.js";
import { normalizeAlabamaCandidateNameForStorage } from "./alabamaCandidateRaceResolver.js";
import {
  createAlabamaOfficeRaceContextLoader,
  type AlabamaOfficeRaceContext,
} from "./alabamaCandidateFinanceAutoLink.js";
import {
  aggregateAlabamaDirectFinance,
  type AlabamaDirectFinanceAggregationResult,
} from "./alabamaDirectFinanceAggregator.js";
import {
  alabamaFcpaOfficeLabelForRace,
  alabamaOfficeTermYears,
  isAlabamaFinanceEligibleOffice,
} from "./alabamaFinanceEligibleOffices.js";
import {
  replaceAlabamaCandidateFinanceSnapshot,
  updateAlabamaFinanceLinkFcpaCommitteeNumber,
  type AlabamaFinanceLinkSource,
} from "./alabamaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

export class AlabamaCandidateFinanceSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AlabamaCandidateFinanceSyncError";
  }
}

/**
 * Transaction-date years whose cash extracts feed the bucket window: the
 * term-length cycle ending in the election year (four years for most v1
 * offices, six for the appellate courts — alabamaOfficeTermYears), clamped
 * to the portal's first extract year. Phase 0 found 2024-dated rows for
 * 2025-registered committees, so the window is transaction-date years, not
 * registration years.
 */
export function alabamaBucketExtractYears(electionYear: number, termYears: number): number[] {
  if (!Number.isInteger(termYears) || termYears < 1) {
    throw new AlabamaCandidateFinanceSyncError(`invalid term years: ${termYears}`);
  }
  const years: number[] = [];
  for (let year = Math.max(2013, electionYear - (termYears - 1)); year <= electionYear; year += 1) {
    years.push(year);
  }
  return years;
}

export type AlabamaCashRowsLoader = (
  year: number
) => Promise<{ rows: AlabamaCashRow[]; quarantinedCount: number }>;

/** Reads and parses one cached cash artifact per year; no live fetches. */
export function createAlabamaCashRowsLoader(cacheDir?: string): AlabamaCashRowsLoader {
  const resolvedCacheDir = resolve(
    cacheDir ??
      (process.env.ALABAMA_FCPA_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_ALABAMA_FCPA_CACHE_DIR)
  );
  const parsed = new Map<number, Promise<{ rows: AlabamaCashRow[]; quarantinedCount: number }>>();
  return (year) => {
    let entry = parsed.get(year);
    if (entry === undefined) {
      entry = (async () => {
        const { csvText } = await readAlabamaFcpaArtifact({
          kind: "cash",
          year,
          cacheDir: resolvedCacheDir,
        });
        const result = parseAlabamaCashExtract(csvText);
        return { rows: result.rows, quarantinedCount: result.quarantined.length };
      })();
      parsed.set(year, entry);
    }
    return entry;
  };
}

function roundToCents(value: number): number {
  return Math.round(value * 100) / 100;
}

export type AlabamaCandidateFinanceSyncResult = {
  dryRun: boolean;
  status: "synced";
  internalCommitteeId: number;
  fcpaCommitteeNumber: string | null;
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number;
  cashOnHand: number;
  bucketExtractYears: number[];
  coverageRatio: number | null;
  coverageCashCents: number | null;
  bucketsWritten: number;
  /** Empty when buckets were written; the gate reasons otherwise. */
  bucketDiagnostics: string[];
  quarantinedRowCount: number;
  summaryWritten: boolean;
};

export async function syncAlabamaCandidateFinance(input: {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  ballotTitle: string;
  district?: string | null;
  link: {
    internalCommitteeId: number;
    committeeName: string;
    fcpaCommitteeNumber: string | null;
    linkSource: AlabamaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  now?: Date;
  dryRun?: boolean;
  cacheDir?: string;
  clientOptions?: AlabamaFcpaClientOptions;
  loadOfficeRaceContext?: (electionYear: number, officeLabel: string) => Promise<AlabamaOfficeRaceContext>;
  loadCashRows?: AlabamaCashRowsLoader;
}): Promise<AlabamaCandidateFinanceSyncResult> {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    throw new AlabamaCandidateFinanceSyncError("candidateName is required");
  }
  if (!isAlabamaFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    throw new AlabamaCandidateFinanceSyncError(
      `office ${input.officeScope}::${input.officeName} is not Alabama-finance eligible`
    );
  }
  if (!Number.isSafeInteger(input.link.internalCommitteeId) || input.link.internalCommitteeId <= 0) {
    throw new AlabamaCandidateFinanceSyncError(
      `invalid internal committee id: ${input.link.internalCommitteeId}`
    );
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new AlabamaCandidateFinanceSyncError("invalid now");
  }
  const dryRun = input.dryRun === true;

  const officeLabel = alabamaFcpaOfficeLabelForRace({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    ballotTitle: input.ballotTitle,
  });
  if (officeLabel === null) {
    throw new AlabamaCandidateFinanceSyncError(
      `no FCPA office label for ${input.officeScope}::${input.officeName} (${input.ballotTitle})`
    );
  }

  const loadOfficeRaceContext =
    input.loadOfficeRaceContext ??
    createAlabamaOfficeRaceContextLoader({ clientOptions: input.clientOptions });
  const context = await loadOfficeRaceContext(input.electionYear, officeLabel);
  const raceRow: AlabamaRaceRow | undefined = context.raceRows.find(
    (row) => row.COMMITTEEID === input.link.internalCommitteeId
  );
  if (raceRow === undefined) {
    // The link points at a committee the cycle's race query no longer
    // returns — a wrong link or a portal change. Never guess and never
    // write; the prior snapshot stays.
    throw new AlabamaCandidateFinanceSyncError(
      `race row for internal committee id ${input.link.internalCommitteeId} not found under ${officeLabel}`
    );
  }

  const totalReceipts = roundToCents(
    raceRow.MONETARYCONTRIB + raceRow.NONMONETARYCONTRIB + raceRow.OTHERSOURCES
  );
  const directContributionTotal = roundToCents(raceRow.MONETARYCONTRIB + raceRow.NONMONETARYCONTRIB);
  const totalDisbursements = roundToCents(raceRow.MONETARYEXP);
  const cashOnHand = roundToCents(raceRow.ENDINGFUNDS);

  // Self-heal a NULL fcpa_committee_number (a crashed auto-link backfill, or
  // a manual link created without it) from the committee-search join already
  // loaded in the office context. The healed value is used this run and
  // persisted so the next run starts whole.
  let fcpaCommitteeNumber = input.link.fcpaCommitteeNumber;
  if (fcpaCommitteeNumber === null) {
    const derived = context.committeeRowsByInternalId
      .get(input.link.internalCommitteeId)
      ?.committeeId?.trim();
    if (derived && /^[1-9]\d*$/.test(derived)) {
      fcpaCommitteeNumber = derived;
      if (!dryRun) {
        await updateAlabamaFinanceLinkFcpaCommitteeNumber({
          db: input.db,
          candidateId: input.candidateId,
          electionId: input.electionId,
          internalCommitteeId: input.link.internalCommitteeId,
          fcpaCommitteeNumber: derived,
        });
      }
    }
  }

  // Buckets: cached extracts only. Any gate failure clears stored buckets —
  // buckets must always correspond to the summary being written.
  const bucketExtractYears = alabamaBucketExtractYears(
    input.electionYear,
    alabamaOfficeTermYears({ officeScope: input.officeScope, officeCanonicalName: input.officeName })
  );
  const bucketDiagnostics: string[] = [];
  let aggregation: AlabamaDirectFinanceAggregationResult | null = null;
  let quarantinedRowCount = 0;
  if (fcpaCommitteeNumber === null) {
    bucketDiagnostics.push("fcpa_committee_number_missing");
  } else {
    const loadCashRows = input.loadCashRows ?? createAlabamaCashRowsLoader(input.cacheDir);
    const cashRows: AlabamaCashRow[] = [];
    for (const year of bucketExtractYears) {
      try {
        const loaded = await loadCashRows(year);
        cashRows.push(...loaded.rows);
        quarantinedRowCount += loaded.quarantinedCount;
      } catch (error) {
        bucketDiagnostics.push(
          `artifact_unavailable:${year}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
    if (bucketDiagnostics.length === 0) {
      aggregation = aggregateAlabamaDirectFinance({
        cashRows,
        fcpaCommitteeNumber,
        raceMonetaryContrib: raceRow.MONETARYCONTRIB,
      });
      bucketDiagnostics.push(...aggregation.bucketDiagnostics);
    }
  }
  const bucketsUsable = aggregation !== null && aggregation.bucketsUsable;
  const breakdowns = bucketsUsable ? aggregation!.breakdowns : [];

  const sourceUrl = input.link.sourceUrl ?? null;
  let summaryWritten = false;
  let bucketsWritten = 0;
  if (!dryRun) {
    const writeResult = await replaceAlabamaCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeAlabamaCandidateNameForStorage(candidateName),
        officeName: input.officeName,
        district: input.district ?? null,
        internalCommitteeId: input.link.internalCommitteeId,
        committeeName: input.link.committeeName,
        linkStatus: "active",
        linkSource: input.link.linkSource,
        sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts,
        directContributionTotal,
        totalDisbursements,
        cashOnHand,
        sourceUrl,
      },
      directBreakdowns: breakdowns.map((breakdown) => ({
        categoryType: breakdown.categoryType,
        categoryName: breakdown.categoryName,
        amount: breakdown.amount,
        contributorCount: breakdown.contributorCount,
        sourceUrl,
      })),
    });
    summaryWritten = writeResult.summaryWritten;
    bucketsWritten = writeResult.directBreakdownsWritten;
  }

  return {
    dryRun,
    status: "synced",
    internalCommitteeId: input.link.internalCommitteeId,
    fcpaCommitteeNumber,
    totalReceipts,
    directContributionTotal,
    totalDisbursements,
    cashOnHand,
    bucketExtractYears,
    coverageRatio: aggregation?.coverageRatio ?? null,
    coverageCashCents: aggregation?.coverageCashCents ?? null,
    bucketsWritten,
    bucketDiagnostics,
    quarantinedRowCount,
    summaryWritten,
  };
}

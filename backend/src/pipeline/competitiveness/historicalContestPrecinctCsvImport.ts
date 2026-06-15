import type { Pool, PoolClient } from "pg";

import {
  buildCsvHeaderIndex,
  csvCell,
  parseCsvRows,
  requireCsvColumn,
} from "./historicalContestCsv.js";
import {
  normalizeMedslHistoricalContestMargins,
  type HistoricalContestMarginRecord,
  type HistoricalContestNormalizationSkippedRow,
  type MedslHistoricalContestCandidateRow,
} from "./historicalContestNormalizer.js";
import {
  upsertHistoricalContestMargins,
  type HistoricalContestMarginWriteResult,
} from "./historicalContestMarginWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type MedslHistoricalContestPrecinctRow = {
  year: string;
  state_po: string;
  state_fips: string;
  office: string;
  district: string | null;
  candidate: string | null;
  votes: string;
  party_simplified: string | null;
  party_detailed: string | null;
  stage: string | null;
  mode: string | null;
  writein: string | null;
  precinct: string | null;
  county_name: string | null;
  county_fips: string | null;
  jurisdiction_name: string | null;
  jurisdiction_fips: string | null;
  dataverse: string | null;
  special: string | null;
};

export type HistoricalContestPrecinctCsvImportResult = {
  parsedRows: number;
  aggregatedRows: number;
  normalizedRecords: number;
  skippedRows: HistoricalContestNormalizationSkippedRow[];
  writeResult: HistoricalContestMarginWriteResult | null;
};

type ParsedPrecinctVoteRow = {
  row: MedslHistoricalContestPrecinctRow;
  votes: number;
};

type CandidateVoteAccumulator = {
  candidate: string | null;
  votes: number;
  partySimplifiedVotes: Map<string, number>;
  partyDetailedVotes: Map<string, number>;
};

const REQUIRED_MEDSL_PRECINCT_COLUMNS = [
  "year",
  "state_po",
  "state_fips",
  "office",
  "votes",
] as const;

function optionalCell(cells: readonly string[], index: number | undefined): string | null {
  if (index === undefined) {
    return null;
  }
  const value = csvCell(cells, index);
  return value.length > 0 ? value : null;
}

function normalizeText(value: string | null | undefined): string {
  return value?.trim().replace(/\s+/g, " ") ?? "";
}

function normalizeKeyText(value: string | null | undefined): string {
  return normalizeText(value).toUpperCase();
}

function parseNonNegativeInteger(value: string): number | null {
  const normalized = value.trim();
  if (!/^[0-9]+$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function normalizeCandidateBucket(value: string | null | undefined): string {
  return normalizeKeyText(value)
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalCandidateNameKey(value: string | null | undefined): string {
  const rawName = normalizeText(value);
  const commaParts = rawName.split(",").map((part) => normalizeCandidateBucket(part)).filter(Boolean);
  if (commaParts.length === 2) {
    return `${commaParts[1]} ${commaParts[0]}`.trim();
  }
  return normalizeCandidateBucket(rawName);
}

function isGenericWriteInBucket(candidate: string): boolean {
  return (
    candidate === "WRITE IN" ||
    candidate === "WRITE INS" ||
    candidate === "WRITEIN" ||
    candidate === "WRITEINS" ||
    candidate === "WRITE IN CANDIDATE" ||
    candidate === "WRITE IN CANDIDATES" ||
    candidate === "WRITEIN CANDIDATE" ||
    candidate === "WRITEIN CANDIDATES"
  );
}

function isNonCandidateBucket(candidate: string): boolean {
  return (
    candidate === "" ||
    candidate === "UNDERVOTE" ||
    candidate === "UNDERVOTES" ||
    candidate === "UNDER VOTE" ||
    candidate === "UNDER VOTES" ||
    candidate === "OVERVOTE" ||
    candidate === "OVERVOTES" ||
    candidate === "OVER VOTE" ||
    candidate === "OVER VOTES" ||
    candidate === "BLANK" ||
    candidate === "BLANKS" ||
    candidate === "BLANK VOTE" ||
    candidate === "BLANK VOTES" ||
    candidate === "SCATTERING" ||
    candidate === "EXHAUSTED" ||
    candidate === "EXHAUSTED BALLOT" ||
    candidate === "EXHAUSTED BALLOTS" ||
    candidate === "TOTAL" ||
    candidate === "TOTALS" ||
    candidate === "TOTAL VOTE" ||
    candidate === "TOTAL VOTES" ||
    candidate === "TIMES CAST" ||
    candidate === "BALLOTS CAST" ||
    candidate === "REGISTERED VOTERS" ||
    isGenericWriteInBucket(candidate)
  );
}

function isTruthyMedslBoolean(value: string | null): boolean {
  const normalized = normalizeCandidateBucket(value);
  return normalized === "TRUE" || normalized === "YES" || normalized === "Y" || normalized === "1";
}

function isCandidateVoteRow(row: MedslHistoricalContestPrecinctRow): boolean {
  const candidate = normalizeCandidateBucket(row.candidate);
  if (isNonCandidateBucket(candidate)) {
    return false;
  }

  // Keep named write-in candidates, but drop blank/generic write-in buckets.
  return !(isTruthyMedslBoolean(row.writein) && isGenericWriteInBucket(candidate));
}

function isStatewideMitOffice(value: string): boolean {
  const office = normalizeKeyText(value);
  return office === "US PRESIDENT" || office === "US SENATE" || office === "GOVERNOR";
}

function outputDistrict(row: MedslHistoricalContestPrecinctRow): string {
  return row.district ?? (isStatewideMitOffice(row.office) ? "STATEWIDE" : "");
}

function contestKey(row: MedslHistoricalContestPrecinctRow): string {
  return [
    normalizeKeyText(row.year),
    normalizeKeyText(row.state_po),
    normalizeKeyText(row.state_fips),
    normalizeKeyText(row.office),
    normalizeKeyText(outputDistrict(row)),
    normalizeKeyText(row.stage),
  ].join("|");
}

function candidateKey(row: MedslHistoricalContestPrecinctRow): string {
  const candidate = canonicalCandidateNameKey(row.candidate);
  if (candidate) {
    return `candidate:${candidate}`;
  }
  return [
    "unnamed",
    normalizeKeyText(row.party_simplified),
    normalizeKeyText(row.party_detailed),
  ].join("|");
}

function addPartyVotes(partyVotes: Map<string, number>, party: string | null, votes: number): void {
  const normalized = normalizeText(party);
  if (!normalized) {
    return;
  }
  partyVotes.set(normalized, (partyVotes.get(normalized) ?? 0) + votes);
}

function chooseLargestVoteParty(partyVotes: Map<string, number>): string | null {
  const [winner] = [...partyVotes.entries()].sort(
    ([leftParty, leftVotes], [rightParty, rightVotes]) =>
      rightVotes - leftVotes || leftParty.localeCompare(rightParty)
  );
  return winner?.[0] ?? null;
}

function representativeCandidateName(value: string | null): string | null {
  const normalized = normalizeText(value);
  return normalized.length > 0 ? normalized : null;
}

export function parseMedslHistoricalContestPrecinctCsv(csv: string): MedslHistoricalContestPrecinctRow[] {
  const rows = parseCsvRows(csv);
  const header = rows[0];
  if (!header) {
    return [];
  }

  const headerIndex = buildCsvHeaderIndex(header);
  const indexes = Object.fromEntries(
    REQUIRED_MEDSL_PRECINCT_COLUMNS.map((column) => [column, requireCsvColumn(headerIndex, column)])
  ) as Record<(typeof REQUIRED_MEDSL_PRECINCT_COLUMNS)[number], number>;

  const candidateIndex = headerIndex.get("candidate");
  const districtIndex = headerIndex.get("district");
  const partySimplifiedIndex = headerIndex.get("party_simplified");
  const partyDetailedIndex = headerIndex.get("party_detailed");
  const stageIndex = headerIndex.get("stage");
  const modeIndex = headerIndex.get("mode");
  const writeinIndex = headerIndex.get("writein");
  const precinctIndex = headerIndex.get("precinct");
  const countyNameIndex = headerIndex.get("county_name");
  const countyFipsIndex = headerIndex.get("county_fips");
  const jurisdictionNameIndex = headerIndex.get("jurisdiction_name");
  const jurisdictionFipsIndex = headerIndex.get("jurisdiction_fips");
  const dataverseIndex = headerIndex.get("dataverse");
  const specialIndex = headerIndex.get("special");

  return rows.slice(1).map((cells) => ({
    year: csvCell(cells, indexes.year),
    state_po: csvCell(cells, indexes.state_po),
    state_fips: csvCell(cells, indexes.state_fips),
    office: csvCell(cells, indexes.office),
    district: optionalCell(cells, districtIndex),
    candidate: optionalCell(cells, candidateIndex),
    votes: csvCell(cells, indexes.votes),
    party_simplified: optionalCell(cells, partySimplifiedIndex),
    party_detailed: optionalCell(cells, partyDetailedIndex),
    stage: optionalCell(cells, stageIndex),
    mode: optionalCell(cells, modeIndex),
    writein: optionalCell(cells, writeinIndex),
    precinct: optionalCell(cells, precinctIndex),
    county_name: optionalCell(cells, countyNameIndex),
    county_fips: optionalCell(cells, countyFipsIndex),
    jurisdiction_name: optionalCell(cells, jurisdictionNameIndex),
    jurisdiction_fips: optionalCell(cells, jurisdictionFipsIndex),
    dataverse: optionalCell(cells, dataverseIndex),
    special: optionalCell(cells, specialIndex),
  }));
}

export function aggregateMedslPrecinctRowsToCandidateRows(
  rows: readonly MedslHistoricalContestPrecinctRow[]
): MedslHistoricalContestCandidateRow[] {
  const contests = new Map<string, ParsedPrecinctVoteRow[]>();
  const invalidVoteRows: MedslHistoricalContestCandidateRow[] = [];

  for (const row of rows) {
    if (!isCandidateVoteRow(row)) {
      continue;
    }

    const votes = parseNonNegativeInteger(row.votes);
    if (votes === null) {
      invalidVoteRows.push({
        year: row.year,
        state_po: row.state_po,
        state_fips: row.state_fips,
        office: row.office,
        district: outputDistrict(row),
        candidate: representativeCandidateName(row.candidate),
        candidatevotes: row.votes,
        totalvotes: row.votes,
        party_simplified: row.party_simplified,
        party_detailed: row.party_detailed,
        stage: row.stage,
      });
      continue;
    }

    const key = contestKey(row);
    const existing = contests.get(key) ?? [];
    existing.push({ row, votes });
    contests.set(key, existing);
  }

  const candidateRows: MedslHistoricalContestCandidateRow[] = [];

  for (const contestRows of contests.values()) {
    const hasTotalMode = contestRows.some(({ row }) => normalizeKeyText(row.mode) === "TOTAL");
    const selectedRows = hasTotalMode
      ? contestRows.filter(({ row }) => normalizeKeyText(row.mode) === "TOTAL")
      : contestRows;
    const firstRow = selectedRows[0]?.row;
    if (!firstRow) {
      continue;
    }

    const candidates = new Map<string, CandidateVoteAccumulator>();
    for (const { row, votes } of selectedRows) {
      const key = candidateKey(row);
      const existing = candidates.get(key) ?? {
        candidate: representativeCandidateName(row.candidate),
        votes: 0,
        partySimplifiedVotes: new Map<string, number>(),
        partyDetailedVotes: new Map<string, number>(),
      };
      existing.votes += votes;
      addPartyVotes(existing.partySimplifiedVotes, row.party_simplified, votes);
      addPartyVotes(existing.partyDetailedVotes, row.party_detailed, votes);
      candidates.set(key, existing);
    }

    const totalVotes = [...candidates.values()].reduce((sum, candidate) => sum + candidate.votes, 0);
    for (const candidate of candidates.values()) {
      candidateRows.push({
        year: firstRow.year,
        state_po: firstRow.state_po,
        state_fips: firstRow.state_fips,
        office: firstRow.office,
        district: outputDistrict(firstRow),
        candidate: candidate.candidate,
        candidatevotes: String(candidate.votes),
        totalvotes: String(totalVotes),
        party_simplified: chooseLargestVoteParty(candidate.partySimplifiedVotes),
        party_detailed: chooseLargestVoteParty(candidate.partyDetailedVotes),
        stage: firstRow.stage,
      });
    }
  }

  return [...candidateRows, ...invalidVoteRows].sort((left, right) =>
    String(left.year).localeCompare(String(right.year)) ||
    left.state_po.localeCompare(right.state_po) ||
    left.office.localeCompare(right.office) ||
    String(left.district).localeCompare(String(right.district)) ||
    String(left.candidate ?? "").localeCompare(String(right.candidate ?? ""))
  );
}

export function parseAndAggregateMedslHistoricalContestPrecinctCsv(csv: string): MedslHistoricalContestCandidateRow[] {
  return aggregateMedslPrecinctRowsToCandidateRows(parseMedslHistoricalContestPrecinctCsv(csv));
}

export async function importHistoricalContestMarginsFromPrecinctCsv(
  db: Queryable,
  input: {
    csv: string;
    source: string;
    sourceUrl?: string | null;
    staleAfterRedistricting?: boolean;
    dryRun?: boolean;
    importedAt?: Date;
  }
): Promise<HistoricalContestPrecinctCsvImportResult> {
  const parsedRows = parseMedslHistoricalContestPrecinctCsv(input.csv);
  const aggregatedRows = aggregateMedslPrecinctRowsToCandidateRows(parsedRows);
  const normalized = normalizeMedslHistoricalContestMargins({
    source: input.source,
    sourceUrl: input.sourceUrl,
    rows: aggregatedRows,
    staleAfterRedistricting: input.staleAfterRedistricting,
  });

  const records: HistoricalContestMarginRecord[] = normalized.records;
  const writeResult = input.dryRun
    ? null
    : await upsertHistoricalContestMargins(db, records, { importedAt: input.importedAt });

  return {
    parsedRows: parsedRows.length,
    aggregatedRows: aggregatedRows.length,
    normalizedRecords: records.length,
    skippedRows: normalized.skippedRows,
    writeResult,
  };
}

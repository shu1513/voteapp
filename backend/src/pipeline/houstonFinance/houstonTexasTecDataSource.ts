import { stat } from "node:fs/promises";
import {
  DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR,
  TEXAS_TEC_CSV_DATABASE_URL,
  getTexasTecCsvDatabaseArtifactCachePaths,
} from "../texasFinance/texasTecCsvDatabaseArtifactCache.js";
import {
  listTexasTecContributionCsvFileNames,
  listTexasTecExpenditureCsvFileNames,
  readTexasTecCandidateRows,
  readTexasTecContributionRows,
  readTexasTecExpenditureRows,
  readTexasTecFilerRows,
  readTexasTecPurposeRows,
  type TexasTecCandidateRow,
  type TexasTecContributionRow,
  type TexasTecExpenditureRow,
  type TexasTecPurposeRow,
} from "../texasFinance/texasTecCsvDatabaseReader.js";
import { normalizeTexasCandidateNameKeys } from "../texasFinance/texasCandidateCommitteeResolver.js";

export type HoustonTexasTecData = {
  sourceUrl: string;
  purposeRows: TexasTecPurposeRow[];
  candidateRows: TexasTecCandidateRow[];
  expenditureRows: TexasTecExpenditureRow[];
  contributionRows: TexasTecContributionRow[];
  politicalCommitteeNames: Set<string>;
};

function id(value: string): string { return value.trim().toUpperCase(); }
function year(value: string): number | null {
  return Number(/^(\d{4})/.exec(value.trim())?.[1] ?? /\/([0-9]{4})/.exec(value.trim())?.[1] ?? NaN) || null;
}
function rowNameKeys(row: TexasTecCandidateRow): Set<string> {
  return new Set([
    ...normalizeTexasCandidateNameKeys(`${row.candidateNameFirst} ${row.candidateNameLast}`),
    ...normalizeTexasCandidateNameKeys(row.candidateNameOrganization),
  ]);
}
function intersects(left: ReadonlySet<string>, right: ReadonlySet<string>): boolean {
  for (const value of left) if (right.has(value)) return true;
  return false;
}

export async function loadHoustonTexasTecData(input: {
  candidates: readonly { candidateName: string; electionYear: number }[];
  zipPath?: string;
  cacheDir?: string;
}): Promise<HoustonTexasTecData> {
  const cacheDir = input.cacheDir ?? process.env.TEXAS_TEC_CSV_DATABASE_CACHE_DIR?.trim() ?? DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR;
  const zipPath = input.zipPath ?? getTexasTecCsvDatabaseArtifactCachePaths(cacheDir).zipPath;
  try {
    if (!(await stat(zipPath)).isFile()) throw new Error("not a file");
  } catch {
    throw new Error(`Houston outside finance skipped: Texas TEC artifact is unavailable at ${zipPath}`);
  }
  const names = new Set(input.candidates.flatMap((candidate) => [...normalizeTexasCandidateNameKeys(candidate.candidateName)]));
  const years = new Set(input.candidates.flatMap((candidate) => [candidate.electionYear - 1, candidate.electionYear]));
  const purposeRows = await readTexasTecPurposeRows({
    zipPath,
    predicate: (row) => intersects(names, normalizeTexasCandidateNameKeys(row.commActivityName)),
  });
  const filerRows = await readTexasTecFilerRows({ zipPath });
  const politicalCommitteeNames = new Set(
    filerRows.flatMap((row) => [row.filerName, row.filerNameOrganization])
      .map((value) => id(value).replace(/[^A-Z0-9]+/g, " ").replace(/\s+/g, " ").trim())
      .filter(Boolean)
  );
  const candidateRows = await readTexasTecCandidateRows({
    zipPath,
    predicate: (row) => intersects(names, rowNameKeys(row)) && years.has(year(row.expendDt) ?? -1),
  });
  const committeeIds = new Set(candidateRows.map((row) => id(row.filerIdent)).filter(Boolean));
  const expenditureIds = new Set(candidateRows.map((row) => `${id(row.filerIdent)}\u0000${id(row.expendInfoId)}`));
  const expenditureRows: TexasTecExpenditureRow[] = [];
  for (const fileName of await listTexasTecExpenditureCsvFileNames(zipPath)) {
    const rows = await readTexasTecExpenditureRows({
      zipPath,
      fileName,
      predicate: (row) => expenditureIds.has(`${id(row.filerIdent)}\u0000${id(row.expendInfoId)}`),
    });
    for (const row of rows) {
      expenditureRows.push(row);
    }
  }
  const contributionRows: TexasTecContributionRow[] = [];
  for (const fileName of await listTexasTecContributionCsvFileNames(zipPath)) {
    const rows = await readTexasTecContributionRows({
      zipPath,
      fileName,
      predicate: (row) => committeeIds.has(id(row.filerIdent)) && years.has(year(row.contributionDt) ?? -1),
    });
    for (const row of rows) {
      contributionRows.push(row);
    }
  }
  return { sourceUrl: TEXAS_TEC_CSV_DATABASE_URL, purposeRows, candidateRows, expenditureRows, contributionRows, politicalCommitteeNames };
}

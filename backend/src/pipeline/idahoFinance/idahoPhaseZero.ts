// Phase 0 only: validates the Idaho CFIS acquisition contracts and encodes the
// two load-bearing findings (see docs/plans/idaho-finance.md):
// 1. the registration grid's totalRaised equals the sum of the registration's
//    current contribution transactions from the search API, cent-exact;
// 2. the bulk CSV export contains exactly the version-1 transactions, so any
//    contribution edited or added through an amended report is missing from it.
// No database, cache, scheduler, or published snapshot.

import type {
  IdahoCandidateRegistrationRow,
  IdahoContributionRow,
  IdahoIndependentExpenditureRow,
} from "./idahoCfsClient.js";
import { parseIdahoCurrencyCents, type IdahoReceiptCsvRow } from "./idahoCfsCsv.js";

export type IdahoMoneySummary = {
  rowCount: number;
  amountCents: number;
};

const CONTRIBUTION_TRANSACTION_TYPE = "TCON";
const BULK_CONTRIBUTION_TRANSACTION_TYPE = "Contribution";

function sumSearchRows(rows: readonly IdahoContributionRow[]): IdahoMoneySummary {
  return {
    rowCount: rows.length,
    amountCents: rows.reduce((sum, row) => sum + Math.round(row.transactionAmount * 100), 0),
  };
}

function transactionYear(row: IdahoContributionRow): number {
  const match = /^\d{2}\/\d{2}\/(\d{4})$/.exec(row.transactionDate);
  if (!match) throw new Error(`Idaho contribution ${row.transactionId} has an unexpected date ${row.transactionDate}`);
  return Number(match[1]);
}

// Search rows come back for every registration sharing the searched name;
// the registration guid on each row is the only exact attribution key.
export function selectIdahoRegistrationContributions(
  rows: readonly IdahoContributionRow[],
  registrationGuid: string
): IdahoContributionRow[] {
  return rows.filter(
    (row) =>
      row.filerRegistrationGuid === registrationGuid &&
      row.transactionTypeCode === CONTRIBUTION_TRANSACTION_TYPE
  );
}

export type IdahoRegistrationReconciliation = {
  registrationGuid: string;
  filerEntityId: number;
  electionYear: number;
  filerName: string;
  gridTotalRaisedCents: number;
  search: IdahoMoneySummary;
  searchVersionOne: IdahoMoneySummary;
  bulk: IdahoMoneySummary;
  bulkFilingYears: number[];
  deltaCents: number;
  status: "match" | "mismatch";
  // true when the bulk export reproduces exactly the version-1 rows of the
  // registration within the downloaded filing years.
  bulkMatchesVersionOne: boolean;
};

export function reconcileIdahoRegistration(input: {
  registration: IdahoCandidateRegistrationRow;
  searchRows: readonly IdahoContributionRow[];
  bulkRows: readonly IdahoReceiptCsvRow[];
  bulkFilingYears: readonly number[];
}): IdahoRegistrationReconciliation {
  const { registration } = input;
  const rows = selectIdahoRegistrationContributions(input.searchRows, registration.registrationGuid);
  const wrongEntity = rows.find((row) => row.filerEntityId !== registration.filerEntityId);
  if (wrongEntity) {
    throw new Error(
      `Idaho registration ${registration.registrationGuid} received rows for entity ${wrongEntity.filerEntityId}`
    );
  }
  const filingYears = new Set(input.bulkFilingYears);
  const transactionIds = new Set(rows.map((row) => row.transactionId));
  const versionOneRows = rows.filter(
    (row) => row.transactionVersionId === 1 && filingYears.has(transactionYear(row))
  );
  const entityId = String(registration.filerEntityId);
  const bulkRows = input.bulkRows.filter(
    (row) =>
      row["Filing Entity ID"].trim() === entityId &&
      row["Transaction Type"] === BULK_CONTRIBUTION_TRANSACTION_TYPE &&
      transactionIds.has(Number(row["Transaction Id"]))
  );
  const bulk: IdahoMoneySummary = {
    rowCount: bulkRows.length,
    amountCents: bulkRows.reduce((sum, row) => sum + parseIdahoCurrencyCents(row["Transaction Amount"]), 0),
  };
  const search = sumSearchRows(rows);
  const searchVersionOne = sumSearchRows(versionOneRows);
  const gridTotalRaisedCents = Math.round(registration.totalRaised * 100);
  const deltaCents = search.amountCents - gridTotalRaisedCents;
  return {
    registrationGuid: registration.registrationGuid,
    filerEntityId: registration.filerEntityId,
    electionYear: registration.electionYear,
    filerName: registration.filerName,
    gridTotalRaisedCents,
    search,
    searchVersionOne,
    bulk,
    bulkFilingYears: [...input.bulkFilingYears],
    deltaCents,
    status: deltaCents === 0 ? "match" : "mismatch",
    bulkMatchesVersionOne:
      bulk.rowCount === searchVersionOne.rowCount && bulk.amountCents === searchVersionOne.amountCents,
  };
}

export type IdahoIndependentExpenditureProbeSummary = {
  sourceRowCount: number;
  rowCount: number;
  support: IdahoMoneySummary;
  oppose: IdahoMoneySummary;
  candidateTargetRowCount: number;
  measureTargetRowCount: number;
  registrationResolvedRowCount: number;
  nonRegisteredCandidateRowCount: number;
  registeredFilerRowCount: number;
  nonRegisteredFilerRowCount: number;
};

// TIECOM = IE reported by a registered Idaho filer; TEXP = IE reported by an
// entity not registered in Idaho (federal PACs etc.). Electioneering
// communications never appear here (verified: stance is always Support/Oppose).
const IE_TRANSACTION_TYPE_CODES = new Set(["TIECOM", "TEXP"]);

export function summarizeIdahoIndependentExpenditures(
  rows: readonly IdahoIndependentExpenditureRow[],
  electionYear: number
): IdahoIndependentExpenditureProbeSummary {
  const selected: IdahoIndependentExpenditureRow[] = [];
  for (const row of rows) {
    if (!IE_TRANSACTION_TYPE_CODES.has(row.transactionTypeCode)) {
      throw new Error(`Idaho IE search returned unknown transaction type ${row.transactionTypeCode}`);
    }
    if (row.stance !== "Support" && row.stance !== "Oppose") {
      throw new Error(`Idaho IE search returned unknown stance ${JSON.stringify(row.stance)}`);
    }
    if (!/^\d{4}-\d{2}-\d{2}T/.test(row.transactionDate)) {
      throw new Error(`Idaho IE search returned unexpected date ${row.transactionDate}`);
    }
    if (Number(row.transactionDate.slice(0, 4)) === electionYear) selected.push(row);
  }
  const sum = (picked: readonly IdahoIndependentExpenditureRow[]): IdahoMoneySummary => ({
    rowCount: picked.length,
    amountCents: picked.reduce((total, row) => total + Math.round(row.amountApplied * 100), 0),
  });
  const candidateRows = selected.filter((row) => row.officeSought !== null);
  return {
    sourceRowCount: rows.length,
    rowCount: selected.length,
    support: sum(selected.filter((row) => row.stance === "Support")),
    oppose: sum(selected.filter((row) => row.stance === "Oppose")),
    candidateTargetRowCount: candidateRows.length,
    measureTargetRowCount: selected.length - candidateRows.length,
    registrationResolvedRowCount: candidateRows.filter(
      (row) => row.candidateMeasureFilerRegistrationGuid !== null
    ).length,
    nonRegisteredCandidateRowCount: candidateRows.filter((row) => row.isCandidateNonRegisteredEntity).length,
    registeredFilerRowCount: selected.filter((row) => row.transactionTypeCode === "TIECOM").length,
    nonRegisteredFilerRowCount: selected.filter((row) => row.transactionTypeCode === "TEXP").length,
  };
}

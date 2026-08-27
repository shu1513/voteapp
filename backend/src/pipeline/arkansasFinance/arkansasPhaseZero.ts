// Phase 0 gate computations for Arkansas CFIS (plan-arkansas-finance.md).
// Pure functions + streaming accumulators; no database, no publication.

import {
  mergeArkansasOccupation,
  parseArkansasCurrencyCents,
  type ArkansasExpenditureCsvRow,
  type ArkansasReceiptCsvRow,
} from "./arkansasCfisCsv.js";
import type {
  ArkansasFiledReportRow,
  ArkansasFilerRegistrationRow,
  ArkansasTransactionRow,
} from "./arkansasCfisClient.js";

export type ArkansasMoneySummary = {
  rowCount: number;
  amountCents: number;
};

export function centsFromArkansasApiAmount(amount: number): number {
  return Math.round(amount * 100);
}

function emptyMoney(): ArkansasMoneySummary {
  return { rowCount: 0, amountCents: 0 };
}

function addMoney(summary: ArkansasMoneySummary, cents: number): void {
  summary.rowCount += 1;
  summary.amountCents += cents;
}

// ---------------------------------------------------------------------------
// Receipt CSV accumulator (gate 2/3/4/6 inputs). Streaming: global counters
// plus full detail only for the gold filing-entity IDs.
// ---------------------------------------------------------------------------

export type ArkansasReceiptEntityDetail = {
  filingEntityId: number;
  // key: `${Transaction Type}|${Transaction Sub Type}|${Amended}`
  byTypeSubTypeAmended: Record<string, ArkansasMoneySummary>;
  // key: `${Report Name}|${Amended}` — the amendment-lineage join input
  byReportAmended: Record<string, ArkansasMoneySummary>;
  byElectionType: Record<string, ArkansasMoneySummary>;
  total: ArkansasMoneySummary;
};

export type ArkansasReceiptCsvSummary = {
  rowCount: number;
  filerTypeCounts: Record<string, number>;
  transactionTypeCounts: Record<string, number>;
  electionTypeCounts: Record<string, number>;
  amendedRowCount: number;
  // Rows whose Transaction Amount cell is blank or unparseable (2 observed
  // live in TCON 2024 with shifted cells) — skipped entirely, never $0.
  unparseableAmountRowCount: number;
  occupation: {
    individualRowCount: number;
    occupationFilledCount: number;
    occupationFromDropdownCount: number;
    occupationFromOtherCount: number;
    employerFilledCount: number;
    itemizedSmallRowCount: number;
    itemizedSmallWithOccupationCount: number;
  };
  entities: Record<string, ArkansasReceiptEntityDetail>;
  // filing-entity IDs of candidate filers with Amended=Y rows (gate 4 fixture
  // discovery), capped to keep output readable.
  candidateEntitiesWithAmendedRows: number[];
};

export type ArkansasReceiptCsvAccumulator = {
  add: (row: ArkansasReceiptCsvRow) => void;
  result: () => ArkansasReceiptCsvSummary;
};

const AMENDED_ENTITY_SAMPLE_LIMIT = 25;

export function createArkansasReceiptCsvAccumulator(
  goldEntityIds: ReadonlySet<number>
): ArkansasReceiptCsvAccumulator {
  const summary: ArkansasReceiptCsvSummary = {
    rowCount: 0,
    filerTypeCounts: {},
    transactionTypeCounts: {},
    electionTypeCounts: {},
    amendedRowCount: 0,
    unparseableAmountRowCount: 0,
    occupation: {
      individualRowCount: 0,
      occupationFilledCount: 0,
      occupationFromDropdownCount: 0,
      occupationFromOtherCount: 0,
      employerFilledCount: 0,
      itemizedSmallRowCount: 0,
      itemizedSmallWithOccupationCount: 0,
    },
    entities: {},
    candidateEntitiesWithAmendedRows: [],
  };
  const amendedCandidateEntities = new Set<number>();

  const bump = (record: Record<string, number>, key: string): void => {
    record[key] = (record[key] ?? 0) + 1;
  };

  return {
    add(row) {
      summary.rowCount += 1;
      bump(summary.filerTypeCounts, row.FilerType);
      bump(summary.transactionTypeCounts, `${row["Transaction Type"]}|${row["Transaction Sub Type"]}`);
      bump(summary.electionTypeCounts, row["Election Type"]);
      const amended = row.Amended.trim().toUpperCase() === "Y";
      if (amended) summary.amendedRowCount += 1;

      let cents: number;
      try {
        cents = parseArkansasCurrencyCents(row["Transaction Amount"]);
      } catch {
        summary.unparseableAmountRowCount += 1;
        return;
      }
      const entityId = Number(row["Filing Entity ID"]);

      if (amended && row.FilerType === "Candidate" && Number.isInteger(entityId)) {
        amendedCandidateEntities.add(entityId);
      }

      if (row["Funding Source / Loan Source Type"].includes("Individual")) {
        const occupationStats = summary.occupation;
        occupationStats.individualRowCount += 1;
        const merged = mergeArkansasOccupation(row.Occupation, row["Occupation Other"]);
        if (merged.value !== null) {
          occupationStats.occupationFilledCount += 1;
          if (merged.source === "occupation") occupationStats.occupationFromDropdownCount += 1;
          if (merged.source === "occupation_other") occupationStats.occupationFromOtherCount += 1;
        }
        if (row["Employer Name"].trim()) occupationStats.employerFilledCount += 1;
        if (
          row["Transaction Sub Type"] === "Itemized Monetary" &&
          cents > 0 &&
          cents <= 200_00
        ) {
          occupationStats.itemizedSmallRowCount += 1;
          if (merged.value !== null) occupationStats.itemizedSmallWithOccupationCount += 1;
        }
      }

      if (goldEntityIds.has(entityId)) {
        const key = String(entityId);
        const detail = (summary.entities[key] ??= {
          filingEntityId: entityId,
          byTypeSubTypeAmended: {},
          byReportAmended: {},
          byElectionType: {},
          total: emptyMoney(),
        });
        addMoney(detail.total, cents);
        const typeKey = `${row["Transaction Type"]}|${row["Transaction Sub Type"]}|${amended ? "Y" : "N"}`;
        addMoney((detail.byTypeSubTypeAmended[typeKey] ??= emptyMoney()), cents);
        const reportKey = `${row["Report Name"]}|${amended ? "Y" : "N"}`;
        addMoney((detail.byReportAmended[reportKey] ??= emptyMoney()), cents);
        addMoney((detail.byElectionType[row["Election Type"]] ??= emptyMoney()), cents);
      }
    },
    result() {
      summary.candidateEntitiesWithAmendedRows = [...amendedCandidateEntities]
        .sort((left, right) => left - right)
        .slice(0, AMENDED_ENTITY_SAMPLE_LIMIT);
      return summary;
    },
  };
}

// ---------------------------------------------------------------------------
// Expenditure CSV accumulator (gate 2 spent side + the IE free-text scan).
// ---------------------------------------------------------------------------

export type ArkansasExpenditureCsvSummary = {
  rowCount: number;
  filerTypeCounts: Record<string, number>;
  transactionTypeCounts: Record<string, number>;
  unparseableAmountRowCount: number;
  entities: Record<string, ArkansasReceiptEntityDetail>;
  independentExpenditureFilers: {
    rowCount: number;
    amountCents: number;
    distinctEntityCount: number;
    supportPatternRowCount: number;
    opposePatternRowCount: number;
    noPatternRowCount: number;
  };
};

export type ArkansasExpenditureCsvAccumulator = {
  add: (row: ArkansasExpenditureCsvRow) => void;
  result: () => ArkansasExpenditureCsvSummary;
};

const SUPPORT_PATTERN = /\bIN\s+SUPPORT\s+OF\b/i;
const OPPOSE_PATTERN = /\bIN\s+OPPOSITION\s+TO\b|\bOPPOS(?:E|ING)\b|\bAGAINST\b/i;

export function createArkansasExpenditureCsvAccumulator(
  goldEntityIds: ReadonlySet<number>
): ArkansasExpenditureCsvAccumulator {
  const summary: ArkansasExpenditureCsvSummary = {
    rowCount: 0,
    filerTypeCounts: {},
    transactionTypeCounts: {},
    unparseableAmountRowCount: 0,
    entities: {},
    independentExpenditureFilers: {
      rowCount: 0,
      amountCents: 0,
      distinctEntityCount: 0,
      supportPatternRowCount: 0,
      opposePatternRowCount: 0,
      noPatternRowCount: 0,
    },
  };
  const iefEntities = new Set<number>();

  const bump = (record: Record<string, number>, key: string): void => {
    record[key] = (record[key] ?? 0) + 1;
  };

  return {
    add(row) {
      summary.rowCount += 1;
      bump(summary.filerTypeCounts, row.FilerType);
      bump(summary.transactionTypeCounts, `${row["Transaction Type"]}|${row["Transaction Sub Type"]}`);
      let cents: number;
      try {
        cents = parseArkansasCurrencyCents(row["Transaction Amount"]);
      } catch {
        summary.unparseableAmountRowCount += 1;
        return;
      }
      const entityId = Number(row["Filing Entity ID"]);
      const amended = row.Amended.trim().toUpperCase() === "Y";

      if (row.FilerType === "Independent Expenditure Filer") {
        const ie = summary.independentExpenditureFilers;
        ie.rowCount += 1;
        ie.amountCents += cents;
        if (Number.isInteger(entityId)) iefEntities.add(entityId);
        const description = row["Transaction Description"];
        if (SUPPORT_PATTERN.test(description)) ie.supportPatternRowCount += 1;
        else if (OPPOSE_PATTERN.test(description)) ie.opposePatternRowCount += 1;
        else ie.noPatternRowCount += 1;
      }

      if (goldEntityIds.has(entityId)) {
        const key = String(entityId);
        const detail = (summary.entities[key] ??= {
          filingEntityId: entityId,
          byTypeSubTypeAmended: {},
          byReportAmended: {},
          byElectionType: {},
          total: emptyMoney(),
        });
        addMoney(detail.total, cents);
        const typeKey = `${row["Transaction Type"]}|${row["Transaction Sub Type"]}|${amended ? "Y" : "N"}`;
        addMoney((detail.byTypeSubTypeAmended[typeKey] ??= emptyMoney()), cents);
        const reportKey = `${row["Report Name"]}|${amended ? "Y" : "N"}`;
        addMoney((detail.byReportAmended[reportKey] ??= emptyMoney()), cents);
        addMoney((detail.byElectionType[row["Election Type"]] ??= emptyMoney()), cents);
      }
    },
    result() {
      summary.independentExpenditureFilers.distinctEntityCount = iefEntities.size;
      return summary;
    },
  };
}

// ---------------------------------------------------------------------------
// Gate 2 — registration-row totals semantics.
// ---------------------------------------------------------------------------

type ComponentSums = {
  contributionMonetaryCents: number;
  contributionNonmoneyCents: number;
  loanCents: number;
  returnContributionMonetaryCents: number;
  returnContributionNonmoneyCents: number;
  expenditureCents: number;
  returnExpenditureCents: number;
  loanPaymentCents: number;
  debtCents: number;
  debtPaymentCents: number;
  otherCents: Record<string, number>;
};

function componentSums(detail: ArkansasReceiptEntityDetail | undefined): ComponentSums {
  const sums: ComponentSums = {
    contributionMonetaryCents: 0,
    contributionNonmoneyCents: 0,
    loanCents: 0,
    returnContributionMonetaryCents: 0,
    returnContributionNonmoneyCents: 0,
    expenditureCents: 0,
    returnExpenditureCents: 0,
    loanPaymentCents: 0,
    debtCents: 0,
    debtPaymentCents: 0,
    otherCents: {},
  };
  if (!detail) return sums;
  for (const [key, money] of Object.entries(detail.byTypeSubTypeAmended)) {
    const [type = "", subType = ""] = key.split("|");
    const cents = money.amountCents;
    if (type === "Contribution" && subType.includes("Nonmoney")) sums.contributionNonmoneyCents += cents;
    else if (type === "Contribution") sums.contributionMonetaryCents += cents;
    else if (type === "Loan") sums.loanCents += cents;
    else if (type === "Return Contribution" && subType.includes("Nonmoney"))
      sums.returnContributionNonmoneyCents += cents;
    else if (type === "Return Contribution") sums.returnContributionMonetaryCents += cents;
    else if (type === "Expenditure") sums.expenditureCents += cents;
    else if (type === "Return Expenditure") sums.returnExpenditureCents += cents;
    else if (type === "Loan Payment") sums.loanPaymentCents += cents;
    else if (type === "Debt") sums.debtCents += cents;
    else if (type === "Debt Payment") sums.debtPaymentCents += cents;
    else sums.otherCents[key] = (sums.otherCents[key] ?? 0) + cents;
  }
  return sums;
}

export type ArkansasTotalsFormulaResult = {
  formula: string;
  cents: number;
  deltaCents: number;
};

export type ArkansasRegistrationTotalsReconciliation = {
  filingEntityId: number;
  registrationGuid: string;
  totalRaisedCents: number;
  totalSpentCents: number;
  components: ComponentSums;
  raisedFormulas: ArkansasTotalsFormulaResult[];
  spentFormulas: ArkansasTotalsFormulaResult[];
  raisedExactFormulas: string[];
  spentExactFormulas: string[];
};

// Candidate formulas for what the server-computed registration totals include.
// Gate 2 publishes which formula(s) reconcile exactly across the gold set.
export function reconcileArkansasRegistrationTotals(input: {
  registration: ArkansasFilerRegistrationRow;
  receiptDetail: ArkansasReceiptEntityDetail | undefined;
  expenditureDetail: ArkansasReceiptEntityDetail | undefined;
}): ArkansasRegistrationTotalsReconciliation {
  const receiptSums = componentSums(input.receiptDetail);
  const expenditureSums = componentSums(input.expenditureDetail);
  const components: ComponentSums = {
    ...receiptSums,
    expenditureCents: expenditureSums.expenditureCents,
    returnExpenditureCents: expenditureSums.returnExpenditureCents,
    loanPaymentCents: expenditureSums.loanPaymentCents,
    debtCents: expenditureSums.debtCents,
    debtPaymentCents: expenditureSums.debtPaymentCents,
    otherCents: { ...receiptSums.otherCents, ...expenditureSums.otherCents },
  };

  const totalRaisedCents = centsFromArkansasApiAmount(input.registration.totalRaised);
  const totalSpentCents = centsFromArkansasApiAmount(input.registration.totalSpent);

  const monetary = components.contributionMonetaryCents;
  const nonmoney = components.contributionNonmoneyCents;
  const loans = components.loanCents;
  const returns = components.returnContributionMonetaryCents + components.returnContributionNonmoneyCents;

  const raisedCandidates: Array<[string, number]> = [
    ["monetary", monetary],
    ["monetary_plus_nonmoney", monetary + nonmoney],
    ["monetary_plus_loans", monetary + loans],
    ["monetary_plus_nonmoney_plus_loans", monetary + nonmoney + loans],
    ["monetary_minus_returns", monetary - returns],
    ["monetary_plus_nonmoney_minus_returns", monetary + nonmoney - returns],
    ["monetary_plus_loans_minus_returns", monetary + loans - returns],
  ];

  const spend = components.expenditureCents;
  const returnExpenditure = components.returnExpenditureCents;
  const loanPayments = components.loanPaymentCents;
  const debtPayments = components.debtPaymentCents;

  const spentCandidates: Array<[string, number]> = [
    ["expenditure", spend],
    ["expenditure_minus_returns", spend - returnExpenditure],
    ["expenditure_plus_loan_payments", spend + loanPayments],
    ["expenditure_plus_loan_payments_minus_returns", spend + loanPayments - returnExpenditure],
    [
      "expenditure_plus_loan_and_debt_payments_minus_returns",
      spend + loanPayments + debtPayments - returnExpenditure,
    ],
  ];

  const evaluate = (candidates: Array<[string, number]>, target: number): ArkansasTotalsFormulaResult[] =>
    candidates.map(([formula, cents]) => ({ formula, cents, deltaCents: cents - target }));

  const raisedFormulas = evaluate(raisedCandidates, totalRaisedCents);
  const spentFormulas = evaluate(spentCandidates, totalSpentCents);

  return {
    filingEntityId: input.registration.filerEntityId,
    registrationGuid: input.registration.registrationGuid,
    totalRaisedCents,
    totalSpentCents,
    components,
    raisedFormulas,
    spentFormulas,
    raisedExactFormulas: raisedFormulas.filter((entry) => entry.deltaCents === 0).map((entry) => entry.formula),
    spentExactFormulas: spentFormulas.filter((entry) => entry.deltaCents === 0).map((entry) => entry.formula),
  };
}

// ---------------------------------------------------------------------------
// Gate 3 — CSV vs transaction-search completeness for one registration.
// ---------------------------------------------------------------------------

export type ArkansasTransactionSearchSummary = {
  registrationGuid: string;
  rowCount: number;
  amountCents: number;
  hasChildRowCount: number;
  duplicateGuidCount: number;
};

export function summarizeArkansasTransactionRows(
  registrationGuid: string,
  rows: readonly ArkansasTransactionRow[]
): ArkansasTransactionSearchSummary {
  const guids = new Set<string>();
  let duplicateGuidCount = 0;
  let amountCents = 0;
  let hasChildRowCount = 0;
  for (const row of rows) {
    if (row.filerRegistrationGuid !== registrationGuid) {
      throw new Error(
        `Arkansas transaction search was not exact: expected registration ${registrationGuid}, received ${row.filerRegistrationGuid}`
      );
    }
    if (guids.has(row.guid)) duplicateGuidCount += 1;
    guids.add(row.guid);
    amountCents += centsFromArkansasApiAmount(row.transactionAmount);
    if (row.hasChild) hasChildRowCount += 1;
  }
  return {
    registrationGuid,
    rowCount: rows.length,
    amountCents,
    hasChildRowCount,
    duplicateGuidCount,
  };
}

// ---------------------------------------------------------------------------
// Gate 4 — amendment lineage from the filed-reports endpoint.
// ---------------------------------------------------------------------------

export type ArkansasFiledReportLineageSummary = {
  registrationGuid: string;
  reportCount: number;
  versionCounts: Record<string, number>;
  reportsWithVersionAboveOne: number;
  reportNames: string[];
};

export function summarizeArkansasFiledReports(
  registrationGuid: string,
  rows: readonly ArkansasFiledReportRow[]
): ArkansasFiledReportLineageSummary {
  const scoped = rows.filter((row) => row.filerRegistrationGuid === registrationGuid);
  const versionCounts: Record<string, number> = {};
  let reportsWithVersionAboveOne = 0;
  for (const row of scoped) {
    versionCounts[row.reportVersion] = (versionCounts[row.reportVersion] ?? 0) + 1;
    if (row.filerReportVersionId > 1) reportsWithVersionAboveOne += 1;
  }
  return {
    registrationGuid,
    reportCount: scoped.length,
    versionCounts,
    reportsWithVersionAboveOne,
    reportNames: scoped.map((row) => `${row.reportName} [${row.reportVersion}]`).sort(),
  };
}

// ---------------------------------------------------------------------------
// Gate 5 — multi-cycle filers from the registration sweep.
// ---------------------------------------------------------------------------

export type ArkansasMultiCycleEntity = {
  filingEntityId: number;
  filerType: string;
  registrations: Array<{
    registrationGuid: string;
    electionYear: number | null;
    filerEntityVersionId: number;
    office: string | null;
  }>;
};

export function findArkansasMultiCycleCandidates(
  registrations: readonly ArkansasFilerRegistrationRow[],
  limit = 10
): ArkansasMultiCycleEntity[] {
  const byEntity = new Map<number, ArkansasFilerRegistrationRow[]>();
  for (const row of registrations) {
    if (row.filerTypeCode === "SFIFILER") continue;
    const rows = byEntity.get(row.filerEntityId) ?? [];
    rows.push(row);
    byEntity.set(row.filerEntityId, rows);
  }
  const result: ArkansasMultiCycleEntity[] = [];
  for (const [filingEntityId, rows] of byEntity) {
    const candidateRows = rows.filter((row) => row.filerType === "Candidate");
    if (candidateRows.length === 0) continue;
    const distinctYears = new Set(candidateRows.map((row) => row.electionYear).filter((year) => year !== null));
    if (distinctYears.size < 2 && !candidateRows.some((row) => row.filerEntityVersionId > 1)) continue;
    result.push({
      filingEntityId,
      filerType: candidateRows[0]!.filerType,
      registrations: candidateRows
        .map((row) => ({
          registrationGuid: row.registrationGuid,
          electionYear: row.electionYear,
          filerEntityVersionId: row.filerEntityVersionId,
          office: row.office,
        }))
        .sort((left, right) => (left.electionYear ?? 0) - (right.electionYear ?? 0)),
    });
    if (result.length >= limit) break;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Gate 7 — office vocabulary.
// ---------------------------------------------------------------------------

export function summarizeArkansasOfficeVocabulary(
  offices: readonly { value: string; name: string }[],
  requiredOffices: readonly string[]
): { officeCount: number; missingRequiredOffices: string[]; names: string[] } {
  const names = offices.map((office) => office.name).sort();
  const nameSet = new Set(names);
  return {
    officeCount: offices.length,
    missingRequiredOffices: requiredOffices.filter((name) => !nameSet.has(name)),
    names,
  };
}

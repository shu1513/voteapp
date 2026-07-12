import { readCsvObjects, type CsvObject } from "../../utils/csvObjects.js";
import type { NewYorkCityCfbBoroughCode, NewYorkCityCfbOfficeCode } from "./newYorkCityFinanceEligibleOffices.js";

export const NEW_YORK_CITY_CFB_DATA_LIBRARY_URL = "https://www.nyccfb.info/follow-the-money/data-library/";

const CONTRIBUTION_HEADERS = [
  "ELECTION", "OFFICECD", "RECIPID", "RECIPNAME", "FILING", "SCHEDULE", "REFNO", "NAME", "C_CODE",
  "OCCUPATION", "EMPNAME", "AMNT", "ADJTYPECD",
] as const;

const FINANCIAL_ANALYSIS_HEADERS = [
  "el_cycle", "from_stmt", "to_stmt", "office", "cand_name", "cand_id", "boro_dist", "net_cntns",
  "pubfnd_pmt", "net_expnd", "outstanding_bills",
] as const;

export type NewYorkCityCfbContributionRow = {
  electionYear: number;
  officeCode: NewYorkCityCfbOfficeCode;
  candidateId: string;
  candidateName: string;
  filing: number;
  schedule: string;
  referenceNumber: string;
  contributorName: string;
  contributorType: string;
  occupation: string | null;
  employer: string | null;
  amount: number;
  adjustmentType: string | null;
};

export type NewYorkCityCfbFinancialAnalysisRow = {
  electionYear: number;
  fromStatement: number;
  toStatement: number;
  officeCode: NewYorkCityCfbOfficeCode;
  candidateName: string;
  candidateId: string;
  boroughCode: NewYorkCityCfbBoroughCode | null;
  privateContributions: number | null;
  publicFunds: number | null;
  netExpenditures: number | null;
  outstandingBills: number | null;
};

function text(row: CsvObject, name: string): string {
  return row[name]?.replace(/\0/g, "").trim() ?? "";
}

function integer(row: CsvObject, name: string): number | null {
  const value = Number.parseInt(text(row, name), 10);
  return Number.isInteger(value) ? value : null;
}

function money(row: CsvObject, name: string): number | null {
  const raw = text(row, name).replace(/[$,]/g, "");
  if (!raw) {
    return null;
  }
  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function signedMoney(row: CsvObject, name: string): number | null {
  const raw = text(row, name).replace(/[$,]/g, "");
  if (!raw) return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function officeCode(value: string): NewYorkCityCfbOfficeCode | null {
  return value === "1" || value === "2" || value === "3" || value === "4" ? value : null;
}

function boroughCode(value: string): NewYorkCityCfbBoroughCode | null {
  const normalized = value.trim().replace(/[()]/g, "").toUpperCase();
  if (normalized === "BX" || normalized === "BRONX") return "X";
  if (normalized === "BK" || normalized === "BROOKLYN") return "K";
  if (normalized === "MANHATTAN") return "M";
  if (normalized === "QUEENS") return "Q";
  if (normalized === "SI" || normalized === "STATEN ISLAND") return "S";
  return normalized === "X" || normalized === "K" || normalized === "M" || normalized === "Q" || normalized === "S"
    ? normalized
    : null;
}

export function mapNewYorkCityCfbContributionRow(row: CsvObject): NewYorkCityCfbContributionRow | null {
  const electionYear = integer(row, "ELECTION");
  const parsedOfficeCode = officeCode(text(row, "OFFICECD"));
  const filing = integer(row, "FILING");
  const amount = signedMoney(row, "AMNT");
  const candidateId = text(row, "RECIPID");
  const candidateName = text(row, "RECIPNAME");
  const referenceNumber = text(row, "REFNO");
  if (!electionYear || !parsedOfficeCode || filing === null || amount === null || !candidateId || !candidateName || !referenceNumber) {
    return null;
  }
  return {
    electionYear,
    officeCode: parsedOfficeCode,
    candidateId,
    candidateName,
    filing,
    schedule: text(row, "SCHEDULE").toUpperCase(),
    referenceNumber,
    contributorName: text(row, "NAME"),
    contributorType: text(row, "C_CODE").toUpperCase(),
    occupation: text(row, "OCCUPATION") || null,
    employer: text(row, "EMPNAME") || null,
    amount,
    adjustmentType: text(row, "ADJTYPECD") || null,
  };
}

export function mapNewYorkCityCfbFinancialAnalysisRow(row: CsvObject): NewYorkCityCfbFinancialAnalysisRow | null {
  const electionYear = integer(row, "el_cycle");
  const fromStatement = integer(row, "from_stmt");
  const toStatement = integer(row, "to_stmt");
  const parsedOfficeCode = officeCode(text(row, "office"));
  const candidateId = text(row, "cand_id");
  const candidateName = text(row, "cand_name");
  if (!electionYear || fromStatement === null || toStatement === null || !parsedOfficeCode || !candidateId || !candidateName) {
    return null;
  }
  const parsedBorough = boroughCode(text(row, "boro_dist"));
  if (parsedOfficeCode === "4" && !parsedBorough) {
    return null;
  }
  return {
    electionYear,
    fromStatement,
    toStatement,
    officeCode: parsedOfficeCode,
    candidateName,
    candidateId,
    boroughCode: parsedOfficeCode === "4" ? parsedBorough : null,
    privateContributions: money(row, "net_cntns"),
    publicFunds: money(row, "pubfnd_pmt"),
    netExpenditures: money(row, "net_expnd"),
    outstandingBills: money(row, "outstanding_bills"),
  };
}

export async function readNewYorkCityCfbContributions(input: {
  filePath: string;
  candidateIds?: ReadonlySet<string>;
}): Promise<{ rows: NewYorkCityCfbContributionRow[]; rawRowCount: number; malformedRowCount: number }> {
  const rows: NewYorkCityCfbContributionRow[] = [];
  let malformedRowCount = 0;
  const result = await readCsvObjects({
    filePath: input.filePath,
    requiredHeaders: CONTRIBUTION_HEADERS,
    onRow: (raw) => {
      const candidateId = text(raw, "RECIPID");
      if (input.candidateIds && !input.candidateIds.has(candidateId)) {
        return;
      }
      const row = mapNewYorkCityCfbContributionRow(raw);
      if (row) rows.push(row);
      else malformedRowCount += 1;
    },
  });
  return {
    rows,
    rawRowCount: result.rowCount + result.malformedRowCount,
    malformedRowCount: malformedRowCount + result.malformedRowCount,
  };
}

export async function readNewYorkCityCfbFinancialAnalysis(input: {
  filePath: string;
  candidateIds?: ReadonlySet<string>;
}): Promise<{ rows: NewYorkCityCfbFinancialAnalysisRow[]; rawRowCount: number; malformedRowCount: number }> {
  const rows: NewYorkCityCfbFinancialAnalysisRow[] = [];
  let malformedRowCount = 0;
  const result = await readCsvObjects({
    filePath: input.filePath,
    requiredHeaders: FINANCIAL_ANALYSIS_HEADERS,
    onRow: (raw) => {
      const candidateId = text(raw, "cand_id");
      if (input.candidateIds && !input.candidateIds.has(candidateId)) {
        return;
      }
      if (!officeCode(text(raw, "office"))) {
        return;
      }
      const row = mapNewYorkCityCfbFinancialAnalysisRow(raw);
      if (row) rows.push(row);
      else malformedRowCount += 1;
    },
  });
  return {
    rows,
    rawRowCount: result.rowCount + result.malformedRowCount,
    malformedRowCount: malformedRowCount + result.malformedRowCount,
  };
}

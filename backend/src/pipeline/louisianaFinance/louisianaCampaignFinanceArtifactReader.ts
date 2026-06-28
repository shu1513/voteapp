import { createReadStream } from "node:fs";
import { StringDecoder } from "node:string_decoder";

import {
  LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME,
  LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURES_CSV_FILE_NAME,
} from "./louisianaCampaignFinanceArtifactCache.js";

export type LouisianaCampaignFinanceCsvRow = Record<string, string>;
export type LouisianaCampaignFinanceCsvRowPredicate = (row: LouisianaCampaignFinanceCsvRow) => boolean;

export const LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS = [
  "FilerNumber",
  "FilerLastName",
  "FilerFirstName",
  "ReportCode",
  "ReportType",
  "ReportNumber",
  "ContributorTypeCode",
  "ContributorName",
  "ContributorAddr1",
  "ContributorAddr2",
  "ContributorCity",
  "ContributorrState",
  "ContributorZip",
  "ContributionType",
  "ContributionDescription",
  "ContributionDate",
  "ContributionAmt",
  "ContributionDesignatedElectionAdditionInfo",
] as const;

export const LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS = [
  "FilerNumber",
  "FilerLastName",
  "FilerFirstName",
  "ReportCode",
  "ReportType",
  "ReportNumber",
  "Schedule",
  "RecipientName",
  "RecipientAddr1",
  "RecipientAddr2",
  "RecipientCity",
  "RecipientState",
  "RecipientZip",
  "ExpenditureDescription",
  "CandidateBeneficiary",
  "ExpenditureDate",
  "ExpenditureAmt",
] as const;

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Louisiana campaign finance ${fieldName}: ${value}`);
  }
  return value;
}

export function normalizeLouisianaCampaignFinanceHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function buildHeader(cells: readonly string[]): string[] {
  const header = cells.map(normalizeLouisianaCampaignFinanceHeader);
  const seen = new Set<string>();
  for (const name of header) {
    if (!name) {
      continue;
    }
    if (seen.has(name)) {
      throw new Error(`Duplicate Louisiana campaign finance CSV header: ${name}`);
    }
    seen.add(name);
  }
  if (seen.size === 0) {
    throw new Error("Louisiana campaign finance CSV header row is empty");
  }
  return header;
}

function assertRequiredHeaders(headers: readonly string[], requiredHeaders: readonly string[], label: string): void {
  const headerSet = new Set(headers);
  const missing = requiredHeaders.filter((header) => !headerSet.has(header));
  if (missing.length > 0) {
    throw new Error(`Louisiana campaign finance ${label} CSV missing required headers: ${missing.join(", ")}`);
  }
}

function rowObjectFromCells(headers: readonly string[], row: readonly string[]): LouisianaCampaignFinanceCsvRow {
  return Object.fromEntries(headers.map((header, index) => [header || `column_${index + 1}`, row[index] ?? ""]));
}

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let pendingQuoteInQuotedField = false;

  const finishRow = (): void => {
    row.push(field);
    if (row.some((value) => value.trim().length > 0)) {
      rows.push(row);
    }
    row = [];
    field = "";
  };

  const processText = (text: string, isFinal = false): void => {
    let index = 0;
    if (pendingQuoteInQuotedField) {
      pendingQuoteInQuotedField = false;
      if (text[0] === '"') {
        field += '"';
        index = 1;
      } else {
        inQuotes = false;
      }
    }

    for (; index < text.length; index += 1) {
      const char = text[index];
      const next = text[index + 1];

      if (inQuotes) {
        if (char === '"' && next === '"') {
          field += '"';
          index += 1;
        } else if (char === '"' && next === undefined && !isFinal) {
          pendingQuoteInQuotedField = true;
        } else if (char === '"') {
          inQuotes = false;
        } else {
          field += char;
        }
        continue;
      }

      if (char === '"') {
        inQuotes = true;
        continue;
      }

      if (char === ",") {
        row.push(field);
        field = "";
        continue;
      }

      if (char === "\n") {
        finishRow();
        continue;
      }

      if (char === "\r") {
        continue;
      }

      field += char;
    }
  };

  processText(csv, true);
  if (inQuotes || pendingQuoteInQuotedField) {
    throw new Error("Louisiana campaign finance CSV has an unterminated quoted field");
  }
  if (field.length > 0 || row.length > 0) {
    finishRow();
  }

  return rows;
}

function parseRowsForCsv(
  csv: string,
  requiredHeaders: readonly string[],
  label: "contribution" | "expenditure"
): LouisianaCampaignFinanceCsvRow[] {
  const rows = parseCsvRows(csv);
  const headerCells = rows[0];
  if (!headerCells) {
    throw new Error(`Louisiana campaign finance ${label} CSV missing header row`);
  }
  const headers = buildHeader(headerCells);
  assertRequiredHeaders(headers, requiredHeaders, label);
  return rows.slice(1).map((cells) => rowObjectFromCells(headers, cells));
}

async function readLouisianaCampaignFinanceCsvRowsFromFile(input: {
  filePath: string;
  requiredHeaders: readonly string[];
  label: "contribution" | "expenditure";
  predicate?: LouisianaCampaignFinanceCsvRowPredicate;
  maxRows?: number;
}): Promise<LouisianaCampaignFinanceCsvRow[]> {
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const source = createReadStream(input.filePath);
  const decoder = new StringDecoder("utf8");

  return await new Promise((resolve, reject) => {
    const rows: LouisianaCampaignFinanceCsvRow[] = [];
    let headers: string[] | null = null;
    let row: string[] = [];
    let field = "";
    let inQuotes = false;
    let pendingQuoteInQuotedField = false;
    let settled = false;

    const rejectOnce = (error: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      reject(error);
    };

    const resolveOnce = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(rows);
    };

    const finishRow = (): void => {
      row.push(field);
      const cells = row;
      row = [];
      field = "";

      if (!cells.some((value) => value.trim().length > 0)) {
        return;
      }
      if (!headers) {
        headers = buildHeader(cells);
        assertRequiredHeaders(headers, input.requiredHeaders, input.label);
        return;
      }
      if (maxRows !== undefined && rows.length >= maxRows) {
        return;
      }

      const parsedRow = rowObjectFromCells(headers, cells);
      if (!input.predicate || input.predicate(parsedRow)) {
        rows.push(parsedRow);
        if (maxRows !== undefined && rows.length >= maxRows) {
          resolveOnce();
          source.destroy();
        }
      }
    };

    const processText = (text: string, isFinal = false): void => {
      let index = 0;
      if (pendingQuoteInQuotedField) {
        pendingQuoteInQuotedField = false;
        if (text[0] === '"') {
          field += '"';
          index = 1;
        } else {
          inQuotes = false;
        }
      }

      for (; index < text.length && !settled; index += 1) {
        const char = text[index];
        const next = text[index + 1];

        if (inQuotes) {
          if (char === '"' && next === '"') {
            field += '"';
            index += 1;
          } else if (char === '"' && next === undefined && !isFinal) {
            pendingQuoteInQuotedField = true;
          } else if (char === '"') {
            inQuotes = false;
          } else {
            field += char;
          }
          continue;
        }

        if (char === '"') {
          inQuotes = true;
          continue;
        }

        if (char === ",") {
          row.push(field);
          field = "";
          continue;
        }

        if (char === "\n") {
          finishRow();
          continue;
        }

        if (char === "\r") {
          continue;
        }

        field += char;
      }
    };

    source.on("data", (chunk: string | Buffer) => {
      if (settled) {
        return;
      }
      try {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        processText(decoder.write(buffer));
      } catch (error) {
        rejectOnce(error as Error);
        source.destroy(error as Error);
      }
    });

    source.on("end", () => {
      if (settled) {
        return;
      }
      try {
        processText(decoder.end(), true);
        if (inQuotes || pendingQuoteInQuotedField) {
          rejectOnce(new Error("Louisiana campaign finance CSV has an unterminated quoted field"));
          return;
        }
        if (field.length > 0 || row.length > 0) {
          finishRow();
        }
        if (!headers) {
          rejectOnce(new Error(`Louisiana campaign finance ${input.label} CSV missing header row`));
          return;
        }
        resolveOnce();
      } catch (error) {
        rejectOnce(error as Error);
      }
    });

    source.on("error", (error: Error) => rejectOnce(error));
  });
}

export function parseLouisianaCampaignFinanceContributionCsvRows(csv: string): LouisianaCampaignFinanceCsvRow[] {
  return parseRowsForCsv(csv, LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS, "contribution");
}

export function parseLouisianaCampaignFinanceExpenditureCsvRows(csv: string): LouisianaCampaignFinanceCsvRow[] {
  return parseRowsForCsv(csv, LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS, "expenditure");
}

export async function readLouisianaCampaignFinanceContributionRows(input: {
  filePath: string;
  predicate?: LouisianaCampaignFinanceCsvRowPredicate;
  maxRows?: number;
}): Promise<LouisianaCampaignFinanceCsvRow[]> {
  return await readLouisianaCampaignFinanceCsvRowsFromFile({
    ...input,
    requiredHeaders: LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS,
    label: "contribution",
  });
}

export async function readLouisianaCampaignFinanceExpenditureRows(input: {
  filePath: string;
  predicate?: LouisianaCampaignFinanceCsvRowPredicate;
  maxRows?: number;
}): Promise<LouisianaCampaignFinanceCsvRow[]> {
  return await readLouisianaCampaignFinanceCsvRowsFromFile({
    ...input,
    requiredHeaders: LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS,
    label: "expenditure",
  });
}

export {
  LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME,
  LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURES_CSV_FILE_NAME,
} from "./louisianaCampaignFinanceArtifactCache.js";

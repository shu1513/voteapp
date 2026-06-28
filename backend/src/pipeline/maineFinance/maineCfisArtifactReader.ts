import { createReadStream } from "node:fs";
import { StringDecoder } from "node:string_decoder";

export const MAINE_CFIS_CONTRIBUTION_COLUMNS = [
  "OrgID",
  "LegacyID",
  "Committee Name",
  "Candidate Name",
  "Receipt Amount",
  "Receipt Date",
  "Office",
  "District",
  "Last Name",
  "First Name",
  "Middle Name",
  "Suffix",
  "Address1",
  "Address2",
  "City",
  "State",
  "Zip",
  "Description",
  "Receipt ID",
  "Filed Date",
  "Report Name",
  "Receipt Source Type",
  "Receipt Type",
  "Committee Type",
  "Amended",
  "Employer",
  "Occupation",
  "Occupation Comment",
  "Employment Information Requested",
  "Forgiven Loan",
  "ElectionType",
] as const;

export const MAINE_CFIS_EXPENDITURE_COLUMNS = [
  "Election Year",
  "OrgID",
  "LegacyID",
  "Committee Type",
  "Committee Name",
  "Candidate Name",
  "Jurisdiction",
  "Office",
  "District",
  "Party",
  "IncumbentStatus",
  "Financing Type",
  "Payee Last Name",
  "Payee First Name",
  "Payee Middle Name",
  "Suffix",
  "Address1",
  "Address2",
  "City",
  "State",
  "Zip",
  "Expenditure ID",
  "Expenditure Date",
  "Expenditure Purpose",
  "Expenditure Amount",
  "Explanation",
  "Date Filed",
  "Amended",
  "IE Report",
  "24-Hour Report",
  "Report Name",
  "Operating Expense",
  "Support/Oppose Ballot Question",
  "Support/Oppose Candidate",
  "Ballot Question Number",
  "Ballot Question Description/Title",
  "Candidate",
  "Candidate ID",
  "Candidate Jurisdiction",
  "Candidate Office",
  "Candidate District",
  "Candidate Party",
  "Candidate IncumbentStatus",
  "Candidate Financing Type",
] as const;

export type MaineCfisContributionColumn = (typeof MAINE_CFIS_CONTRIBUTION_COLUMNS)[number];
export type MaineCfisExpenditureColumn = (typeof MAINE_CFIS_EXPENDITURE_COLUMNS)[number];

export type MaineCfisContributionRow = Record<MaineCfisContributionColumn, string>;
export type MaineCfisExpenditureRow = Record<MaineCfisExpenditureColumn, string>;

export type MaineCfisContributionRowPredicate = (row: MaineCfisContributionRow) => boolean;
export type MaineCfisExpenditureRowPredicate = (row: MaineCfisExpenditureRow) => boolean;

type MaineCfisRowForColumns<TColumns extends readonly string[]> = Record<TColumns[number], string>;

export function normalizeMaineCfisHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

export function parseMaineCfisMoney(value: string): number | null {
  const normalized = value.trim();
  if (!normalized) {
    return null;
  }
  const isParenthesized = normalized.startsWith("(") && normalized.endsWith(")");
  const withoutAdornment = normalized.replace(/[$,()\s]/g, "");
  if (!withoutAdornment) {
    return null;
  }
  const parsed = Number(withoutAdornment);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return isParenthesized ? -parsed : parsed;
}

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Maine CFIS ${fieldName}: ${value}`);
  }
  return value;
}

function cell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

function buildHeader(cells: readonly string[]): string[] {
  const header = cells.map(normalizeMaineCfisHeader);
  const seen = new Set<string>();
  for (const name of header) {
    if (!name) {
      continue;
    }
    if (seen.has(name) && name !== "Jurisdiction") {
      throw new Error(`Duplicate Maine CFIS CSV header: ${name}`);
    }
    seen.add(name);
  }
  if (seen.size === 0) {
    throw new Error("Maine CFIS CSV header row is empty");
  }
  return header;
}

function buildHeaderIndex(header: readonly string[]): Map<string, number> {
  const headerIndex = new Map<string, number>();
  header.forEach((name, index) => {
    const normalizedName = normalizeMaineCfisHeader(name);
    if (normalizedName && !headerIndex.has(normalizedName)) {
      headerIndex.set(normalizedName, index);
    }
  });
  return headerIndex;
}

function allHeaderIndexes(header: readonly string[], column: string): number[] {
  const normalizedColumn = normalizeMaineCfisHeader(column);
  return header
    .map((name, index) => ({ name: normalizeMaineCfisHeader(name), index }))
    .filter((item) => item.name === normalizedColumn)
    .map((item) => item.index);
}

function requireColumn(input: {
  header: readonly string[];
  headerIndex: ReadonlyMap<string, number>;
  column: string;
  label: string;
}): number {
  if (input.label === "expenditure" && input.column === "Candidate Jurisdiction") {
    const jurisdictionIndexes = allHeaderIndexes(input.header, "Jurisdiction");
    const index = jurisdictionIndexes[1];
    if (index !== undefined) {
      return index;
    }
  }
  const index = input.headerIndex.get(input.column);
  if (index === undefined) {
    throw new Error(`Missing required Maine CFIS ${input.label} CSV column: ${input.column}`);
  }
  return index;
}

function indexesForColumns<TColumns extends readonly string[]>(
  header: readonly string[],
  columns: TColumns,
  label: string
): Record<TColumns[number], number> {
  const headerIndex = buildHeaderIndex(header);
  return Object.fromEntries(
    columns.map((column) => [column, requireColumn({ header, headerIndex, column, label })])
  ) as Record<TColumns[number], number>;
}

function rowObjectFromCells<TColumns extends readonly string[]>(
  cells: readonly string[],
  columns: TColumns,
  indexes: Record<TColumns[number], number>
): MaineCfisRowForColumns<TColumns> {
  const entries = (columns as readonly TColumns[number][]).map((column) => [column, cell(cells, indexes[column])]);
  return Object.fromEntries(entries) as MaineCfisRowForColumns<TColumns>;
}

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
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
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if (char === "\r") {
      continue;
    }
    field += char;
  }

  if (inQuotes) {
    throw new Error("Maine CFIS CSV has an unterminated quoted field");
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((cells) => cells.some((value) => value.trim().length > 0));
}

function parseRowsForColumns<TColumns extends readonly string[]>(input: {
  csv: string;
  columns: TColumns;
  label: string;
}): MaineCfisRowForColumns<TColumns>[] {
  const rows = parseCsvRows(input.csv);
  const headerCells = rows[0];
  if (!headerCells) {
    throw new Error(`Missing Maine CFIS ${input.label} CSV header row`);
  }
  const indexes = indexesForColumns(buildHeader(headerCells), input.columns, input.label);
  return rows.slice(1).map((cells) => rowObjectFromCells(cells, input.columns, indexes));
}

export function parseMaineCfisContributionCsvRows(csv: string): MaineCfisContributionRow[] {
  return parseRowsForColumns({
    csv,
    columns: MAINE_CFIS_CONTRIBUTION_COLUMNS,
    label: "contribution",
  });
}

export function parseMaineCfisExpenditureCsvRows(csv: string): MaineCfisExpenditureRow[] {
  return parseRowsForColumns({
    csv,
    columns: MAINE_CFIS_EXPENDITURE_COLUMNS,
    label: "expenditure",
  });
}

async function readMaineCfisRows<TColumns extends readonly string[]>(input: {
  filePath: string;
  columns: TColumns;
  label: string;
  predicate?: (row: MaineCfisRowForColumns<TColumns>) => boolean;
  maxRows?: number;
}): Promise<MaineCfisRowForColumns<TColumns>[]> {
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const source = createReadStream(input.filePath);
  const decoder = new StringDecoder("utf8");

  return await new Promise((resolve, reject) => {
    const rows: MaineCfisRowForColumns<TColumns>[] = [];
    let indexes: Record<TColumns[number], number> | null = null;
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

    const consumeCompletedRow = (cells: string[]): void => {
      if (!cells.some((value) => value.trim().length > 0)) {
        return;
      }
      if (!indexes) {
        indexes = indexesForColumns(buildHeader(cells), input.columns, input.label);
        return;
      }
      if (maxRows !== undefined && rows.length >= maxRows) {
        return;
      }

      const parsedRow = rowObjectFromCells(cells, input.columns, indexes);
      if (!input.predicate || input.predicate(parsedRow)) {
        rows.push(parsedRow);
        if (maxRows !== undefined && rows.length >= maxRows) {
          resolveOnce();
          source.destroy();
        }
      }
    };

    const finishCurrentRow = (): void => {
      row.push(field);
      consumeCompletedRow(row);
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
          finishCurrentRow();
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
          rejectOnce(new Error("Maine CFIS CSV has an unterminated quoted field"));
          return;
        }
        if (field.length > 0 || row.length > 0) {
          finishCurrentRow();
        }
        if (!indexes) {
          rejectOnce(new Error(`Missing Maine CFIS ${input.label} CSV header row`));
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

export async function readMaineCfisContributionRows(input: {
  filePath: string;
  predicate?: MaineCfisContributionRowPredicate;
  maxRows?: number;
}): Promise<MaineCfisContributionRow[]> {
  return readMaineCfisRows({
    filePath: input.filePath,
    columns: MAINE_CFIS_CONTRIBUTION_COLUMNS,
    label: "contribution",
    predicate: input.predicate,
    maxRows: input.maxRows,
  });
}

export async function readMaineCfisExpenditureRows(input: {
  filePath: string;
  predicate?: MaineCfisExpenditureRowPredicate;
  maxRows?: number;
}): Promise<MaineCfisExpenditureRow[]> {
  return readMaineCfisRows({
    filePath: input.filePath,
    columns: MAINE_CFIS_EXPENDITURE_COLUMNS,
    label: "expenditure",
    predicate: input.predicate,
    maxRows: input.maxRows,
  });
}

import { createReadStream } from "node:fs";
import { StringDecoder } from "node:string_decoder";

export const NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS = [
  "OrgID",
  "Transaction Amount",
  "Transaction Date",
  "Last Name",
  "First Name",
  "Middle Name",
  "Prefix",
  "Suffix",
  "Contributor Address Line 1",
  "Contributor Address Line 2",
  "Contributor City",
  "Contributor State",
  "Contributor Zip Code",
  "Description",
  "Check Number",
  "Transaction ID",
  "Filed Date",
  "Election",
  "Report Name",
  "Start of Period",
  "End of Period",
  "Contributor Code",
  "Contribution Type",
  "Report Entity Type",
  "Committee Name",
  "Candidate Last Name",
  "Candidate First Name",
  "Candidate Middle Name",
  "Candidate Prefix",
  "Candidate Suffix",
  "Amended",
  "Contributor Employer",
  "Contributor Occupation",
  "Occupation Comment",
  "Employment Information Requested",
] as const;

export const NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS = [
  "OrgID",
  "Expenditure Amount",
  "Expenditure Date",
  "Payee Last Name",
  "Payee First Name",
  "Payee Middle Name",
  "Payee Prefix",
  "Payee Suffix",
  "Payee Address 1",
  "Payee Address 2",
  "Payee City",
  "Payee State",
  "Payee Zip Code",
  "Description",
  "Expenditure ID",
  "Filed Date",
  "Election",
  "Report Name",
  "Start of Period",
  "End of Period",
  "Purpose",
  "Expenditure Type",
  "Reason",
  "Stance",
  "Report Entity Type",
  "Committee Name",
  "Candidate Last Name",
  "Candidate First Name",
  "Candidate Middle Name",
  "Candidate Prefix",
  "Candidate Suffix",
  "Amended",
] as const;

export type NewMexicoCfisContributionColumn = (typeof NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS)[number];
export type NewMexicoCfisExpenditureColumn = (typeof NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS)[number];

export type NewMexicoCfisContributionRow = Record<NewMexicoCfisContributionColumn, string>;
export type NewMexicoCfisExpenditureRow = Record<NewMexicoCfisExpenditureColumn, string>;

export type NewMexicoCfisContributionRowPredicate = (row: NewMexicoCfisContributionRow) => boolean;
export type NewMexicoCfisExpenditureRowPredicate = (row: NewMexicoCfisExpenditureRow) => boolean;

type CfisRowForColumns<TColumns extends readonly string[]> = Record<TColumns[number], string>;

export function normalizeNewMexicoCfisHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid New Mexico CFIS ${fieldName}: ${value}`);
  }
  return value;
}

function cell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

function buildHeader(cells: readonly string[]): string[] {
  const header = cells.map(normalizeNewMexicoCfisHeader);
  const seen = new Set<string>();
  for (const name of header) {
    if (!name) {
      continue;
    }
    if (seen.has(name)) {
      throw new Error(`Duplicate New Mexico CFIS CSV header: ${name}`);
    }
    seen.add(name);
  }
  if (seen.size === 0) {
    throw new Error("New Mexico CFIS CSV header row is empty");
  }
  return header;
}

function buildHeaderIndex(header: readonly string[]): Map<string, number> {
  return new Map(header.map((name, index) => [normalizeNewMexicoCfisHeader(name), index]));
}

function requireColumn(headerIndex: ReadonlyMap<string, number>, column: string, label: string): number {
  const index = headerIndex.get(column);
  if (index === undefined) {
    throw new Error(`Missing required New Mexico CFIS ${label} CSV column: ${column}`);
  }
  return index;
}

function indexesForColumns<TColumns extends readonly string[]>(
  header: readonly string[],
  columns: TColumns,
  label: string
): Record<TColumns[number], number> {
  const headerIndex = buildHeaderIndex(header);
  return Object.fromEntries(columns.map((column) => [column, requireColumn(headerIndex, column, label)])) as Record<
    TColumns[number],
    number
  >;
}

function rowObjectFromCells<TColumns extends readonly string[]>(
  cells: readonly string[],
  columns: TColumns,
  indexes: Record<TColumns[number], number>
): CfisRowForColumns<TColumns> {
  const entries = (columns as readonly TColumns[number][]).map((column) => [
    column,
    cell(cells, indexes[column]),
  ]);
  return Object.fromEntries(entries) as CfisRowForColumns<TColumns>;
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
    throw new Error("New Mexico CFIS CSV has an unterminated quoted field");
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
}): CfisRowForColumns<TColumns>[] {
  const rows = parseCsvRows(input.csv);
  const headerCells = rows[0];
  if (!headerCells) {
    return [];
  }
  const header = buildHeader(headerCells);
  const indexes = indexesForColumns(header, input.columns, input.label);
  return rows.slice(1).map((cells) => rowObjectFromCells(cells, input.columns, indexes));
}

export function parseNewMexicoCfisContributionCsvRows(csv: string): NewMexicoCfisContributionRow[] {
  return parseRowsForColumns({
    csv,
    columns: NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS,
    label: "contribution",
  });
}

export function parseNewMexicoCfisExpenditureCsvRows(csv: string): NewMexicoCfisExpenditureRow[] {
  return parseRowsForColumns({
    csv,
    columns: NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS,
    label: "expenditure",
  });
}

async function readNewMexicoCfisRows<TColumns extends readonly string[]>(input: {
  filePath: string;
  columns: TColumns;
  label: string;
  predicate?: (row: CfisRowForColumns<TColumns>) => boolean;
  maxRows?: number;
}): Promise<CfisRowForColumns<TColumns>[]> {
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const source = createReadStream(input.filePath);
  const decoder = new StringDecoder("utf8");

  return await new Promise((resolve, reject) => {
    const rows: CfisRowForColumns<TColumns>[] = [];
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
          rejectOnce(new Error("New Mexico CFIS CSV has an unterminated quoted field"));
          return;
        }
        if (field.length > 0 || row.length > 0) {
          finishCurrentRow();
        }
        resolveOnce();
      } catch (error) {
        rejectOnce(error as Error);
      }
    });

    source.on("error", (error: Error) => rejectOnce(error));
  });
}

export async function readNewMexicoCfisContributionRows(input: {
  filePath: string;
  predicate?: NewMexicoCfisContributionRowPredicate;
  maxRows?: number;
}): Promise<NewMexicoCfisContributionRow[]> {
  return await readNewMexicoCfisRows({
    filePath: input.filePath,
    columns: NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS,
    label: "contribution",
    predicate: input.predicate,
    maxRows: input.maxRows,
  });
}

export async function readNewMexicoCfisExpenditureRows(input: {
  filePath: string;
  predicate?: NewMexicoCfisExpenditureRowPredicate;
  maxRows?: number;
}): Promise<NewMexicoCfisExpenditureRow[]> {
  return await readNewMexicoCfisRows({
    filePath: input.filePath,
    columns: NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS,
    label: "expenditure",
    predicate: input.predicate,
    maxRows: input.maxRows,
  });
}

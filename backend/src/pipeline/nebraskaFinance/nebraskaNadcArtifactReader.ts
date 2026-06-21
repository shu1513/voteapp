import { createReadStream } from "node:fs";
import { open, stat } from "node:fs/promises";
import { StringDecoder } from "node:string_decoder";
import { createInflateRaw } from "node:zlib";

import {
  type NebraskaNadcArtifactKind,
  normalizeNebraskaNadcArtifactIdentity,
} from "./nebraskaNadcArtifactCache.js";

const EOCD_SIGNATURE = 0x06054b50;
const CENTRAL_DIRECTORY_SIGNATURE = 0x02014b50;
const LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;
const EOCD_MIN_LENGTH = 22;
const EOCD_MAX_COMMENT_LENGTH = 65_535;

type InternalZipEntry = {
  fileName: string;
  compressedSize: number;
  compressionMethodCode: number;
  isDirectory: boolean;
  localHeaderOffset: number;
  encrypted: boolean;
};

export const NEBRASKA_NADC_CONTRIBUTION_COLUMNS = [
  "Receipt ID",
  "Org ID",
  "Filer Type",
  "Filer Name",
  "Candidate Name",
  "Receipt Transaction/Contribution Type",
  "Other Funds Type",
  "Receipt Date",
  "Receipt Amount",
  "Description",
  "Contributor or Transaction Source Type",
  "Contributor or Source Name (Individual Last Name)",
  "First Name",
  "Middle Name",
  "Suffix",
  "Address 1",
  "Address 2",
  "City",
  "State",
  "Zip",
  "Filed Date",
  "Amended",
  "Employer",
  "Occupation",
] as const;

export const NEBRASKA_NADC_EXPENDITURE_COLUMNS = [
  "Expenditure ID",
  "Org ID",
  "Filer Type",
  "Filer Name",
  "Candidate Name",
  "Expenditure Transaction Type",
  "Expenditure Sub Type",
  "Expenditure Date",
  "Expenditure Amount",
  "Description",
  "Payee or Recipient or In-Kind Contributor Type",
  "Payee or Recipient or In-Kind Contributor Name",
  "First Name",
  "Middle Name",
  "Suffix",
  "Address 1",
  "Address 2",
  "City",
  "State",
  "Zip",
  "Filed Date",
  "Support Or Oppose",
  "Candidate Name or Ballot Issue",
  "Jurisdiction - Office - District or Ballot Description",
  "Amended",
  "Employer",
  "Occupation",
  "Principal Place of Business",
] as const;

export type NebraskaNadcContributionRow = Record<(typeof NEBRASKA_NADC_CONTRIBUTION_COLUMNS)[number], string>;
export type NebraskaNadcExpenditureRow = Record<(typeof NEBRASKA_NADC_EXPENDITURE_COLUMNS)[number], string>;
export type NebraskaNadcArtifactRow = NebraskaNadcContributionRow | NebraskaNadcExpenditureRow;

export type NebraskaNadcContributionRowPredicate = (row: NebraskaNadcContributionRow) => boolean;
export type NebraskaNadcExpenditureRowPredicate = (row: NebraskaNadcExpenditureRow) => boolean;

function officialArtifactPart(kind: NebraskaNadcArtifactKind): string {
  return kind === "contribution_loan" ? "ContributionLoan" : "Expenditure";
}

export function nebraskaNadcCsvFileName(input: { year: number; artifactKind: NebraskaNadcArtifactKind }): string {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  return `${artifact.year}_${officialArtifactPart(artifact.artifactKind)}Extract.csv`;
}

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Nebraska NADC ${fieldName}: ${value}`);
  }
  return value;
}

async function readFileRange(path: string, position: number, length: number): Promise<Buffer> {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(length);
    const { bytesRead } = await handle.read(buffer, 0, length, position);
    if (bytesRead !== length) {
      throw new Error(`Unable to read Nebraska NADC ZIP range at ${position}`);
    }
    return buffer;
  } finally {
    await handle.close();
  }
}

function findEndOfCentralDirectory(tail: Buffer): number {
  for (let offset = tail.length - EOCD_MIN_LENGTH; offset >= 0; offset -= 1) {
    if (tail.readUInt32LE(offset) === EOCD_SIGNATURE) {
      return offset;
    }
  }
  throw new Error("Nebraska NADC ZIP end-of-central-directory record not found");
}

async function readCentralDirectory(path: string): Promise<Buffer> {
  const fileStat = await stat(path);
  if (!fileStat.isFile()) {
    throw new Error(`Nebraska NADC ZIP path is not a file: ${path}`);
  }
  if (fileStat.size < EOCD_MIN_LENGTH) {
    throw new Error(`Nebraska NADC ZIP is too small: ${path}`);
  }

  const tailLength = Math.min(fileStat.size, EOCD_MIN_LENGTH + EOCD_MAX_COMMENT_LENGTH);
  const tail = await readFileRange(path, fileStat.size - tailLength, tailLength);
  const eocdOffset = findEndOfCentralDirectory(tail);

  const diskNumber = tail.readUInt16LE(eocdOffset + 4);
  const centralDirectoryDisk = tail.readUInt16LE(eocdOffset + 6);
  if (diskNumber !== 0 || centralDirectoryDisk !== 0) {
    throw new Error("Multi-disk Nebraska NADC ZIP archives are not supported");
  }

  const centralDirectorySize = tail.readUInt32LE(eocdOffset + 12);
  const centralDirectoryOffset = tail.readUInt32LE(eocdOffset + 16);
  if (centralDirectorySize === 0xffffffff || centralDirectoryOffset === 0xffffffff) {
    throw new Error("ZIP64 Nebraska NADC archives are not supported by the lightweight reader");
  }
  if (centralDirectoryOffset + centralDirectorySize > fileStat.size) {
    throw new Error("Nebraska NADC ZIP central directory points outside the archive");
  }

  return await readFileRange(path, centralDirectoryOffset, centralDirectorySize);
}

function parseCentralDirectory(buffer: Buffer): InternalZipEntry[] {
  const entries: InternalZipEntry[] = [];
  let offset = 0;

  while (offset < buffer.length) {
    if (buffer.readUInt32LE(offset) !== CENTRAL_DIRECTORY_SIGNATURE) {
      throw new Error(`Invalid Nebraska NADC ZIP central directory entry at offset ${offset}`);
    }

    const flags = buffer.readUInt16LE(offset + 8);
    const compressionMethodCode = buffer.readUInt16LE(offset + 10);
    const compressedSize = buffer.readUInt32LE(offset + 20);
    const fileNameLength = buffer.readUInt16LE(offset + 28);
    const extraFieldLength = buffer.readUInt16LE(offset + 30);
    const fileCommentLength = buffer.readUInt16LE(offset + 32);
    const localHeaderOffset = buffer.readUInt32LE(offset + 42);
    const fileNameStart = offset + 46;
    const fileNameEnd = fileNameStart + fileNameLength;
    const fileName = buffer.toString("utf8", fileNameStart, fileNameEnd).replace(/\\/g, "/");

    if (compressedSize === 0xffffffff || localHeaderOffset === 0xffffffff) {
      throw new Error(`ZIP64 Nebraska NADC entry is not supported: ${fileName}`);
    }

    entries.push({
      fileName,
      compressedSize,
      compressionMethodCode,
      isDirectory: fileName.endsWith("/"),
      localHeaderOffset,
      encrypted: (flags & 0x1) === 0x1,
    });

    offset = fileNameEnd + extraFieldLength + fileCommentLength;
  }

  return entries;
}

async function readZipEntries(path: string): Promise<InternalZipEntry[]> {
  return parseCentralDirectory(await readCentralDirectory(path));
}

async function readEntryDataOffset(zipPath: string, entry: InternalZipEntry): Promise<number> {
  const localHeader = await readFileRange(zipPath, entry.localHeaderOffset, 30);
  if (localHeader.readUInt32LE(0) !== LOCAL_FILE_HEADER_SIGNATURE) {
    throw new Error(`Invalid Nebraska NADC ZIP local header for ${entry.fileName}`);
  }
  const fileNameLength = localHeader.readUInt16LE(26);
  const extraFieldLength = localHeader.readUInt16LE(28);
  return entry.localHeaderOffset + 30 + fileNameLength + extraFieldLength;
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
    throw new Error("Nebraska NADC CSV has an unterminated quoted field");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((cells) => cells.some((cell) => cell.trim().length > 0));
}

function normalizeCsvHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function buildHeaderIndex(header: readonly string[]): Map<string, number> {
  return new Map(header.map((name, index) => [normalizeCsvHeader(name), index]));
}

function requireColumn(headerIndex: ReadonlyMap<string, number>, column: string): number {
  const index = headerIndex.get(column);
  if (index === undefined) {
    throw new Error(`Missing required Nebraska NADC CSV column: ${column}`);
  }
  return index;
}

function cell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

function rowObjectFromCells<const Columns extends readonly string[]>(
  cells: readonly string[],
  columns: Columns,
  indexes: Record<Columns[number], number>
): Record<Columns[number], string> {
  const row = {} as Record<Columns[number], string>;
  for (const column of columns as readonly Columns[number][]) {
    row[column] = cell(cells, indexes[column]);
  }
  return row;
}

function parseNebraskaNadcCsvForColumns<const Columns extends readonly string[]>(
  csv: string,
  columns: Columns
): Record<Columns[number], string>[] {
  const rows = parseCsvRows(csv);
  const header = rows[0];
  if (!header) {
    return [];
  }

  const headerIndex = buildHeaderIndex(header);
  const indexes = Object.fromEntries(columns.map((column) => [column, requireColumn(headerIndex, column)])) as Record<
    Columns[number],
    number
  >;

  return rows.slice(1).map((cells) => rowObjectFromCells(cells, columns, indexes));
}

export function parseNebraskaNadcContributionCsv(csv: string): NebraskaNadcContributionRow[] {
  return parseNebraskaNadcCsvForColumns(csv, NEBRASKA_NADC_CONTRIBUTION_COLUMNS) as NebraskaNadcContributionRow[];
}

export function parseNebraskaNadcExpenditureCsv(csv: string): NebraskaNadcExpenditureRow[] {
  return parseNebraskaNadcCsvForColumns(csv, NEBRASKA_NADC_EXPENDITURE_COLUMNS) as NebraskaNadcExpenditureRow[];
}

async function streamNebraskaNadcRows<const Columns extends readonly string[]>(input: {
  zipPath: string;
  entry: InternalZipEntry;
  columns: Columns;
  predicate?: (row: Record<Columns[number], string>) => boolean;
  maxRows?: number;
}): Promise<Record<Columns[number], string>[]> {
  if (input.entry.encrypted) {
    throw new Error(`Encrypted Nebraska NADC ZIP entries are not supported: ${input.entry.fileName}`);
  }
  if (input.entry.compressionMethodCode !== 0 && input.entry.compressionMethodCode !== 8) {
    throw new Error(
      `Unsupported Nebraska NADC ZIP compression method ${input.entry.compressionMethodCode} for ${input.entry.fileName}`
    );
  }
  if (input.entry.compressedSize === 0) {
    return [];
  }

  const dataOffset = await readEntryDataOffset(input.zipPath, input.entry);
  const source = createReadStream(input.zipPath, {
    start: dataOffset,
    end: dataOffset + input.entry.compressedSize - 1,
  });
  const stream = input.entry.compressionMethodCode === 8 ? source.pipe(createInflateRaw()) : source;
  const decoder = new StringDecoder("latin1");

  return await new Promise((resolve, reject) => {
    const rows: Record<Columns[number], string>[] = [];
    let row: string[] = [];
    let field = "";
    let inQuotes = false;
    let pendingQuoteInQuotedField = false;
    let headerIndexes: Record<Columns[number], number> | null = null;
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
      if (!headerIndexes) {
        const headerIndex = buildHeaderIndex(cells);
        headerIndexes = Object.fromEntries(
          input.columns.map((column) => [column, requireColumn(headerIndex, column)])
        ) as Record<Columns[number], number>;
        return;
      }
      if (input.maxRows !== undefined && rows.length >= input.maxRows) {
        return;
      }

      const parsedRow = rowObjectFromCells(cells, input.columns, headerIndexes);
      if (!input.predicate || input.predicate(parsedRow)) {
        rows.push(parsedRow);
        if (input.maxRows !== undefined && rows.length >= input.maxRows) {
          source.destroy();
          if (stream !== source) {
            stream.destroy();
          }
          resolveOnce();
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

      if (isFinal && pendingQuoteInQuotedField) {
        pendingQuoteInQuotedField = false;
        inQuotes = false;
      }
    };

    stream.on("data", (chunk: Buffer) => {
      if (settled) {
        return;
      }
      try {
        processText(decoder.write(chunk));
      } catch (error) {
        rejectOnce(error instanceof Error ? error : new Error(String(error)));
        stream.destroy();
      }
    });

    stream.on("end", () => {
      if (settled) {
        return;
      }
      try {
        processText(decoder.end(), true);
        if (inQuotes) {
          throw new Error("Nebraska NADC CSV has an unterminated quoted field");
        }
        if (field.length > 0 || row.length > 0) {
          finishCurrentRow();
        }
        resolveOnce();
      } catch (error) {
        rejectOnce(error instanceof Error ? error : new Error(String(error)));
      }
    });

    stream.on("error", (error: Error) => {
      if (!settled) {
        rejectOnce(error);
      }
    });
  });
}

async function readNebraskaNadcRows<const Columns extends readonly string[]>(input: {
  zipPath: string;
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
  columns: Columns;
  predicate?: (row: Record<Columns[number], string>) => boolean;
  maxRows?: number;
}): Promise<Record<Columns[number], string>[]> {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const fileName = nebraskaNadcCsvFileName(artifact);
  const entries = await readZipEntries(input.zipPath);
  const entry = entries.find((candidate) => candidate.fileName === fileName);
  if (!entry || entry.isDirectory) {
    throw new Error(`Nebraska NADC CSV not found in ZIP: ${fileName}`);
  }

  return await streamNebraskaNadcRows({
    zipPath: input.zipPath,
    entry,
    columns: input.columns,
    predicate: input.predicate,
    maxRows,
  });
}

export async function readNebraskaNadcContributionRows(input: {
  zipPath: string;
  year: number;
  predicate?: NebraskaNadcContributionRowPredicate;
  maxRows?: number;
}): Promise<NebraskaNadcContributionRow[]> {
  return (await readNebraskaNadcRows({
    zipPath: input.zipPath,
    year: input.year,
    artifactKind: "contribution_loan",
    columns: NEBRASKA_NADC_CONTRIBUTION_COLUMNS,
    predicate: input.predicate as ((row: Record<(typeof NEBRASKA_NADC_CONTRIBUTION_COLUMNS)[number], string>) => boolean) | undefined,
    maxRows: input.maxRows,
  })) as NebraskaNadcContributionRow[];
}

export async function readNebraskaNadcExpenditureRows(input: {
  zipPath: string;
  year: number;
  predicate?: NebraskaNadcExpenditureRowPredicate;
  maxRows?: number;
}): Promise<NebraskaNadcExpenditureRow[]> {
  return (await readNebraskaNadcRows({
    zipPath: input.zipPath,
    year: input.year,
    artifactKind: "expenditure",
    columns: NEBRASKA_NADC_EXPENDITURE_COLUMNS,
    predicate: input.predicate as ((row: Record<(typeof NEBRASKA_NADC_EXPENDITURE_COLUMNS)[number], string>) => boolean) | undefined,
    maxRows: input.maxRows,
  })) as NebraskaNadcExpenditureRow[];
}

import { createReadStream } from "node:fs";

// Chunk-feedable CSV parser for the Ohio SoS bulk exports served by the
// CFDISCLOSURE file-transfer page. Quirks pinned by the 2026-08-04 acquisition
// spike (ohio_plan.md decision 10): bytes are Windows-1252 (not valid UTF-8),
// rows are separated by bare CR (no LF anywhere in the files), currency
// amounts arrive quoted ("$150,000.00"), headers drift between underscores and
// spaces (CANDIDATE_FIRST_NAME vs CANDIDATE FIRST NAME), the active-candidate
// list repeats the OFFICE header, and values carry HTML entities (&AMP;).
// Every published file ends with a row separator, so a missing final
// separator is treated as truncation, matching the Illinois SBE reader.

export type OhioSosCsvRowVisitor = (row: string[]) => void;

export type OhioSosCsvMalformedRowReporter = (input: {
  line: number;
  columnCount: number;
  row: readonly string[];
}) => void;

export type OhioSosCsvRowSeparator = "\r" | "\n" | "\r\n";

// Headers are compared after normalization so whitespace/underscore drift
// ("CANDIDATE FIRST NAME", "Payee  Non Individual") cannot break a pinned
// schema, while real column changes still fail loudly.
export function normalizeOhioSosHeader(value: string): string {
  return value
    .replace(/^﻿/, "")
    .toUpperCase()
    .replace(/[_\s]+/g, " ")
    .trim();
}

export class OhioSosCsvParser {
  private readonly label: string;
  private readonly expectedHeader: readonly string[];
  private readonly normalizedExpectedHeader: readonly string[];
  private readonly visit: OhioSosCsvRowVisitor;
  private readonly onMalformedRow: OhioSosCsvMalformedRowReporter | undefined;
  private row: string[] = [];
  private field = "";
  private quoted = false;
  // A quote seen at the end of a chunk while quoted: whether it closes the
  // field or escapes a quote depends on the next character.
  private pendingQuote = false;
  // A CR at a chunk boundary may be half of a CRLF pair.
  private pendingCr = false;
  private line = 1;
  private headerSeen = false;
  private atStart = true;
  private endedWithSeparator = false;
  private detectedRowSeparator: OhioSosCsvRowSeparator | null = null;

  constructor(input: {
    label: string;
    expectedHeader: readonly string[];
    visit: OhioSosCsvRowVisitor;
    // Without a reporter a wrong column count throws — right for the small
    // files, where it can only mean schema drift. The ~90 MB contribution
    // files pass a reporter so a stray damaged row is skipped and counted
    // instead of failing the whole run; the header check stays strict.
    onMalformedRow?: OhioSosCsvMalformedRowReporter;
  }) {
    this.label = input.label;
    this.expectedHeader = input.expectedHeader;
    this.normalizedExpectedHeader = input.expectedHeader.map(normalizeOhioSosHeader);
    this.visit = input.visit;
    this.onMalformedRow = input.onMalformedRow;
  }

  get rowSeparator(): OhioSosCsvRowSeparator | null {
    return this.detectedRowSeparator;
  }

  push(chunk: string): void {
    let source = chunk;
    if (this.atStart && source.length > 0) {
      source = source.replace(/^﻿/, "");
      this.atStart = false;
    }
    for (let index = 0; index < source.length; index += 1) {
      const character = source[index]!;
      if (this.pendingCr) {
        this.pendingCr = false;
        if (character === "\n") {
          if (this.detectedRowSeparator === "\r") {
            this.detectedRowSeparator = "\r\n";
          }
          continue;
        }
      }
      if (this.quoted) {
        if (this.pendingQuote) {
          this.pendingQuote = false;
          if (character === '"') {
            // The held quote was the first half of an escaped quote.
            this.field += '"';
            continue;
          }
          // The held quote closed the field; reprocess this character.
          this.quoted = false;
          index -= 1;
          continue;
        }
        if (character === '"') {
          if (index + 1 === source.length) {
            this.pendingQuote = true;
          } else if (source[index + 1] === '"') {
            this.field += '"';
            index += 1;
          } else {
            this.quoted = false;
          }
        } else {
          this.field += character;
        }
        continue;
      }
      if (character === '"' && this.field.length === 0) {
        this.quoted = true;
        this.endedWithSeparator = false;
      } else if (character === ",") {
        this.row.push(this.field);
        this.field = "";
        this.endedWithSeparator = false;
      } else if (character === "\r") {
        this.detectedRowSeparator ??= "\r";
        this.finishRow();
        this.pendingCr = true;
        this.endedWithSeparator = true;
      } else if (character === "\n") {
        this.detectedRowSeparator ??= "\n";
        this.finishRow();
        this.endedWithSeparator = true;
      } else {
        this.field += character;
        this.endedWithSeparator = false;
      }
    }
  }

  end(): void {
    if (this.pendingQuote) {
      this.pendingQuote = false;
      this.quoted = false;
      // A quote at the very end closed the field; the missing separator is
      // still reported below.
      this.endedWithSeparator = false;
    }
    if (this.quoted) {
      throw new Error(`Ohio SoS ${this.label} file is incomplete: unterminated quoted field`);
    }
    if (!this.endedWithSeparator) {
      throw new Error(`Ohio SoS ${this.label} file is incomplete: final row separator is missing`);
    }
    if (!this.headerSeen) {
      throw new Error(`Ohio SoS ${this.label} file has no header`);
    }
  }

  private finishRow(): void {
    this.row.push(this.field);
    this.field = "";
    if (!this.headerSeen) {
      const normalized = this.row.map(normalizeOhioSosHeader);
      if (
        normalized.length !== this.normalizedExpectedHeader.length ||
        normalized.some((value, index) => value !== this.normalizedExpectedHeader[index])
      ) {
        throw new Error(
          `Ohio SoS ${this.label} header does not match the pinned schema: got ${JSON.stringify(this.row)}`
        );
      }
      this.headerSeen = true;
    } else if (this.row.some((value) => value.length > 0)) {
      if (this.row.length !== this.expectedHeader.length) {
        if (!this.onMalformedRow) {
          throw new Error(
            `Ohio SoS ${this.label} row ${this.line} has ${this.row.length} columns; expected ${this.expectedHeader.length}`
          );
        }
        this.onMalformedRow({ line: this.line, columnCount: this.row.length, row: this.row });
        this.row = [];
        this.line += 1;
        return;
      }
      this.visit(this.row);
    }
    this.row = [];
    this.line += 1;
  }
}

export type OhioSosCsvParseResult = {
  rowSeparator: OhioSosCsvRowSeparator | null;
  malformedRowCount: number;
};

export function parseOhioSosCsvText(
  text: string,
  input: {
    label: string;
    expectedHeader: readonly string[];
    visit: OhioSosCsvRowVisitor;
    onMalformedRow?: OhioSosCsvMalformedRowReporter;
  }
): OhioSosCsvParseResult {
  let malformedRowCount = 0;
  const parser = new OhioSosCsvParser({
    label: input.label,
    expectedHeader: input.expectedHeader,
    visit: input.visit,
    onMalformedRow: input.onMalformedRow
      ? (malformed) => {
          malformedRowCount += 1;
          input.onMalformedRow!(malformed);
        }
      : undefined,
  });
  parser.push(text);
  parser.end();
  return { rowSeparator: parser.rowSeparator, malformedRowCount };
}

// The bulk files are Windows-1252, which UTF-8 decoding would mangle;
// windows-1252 is single-byte, so streaming decode never splits a character.
export async function parseOhioSosCsvFile(
  path: string,
  input: {
    label: string;
    expectedHeader: readonly string[];
    visit: OhioSosCsvRowVisitor;
    onMalformedRow?: OhioSosCsvMalformedRowReporter;
  }
): Promise<OhioSosCsvParseResult> {
  let malformedRowCount = 0;
  const parser = new OhioSosCsvParser({
    label: input.label,
    expectedHeader: input.expectedHeader,
    visit: input.visit,
    onMalformedRow: input.onMalformedRow
      ? (malformed) => {
          malformedRowCount += 1;
          input.onMalformedRow!(malformed);
        }
      : undefined,
  });
  const decoder = new TextDecoder("windows-1252");
  const stream = createReadStream(path);
  for await (const chunk of stream) {
    parser.push(decoder.decode(chunk as Buffer, { stream: true }));
  }
  parser.push(decoder.decode());
  parser.end();
  return { rowSeparator: parser.rowSeparator, malformedRowCount };
}

const HTML_ENTITY_REPLACEMENTS = new Map<string, string>([
  ["AMP", "&"],
  ["LT", "<"],
  ["GT", ">"],
  ["QUOT", '"'],
  ["APOS", "'"],
  ["NBSP", " "],
]);

function decodeOhioSosHtmlEntities(value: string): string {
  return value.replace(/&(#\d{1,6}|[A-Za-z]{2,6});/g, (match, body: string) => {
    if (body.startsWith("#")) {
      const codePoint = Number(body.slice(1));
      return Number.isInteger(codePoint) && codePoint > 0 && codePoint <= 0x10ffff
        ? String.fromCodePoint(codePoint)
        : match;
    }
    return HTML_ENTITY_REPLACEMENTS.get(body.toUpperCase()) ?? match;
  });
}

// Display cleanup for Ohio SoS values: entity decode (&AMP; appears in real
// rows), NBSP + whitespace collapse, trim.
export function normalizeOhioSosText(value: string | undefined): string {
  return decodeOhioSosHtmlEntities(value ?? "")
    .replace(/\u00a0/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

export function normalizeOhioSosTextOrNull(value: string | undefined): string | null {
  const normalized = normalizeOhioSosText(value);
  return normalized.length > 0 ? normalized : null;
}

// Amounts appear both bare (2500, .4) and currency-formatted ("$31,550.42");
// negatives use a leading minus (negative BALANCE_ON_HAND is real — decision
// 1). Returns integer cents so aggregation never accumulates float error.
export function parseOhioSosAmountCents(raw: string | undefined): number | null {
  const normalized = normalizeOhioSosText(raw).replace(/[$,]/g, "");
  if (!normalized || !/^-?(?:\d+(?:\.\d{1,4})?|\.\d{1,4})$/.test(normalized)) {
    return null;
  }
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

export function ohioSosCentsToDollars(cents: number): number {
  return cents / 100;
}

const OHIO_SOS_MONTHS: Readonly<Record<string, number>> = {
  JAN: 1,
  FEB: 2,
  MAR: 3,
  APR: 4,
  MAY: 5,
  JUN: 6,
  JUL: 7,
  AUG: 8,
  SEP: 9,
  OCT: 10,
  NOV: 11,
  DEC: 12,
};

// Transaction dates are MM/DD/YYYY; cover-page DATE_REPORT_FILED is
// DD-MON-YY (26-APR-90). Ohio bulk data starts in 1990, so two-digit years
// pivot at 70: 70–99 → 19xx, 00–69 → 20xx. Returns YYYY-MM-DD, rejecting
// impossible calendar dates.
export function parseOhioSosDateIso(raw: string | undefined): string | null {
  const trimmed = normalizeOhioSosText(raw);
  if (!trimmed) {
    return null;
  }
  let year: number | null = null;
  let month: number | null = null;
  let day: number | null = null;
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(trimmed);
  if (slashMatch) {
    month = Number(slashMatch[1]);
    day = Number(slashMatch[2]);
    year = Number(slashMatch[3]);
  } else {
    const monMatch = /^(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})$/.exec(trimmed);
    const monthFromName = monMatch ? OHIO_SOS_MONTHS[monMatch[2]!.toUpperCase()] : undefined;
    if (!monMatch || !monthFromName) {
      return null;
    }
    day = Number(monMatch[1]);
    month = monthFromName;
    const yearPart = monMatch[3]!;
    if (yearPart.length === 4) {
      year = Number(yearPart);
    } else {
      const twoDigit = Number(yearPart);
      year = twoDigit >= 70 ? 1900 + twoDigit : 2000 + twoDigit;
    }
  }
  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) {
    return null;
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

export function parseOhioSosDateYear(raw: string | undefined): number | null {
  const iso = parseOhioSosDateIso(raw);
  return iso ? Number(iso.slice(0, 4)) : null;
}

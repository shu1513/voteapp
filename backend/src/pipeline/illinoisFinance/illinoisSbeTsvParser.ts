// Chunk-feedable parser for the Illinois SBE bulk tab-separated exports.
// Semantics shared by every bulk file: an exact published header row, tab
// delimiters, quoted fields ("" escapes a quote) that may span newlines, a
// required final newline, and blank rows that are skipped.

export type IllinoisSbeTsvRowVisitor = (row: string[]) => void;

export type IllinoisSbeTsvMalformedRowReporter = (input: {
  line: number;
  columnCount: number;
  row: readonly string[];
}) => void;

export class IllinoisSbeTsvParser {
  private readonly label: string;
  private readonly expectedHeader: readonly string[];
  private readonly visit: IllinoisSbeTsvRowVisitor;
  private readonly onMalformedRow: IllinoisSbeTsvMalformedRowReporter | undefined;
  private row: string[] = [];
  private field = "";
  private quoted = false;
  // A quote seen at the end of a chunk while quoted: whether it closes the
  // field or escapes a quote depends on the next character, which may arrive
  // in the next chunk.
  private pendingQuote = false;
  private line = 1;
  private headerSeen = false;
  private atStart = true;
  private lastCharacter: string | null = null;

  constructor(input: {
    label: string;
    expectedHeader: readonly string[];
    visit: IllinoisSbeTsvRowVisitor;
    // Without a reporter a wrong column count throws, which is what the small
    // bulk files want: there, it can only mean schema drift. The multi-million
    // row receipts export carries a handful of rows with unescaped tabs or
    // newlines, so its reader passes a reporter to skip and count them instead
    // of failing the whole sync. The header check stays strict either way, so
    // real schema drift is still caught.
    onMalformedRow?: IllinoisSbeTsvMalformedRowReporter;
  }) {
    this.label = input.label;
    this.expectedHeader = input.expectedHeader;
    this.visit = input.visit;
    this.onMalformedRow = input.onMalformedRow;
  }

  push(chunk: string): void {
    let source = chunk;
    if (this.atStart && source.length > 0) {
      source = source.replace(/^﻿/, "");
      this.atStart = false;
    }
    for (let index = 0; index < source.length; index += 1) {
      const character = source[index]!;
      this.lastCharacter = character;
      if (this.quoted) {
        if (this.pendingQuote) {
          this.pendingQuote = false;
          if (character === '"') {
            // The held quote was the first half of an escaped quote.
            this.field += '"';
            continue;
          }
          // The held quote closed the field; reprocess this character unquoted.
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
      } else if (character === '"' && this.field.length === 0) {
        this.quoted = true;
      } else if (character === "\t") {
        this.row.push(this.field);
        this.field = "";
      } else if (character === "\n") {
        if (this.field.endsWith("\r")) {
          this.field = this.field.slice(0, -1);
        }
        this.finishRow();
      } else {
        this.field += character;
      }
    }
  }

  end(): void {
    if (this.pendingQuote) {
      // A quote at the very end of the input closes the field; the missing
      // final newline is still reported below, matching whole-string parsing.
      this.pendingQuote = false;
      this.quoted = false;
    }
    if (this.lastCharacter !== "\n") {
      throw new Error(`Illinois SBE ${this.label} file is incomplete: final newline is missing`);
    }
    if (this.quoted) {
      throw new Error(`Illinois SBE ${this.label} file is incomplete: unterminated quoted field`);
    }
    if (!this.headerSeen) {
      throw new Error(`Illinois SBE ${this.label} file has no header`);
    }
  }

  private finishRow(): void {
    this.row.push(this.field);
    this.field = "";
    if (!this.headerSeen) {
      if (
        this.row.length !== this.expectedHeader.length ||
        this.row.some((value, index) => value !== this.expectedHeader[index])
      ) {
        throw new Error(`Illinois SBE ${this.label} header does not match the published schema`);
      }
      this.headerSeen = true;
    } else if (this.row.some((value) => value.length > 0)) {
      if (this.row.length !== this.expectedHeader.length) {
        if (!this.onMalformedRow) {
          throw new Error(
            `Illinois SBE ${this.label} row ${this.line} has ${this.row.length} columns; expected ${this.expectedHeader.length}`
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

export function parseIllinoisSbeTsv(
  text: string,
  label: string,
  expectedHeader: readonly string[],
  visit: IllinoisSbeTsvRowVisitor
): void {
  // With the whole string in hand the truncation check can run first, exactly
  // as the pre-streaming parser did — so a file that is both truncated and
  // damaged mid-stream reports the truncation, not the first damaged row.
  if (!text.replace(/^﻿/, "").endsWith("\n")) {
    throw new Error(`Illinois SBE ${label} file is incomplete: final newline is missing`);
  }
  const parser = new IllinoisSbeTsvParser({ label, expectedHeader, visit });
  parser.push(text);
  parser.end();
}

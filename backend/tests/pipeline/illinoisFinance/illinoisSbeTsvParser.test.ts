import { describe, expect, it } from "vitest";

import {
  IllinoisSbeTsvParser,
  parseIllinoisSbeTsv,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeTsvParser.js";

const HEADER = ["ID", "Name", "Notes"] as const;

const SAMPLE =
  'ID\tName\tNotes\n' +
  '1\tPlain\tno quoting\n' +
  '2\t"Quoted, name"\t"He said ""hi"" twice"\n' +
  '3\t"Multi\nline"\twith crlf\r\n' +
  '\t\t\n' +
  '4\tLast\t""\n';

function parseWholeString(text: string): string[][] {
  const rows: string[][] = [];
  parseIllinoisSbeTsv(text, "Sample.txt", HEADER, (row) => rows.push([...row]));
  return rows;
}

function parseInChunks(text: string, splitAt: number): string[][] {
  const rows: string[][] = [];
  const parser = new IllinoisSbeTsvParser({
    label: "Sample.txt",
    expectedHeader: HEADER,
    visit: (row) => rows.push([...row]),
  });
  parser.push(text.slice(0, splitAt));
  parser.push(text.slice(splitAt));
  parser.end();
  return rows;
}

describe("illinoisSbeTsvParser", () => {
  it("parses quoting, escaped quotes, embedded newlines, CRLF, and blank rows", () => {
    expect(parseWholeString(SAMPLE)).toEqual([
      ["1", "Plain", "no quoting"],
      ["2", "Quoted, name", 'He said "hi" twice'],
      ["3", "Multi\nline", "with crlf"],
      ["4", "Last", ""],
    ]);
  });

  it("parses identically at every possible chunk boundary", () => {
    const expected = parseWholeString(SAMPLE);
    for (let splitAt = 0; splitAt <= SAMPLE.length; splitAt += 1) {
      expect(parseInChunks(SAMPLE, splitAt)).toEqual(expected);
    }
  });

  it("parses identically when fed one character at a time", () => {
    const rows: string[][] = [];
    const parser = new IllinoisSbeTsvParser({
      label: "Sample.txt",
      expectedHeader: HEADER,
      visit: (row) => rows.push([...row]),
    });
    for (const character of SAMPLE) {
      parser.push(character);
    }
    parser.end();
    expect(rows).toEqual(parseWholeString(SAMPLE));
  });

  it("strips a byte-order mark before the header", () => {
    expect(parseWholeString(`﻿${SAMPLE}`)).toEqual(parseWholeString(SAMPLE));
  });

  it("rejects a missing final newline", () => {
    expect(() => parseWholeString(SAMPLE.slice(0, -1))).toThrow(
      "Illinois SBE Sample.txt file is incomplete: final newline is missing"
    );
  });

  it("rejects an empty input as missing its final newline", () => {
    expect(() => parseWholeString("")).toThrow(
      "Illinois SBE Sample.txt file is incomplete: final newline is missing"
    );
  });

  it("rejects an unterminated quoted field", () => {
    expect(() => parseWholeString('ID\tName\tNotes\n1\t"Open quote\t\n')).toThrow(
      "Illinois SBE Sample.txt file is incomplete: unterminated quoted field"
    );
  });

  it("rejects a header that does not match the published schema", () => {
    expect(() => parseWholeString("ID\tSurname\tNotes\n")).toThrow(
      "Illinois SBE Sample.txt header does not match the published schema"
    );
  });

  it("rejects a data row with the wrong column count", () => {
    expect(() => parseWholeString("ID\tName\tNotes\n1\tonly-two\n")).toThrow(
      "Illinois SBE Sample.txt row 2 has 2 columns; expected 3"
    );
  });
});

import { describe, expect, it } from "vitest";

import {
  normalizeOhioSosHeader,
  normalizeOhioSosText,
  normalizeOhioSosTextOrNull,
  ohioSosCentsToDollars,
  OhioSosCsvParser,
  parseOhioSosAmountCents,
  parseOhioSosCsvText,
  parseOhioSosDateIso,
  parseOhioSosDateYear,
} from "../../../src/pipeline/ohioFinance/ohioSosCsv.js";

const HEADER = ["A", "B"] as const;

function collect(text: string, options: { expectedHeader?: readonly string[] } = {}): string[][] {
  const rows: string[][] = [];
  parseOhioSosCsvText(text, {
    label: "test file",
    expectedHeader: options.expectedHeader ?? HEADER,
    visit: (row) => rows.push([...row]),
  });
  return rows;
}

describe("ohioSosCsv row separators", () => {
  it("parses CR-only rows, the separator the real Ohio files use", () => {
    const result = parseOhioSosCsvText("A,B\rone,two\rthree,four\r", {
      label: "test file",
      expectedHeader: HEADER,
      visit: () => {},
    });
    expect(result.rowSeparator).toBe("\r");
  });

  it("parses CR-only, LF-only, and CRLF files identically", () => {
    expect(collect("A,B\rone,two\r")).toEqual([["one", "two"]]);
    expect(collect("A,B\none,two\n")).toEqual([["one", "two"]]);
    expect(collect("A,B\r\none,two\r\n")).toEqual([["one", "two"]]);
  });

  it("reports CRLF as its own separator rather than a CR plus a blank row", () => {
    const result = parseOhioSosCsvText("A,B\r\none,two\r\n", {
      label: "test file",
      expectedHeader: HEADER,
      visit: () => {},
    });
    expect(result.rowSeparator).toBe("\r\n");
  });

  it("splits a CRLF pair across chunks without emitting a phantom row", () => {
    const rows: string[][] = [];
    const parser = new OhioSosCsvParser({
      label: "test file",
      expectedHeader: HEADER,
      visit: (row) => rows.push([...row]),
    });
    parser.push("A,B\r");
    parser.push("\none,two\r\n");
    parser.end();
    expect(rows).toEqual([["one", "two"]]);
  });
});

describe("ohioSosCsv quoting", () => {
  it("keeps commas inside quoted currency amounts", () => {
    expect(collect('A,B\r"$150,000.00",x\r')).toEqual([["$150,000.00", "x"]]);
  });

  it("unescapes doubled quotes", () => {
    expect(collect('A,B\r"say ""hi""",x\r')).toEqual([['say "hi"', "x"]]);
  });

  it("keeps a quoted field intact when the closing quote lands on a chunk boundary", () => {
    const rows: string[][] = [];
    const parser = new OhioSosCsvParser({
      label: "test file",
      expectedHeader: HEADER,
      visit: (row) => rows.push([...row]),
    });
    parser.push('A,B\r"$150,000.00"');
    parser.push(",x\r");
    parser.end();
    expect(rows).toEqual([["$150,000.00", "x"]]);
  });

  it("keeps an escaped quote intact across a chunk boundary", () => {
    const rows: string[][] = [];
    const parser = new OhioSosCsvParser({
      label: "test file",
      expectedHeader: HEADER,
      visit: (row) => rows.push([...row]),
    });
    parser.push('A,B\r"say ""');
    parser.push('hi""",x\r');
    parser.end();
    expect(rows).toEqual([['say "hi"', "x"]]);
  });

  it("rejects an unterminated quoted field", () => {
    expect(() => collect('A,B\r"open,x\r')).toThrow(/unterminated quoted field/);
  });
});

describe("ohioSosCsv structural validation", () => {
  it("accepts a header whose spelling drifts between underscores and spaces", () => {
    expect(
      collect("CANDIDATE FIRST NAME,candidate_last_name\rAMY,ACTON\r", {
        expectedHeader: ["CANDIDATE_FIRST_NAME", "CANDIDATE_LAST_NAME"],
      })
    ).toEqual([["AMY", "ACTON"]]);
  });

  it("rejects a header whose columns actually changed", () => {
    expect(() => collect("A,C\rone,two\r")).toThrow(/header does not match the pinned schema/);
  });

  it("treats a missing final row separator as truncation", () => {
    expect(() => collect("A,B\rone,two")).toThrow(/final row separator is missing/);
  });

  it("throws on a wrong column count when no malformed-row reporter is given", () => {
    expect(() => collect("A,B\rone,two,three\r")).toThrow(/has 3 columns; expected 2/);
  });

  it("skips and counts malformed rows when a reporter is given", () => {
    const rows: string[][] = [];
    const malformed: number[] = [];
    const result = parseOhioSosCsvText("A,B\rone,two\rbad,row,extra\rthree,four\r", {
      label: "test file",
      expectedHeader: HEADER,
      visit: (row) => rows.push([...row]),
      onMalformedRow: ({ line }) => malformed.push(line),
    });
    expect(rows).toEqual([
      ["one", "two"],
      ["three", "four"],
    ]);
    expect(result.malformedRowCount).toBe(1);
    expect(malformed).toEqual([3]);
  });

  it("skips blank rows and strips a leading byte-order mark", () => {
    expect(collect("﻿A,B\r\rone,two\r")).toEqual([["one", "two"]]);
  });

  it("rejects a file with no header at all", () => {
    expect(() => collect("")).toThrow(/final row separator is missing/);
  });
});

describe("normalizeOhioSosHeader", () => {
  it("upper-cases and collapses underscores and whitespace", () => {
    expect(normalizeOhioSosHeader("Payee  Non Individual")).toBe("PAYEE NON INDIVIDUAL");
    expect(normalizeOhioSosHeader(" Address")).toBe("ADDRESS");
    expect(normalizeOhioSosHeader("candidate_first_name")).toBe("CANDIDATE FIRST NAME");
  });
});

describe("normalizeOhioSosText", () => {
  it("decodes the HTML entities the portal emits", () => {
    expect(normalizeOhioSosText("SMITH &AMP; JONES")).toBe("SMITH & JONES");
    expect(normalizeOhioSosText("A &#38; B")).toBe("A & B");
  });

  it("leaves an unknown entity alone rather than guessing", () => {
    expect(normalizeOhioSosText("100 &widget; each")).toBe("100 &widget; each");
  });

  it("collapses whitespace, including non-breaking spaces, and trims", () => {
    expect(normalizeOhioSosText(" CITIZENS   FOR KALMBACH ")).toBe("CITIZENS FOR KALMBACH");
  });

  it("maps blank and whitespace-only values to null", () => {
    expect(normalizeOhioSosTextOrNull(" ")).toBeNull();
    expect(normalizeOhioSosTextOrNull(undefined)).toBeNull();
    expect(normalizeOhioSosTextOrNull("x")).toBe("x");
  });
});

describe("parseOhioSosAmountCents", () => {
  it("parses bare, currency-formatted, and leading-dot amounts", () => {
    expect(parseOhioSosAmountCents("2500")).toBe(250_000);
    expect(parseOhioSosAmountCents('"$150,000.00"'.replace(/"/g, ""))).toBe(15_000_000);
    expect(parseOhioSosAmountCents("$31,550.42")).toBe(3_155_042);
    expect(parseOhioSosAmountCents(".4")).toBe(40);
  });

  it("preserves a negative balance rather than clamping it", () => {
    expect(parseOhioSosAmountCents("-31")).toBe(-3100);
  });

  it("returns null for blank and non-numeric values", () => {
    expect(parseOhioSosAmountCents("")).toBeNull();
    expect(parseOhioSosAmountCents(undefined)).toBeNull();
    expect(parseOhioSosAmountCents("N/A")).toBeNull();
    expect(parseOhioSosAmountCents("1.2.3")).toBeNull();
  });

  it("converts cents back to dollars without float drift", () => {
    expect(ohioSosCentsToDollars(parseOhioSosAmountCents("9,800,170.34")!)).toBe(9_800_170.34);
  });
});

describe("parseOhioSosDateIso", () => {
  it("parses transaction dates in MM/DD/YYYY", () => {
    expect(parseOhioSosDateIso("04/28/2026")).toBe("2026-04-28");
    expect(parseOhioSosDateIso("1/5/2026")).toBe("2026-01-05");
  });

  it("parses cover-page dates in DD-MON-YY, pivoting two-digit years at 70", () => {
    expect(parseOhioSosDateIso("26-APR-90")).toBe("1990-04-26");
    expect(parseOhioSosDateIso("15-DEC-00")).toBe("2000-12-15");
  });

  it("rejects blank, malformed, and impossible dates", () => {
    expect(parseOhioSosDateIso("")).toBeNull();
    expect(parseOhioSosDateIso("2026-04-28")).toBeNull();
    expect(parseOhioSosDateIso("02/30/2026")).toBeNull();
    expect(parseOhioSosDateIso("26-XXX-90")).toBeNull();
  });

  it("derives the year from the parsed date", () => {
    expect(parseOhioSosDateYear("04/28/2026")).toBe(2026);
    expect(parseOhioSosDateYear("26-APR-90")).toBe(1990);
    expect(parseOhioSosDateYear("nonsense")).toBeNull();
  });
});

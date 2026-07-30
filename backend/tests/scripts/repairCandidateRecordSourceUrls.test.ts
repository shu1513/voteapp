import { describe, expect, it } from "vitest";

import { parseRepairsFile } from "../../src/scripts/repairCandidateRecordSourceUrls.js";

describe("parseRepairsFile", () => {
  it("accepts a well-formed repair list", () => {
    const parsed = parseRepairsFile(
      JSON.stringify([
        {
          recordId: "  98e5cd52-7f13-463f-b8f6-aaf76d70d4d6  ",
          sourceUrl: " https://www.sos.mn.gov/news/x ",
          note: "decoded from ssc=",
        },
      ])
    );
    expect(parsed).toEqual([
      {
        recordId: "98e5cd52-7f13-463f-b8f6-aaf76d70d4d6",
        sourceUrl: "https://www.sos.mn.gov/news/x",
        note: "decoded from ssc=",
      },
    ]);
  });

  it("rejects a non-array payload", () => {
    expect(() => parseRepairsFile(JSON.stringify({ recordId: "a" }))).toThrow(/JSON array/);
  });

  it("rejects a missing or blank recordId", () => {
    expect(() =>
      parseRepairsFile(JSON.stringify([{ sourceUrl: "https://example.gov/a" }]))
    ).toThrow(/recordId/);
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "   ", sourceUrl: "https://example.gov/a" }]))
    ).toThrow(/recordId/);
  });

  it("rejects a replacement that is not an http(s) URL", () => {
    // A bare hostname or a file path would otherwise reach the UPDATE and
    // store an uncitable value.
    expect(() => parseRepairsFile(JSON.stringify([{ recordId: "a", sourceUrl: "sos.mn.gov" }]))).toThrow(
      /http\(s\) URL/
    );
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "a", sourceUrl: "/tmp/page.html" }]))
    ).toThrow(/http\(s\) URL/);
  });
});

import { describe, expect, it } from "vitest";

import {
  CAL_ACCESS_RAW_DATA_TABLE_MANIFEST,
  listCalAccessRawDataManifestFileNames,
  validateCalAccessRawDataManifest,
} from "../../../src/pipeline/californiaFinance/calAccessRawDataManifest.js";

function sampleForManifestEntry(entry: (typeof CAL_ACCESS_RAW_DATA_TABLE_MANIFEST)[number]) {
  return {
    fileName: entry.fileName,
    delimiter: "tab" as const,
    encoding: "utf8" as const,
    headers: [...entry.requiredColumns],
    rows: [],
    rowObjects: [],
    truncated: false,
  };
}

describe("calAccessRawDataManifest", () => {
  it("lists the raw files required by the California finance importer plan", () => {
    expect(listCalAccessRawDataManifestFileNames()).toEqual([
      "CalAccess/DATA/FILERNAME_CD.TSV",
      "CalAccess/DATA/CVR_CAMPAIGN_DISCLOSURE_CD.TSV",
      "CalAccess/DATA/RCPT_CD.TSV",
      "CalAccess/DATA/S496_CD.TSV",
      "CalAccess/DATA/S497_CD.TSV",
      "CalAccess/DATA/SMRY_CD.TSV",
    ]);
  });

  it("validates a probe result with all manifest files and required columns", () => {
    const entries = CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.map((entry) => ({
      fileName: entry.fileName,
      compressedSize: 100,
      uncompressedSize: 200,
      compressionMethod: "deflated" as const,
      isDirectory: false,
    }));
    const samples = CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.map(sampleForManifestEntry);

    expect(validateCalAccessRawDataManifest({ entries, samples, missingFileNames: [] })).toEqual({
      ok: true,
      missingFiles: [],
      tables: CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.map((entry) => ({
        key: entry.key,
        fileName: entry.fileName,
        present: true,
        missingColumns: [],
      })),
    });
  });

  it("reports missing files and missing required columns", () => {
    const [first, ...rest] = CAL_ACCESS_RAW_DATA_TABLE_MANIFEST;
    const entries = rest.map((entry) => ({
      fileName: entry.fileName,
      compressedSize: 100,
      uncompressedSize: 200,
      compressionMethod: "deflated" as const,
      isDirectory: false,
    }));
    const samples = rest.map((entry, index) =>
      index === 0
        ? {
            ...sampleForManifestEntry(entry),
            headers: entry.requiredColumns.filter((column) => column !== entry.requiredColumns[0]),
          }
        : sampleForManifestEntry(entry)
    );

    const result = validateCalAccessRawDataManifest({
      entries,
      samples,
      missingFileNames: first ? [first.fileName] : [],
    });

    expect(result.ok).toBe(false);
    expect(result.missingFiles).toEqual([first?.fileName]);
    expect(result.tables[0]).toMatchObject({
      key: first?.key,
      present: false,
      missingColumns: first ? [...first.requiredColumns] : [],
    });
    expect(result.tables[1]?.present).toBe(true);
    expect(result.tables[1]?.missingColumns).toEqual([rest[0]?.requiredColumns[0]]);
  });
});

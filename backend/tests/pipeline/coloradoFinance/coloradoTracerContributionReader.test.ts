import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { deflateRawSync } from "node:zlib";

import { afterEach, describe, expect, it } from "vitest";

import {
  COLORADO_TRACER_CONTRIBUTION_COLUMNS,
  coloradoTracerContributionCsvFileName,
  parseColoradoTracerContributionCsv,
  readColoradoTracerContributionRows,
} from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

type ZipFixtureEntry = {
  fileName: string;
  content?: string;
  compressionMethod?: 0 | 8;
};

let tempDirs: string[] = [];

function makeZip(entries: readonly ZipFixtureEntry[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let localOffset = 0;

  for (const entry of entries) {
    const fileName = Buffer.from(entry.fileName, "utf8");
    const content = Buffer.from(entry.content ?? "", "utf8");
    const compressionMethod = entry.compressionMethod ?? 0;
    const compressed = compressionMethod === 8 ? deflateRawSync(content) : content;

    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(0, 6);
    localHeader.writeUInt16LE(compressionMethod, 8);
    localHeader.writeUInt32LE(0, 10);
    localHeader.writeUInt32LE(0, 14);
    localHeader.writeUInt32LE(compressed.length, 18);
    localHeader.writeUInt32LE(content.length, 22);
    localHeader.writeUInt16LE(fileName.length, 26);
    localHeader.writeUInt16LE(0, 28);

    localParts.push(localHeader, fileName, compressed);

    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0, 8);
    centralHeader.writeUInt16LE(compressionMethod, 10);
    centralHeader.writeUInt32LE(0, 12);
    centralHeader.writeUInt32LE(0, 16);
    centralHeader.writeUInt32LE(compressed.length, 20);
    centralHeader.writeUInt32LE(content.length, 24);
    centralHeader.writeUInt16LE(fileName.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(entry.fileName.endsWith("/") ? 0x10 : 0, 38);
    centralHeader.writeUInt32LE(localOffset, 42);
    centralParts.push(centralHeader, fileName);

    localOffset += localHeader.length + fileName.length + compressed.length;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const centralDirectoryOffset = localOffset;
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8);
  eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(centralDirectory.length, 12);
  eocd.writeUInt32LE(centralDirectoryOffset, 16);
  eocd.writeUInt16LE(0, 20);

  return Buffer.concat([...localParts, centralDirectory, eocd]);
}

async function writeFixtureZip(entries: readonly ZipFixtureEntry[]): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "co-tracer-reader-"));
  tempDirs.push(dir);
  const zipPath = path.join(dir, "2024_ContributionData.csv.zip");
  await writeFile(zipPath, makeZip(entries));
  return zipPath;
}

function csvRow(values: Partial<Record<(typeof COLORADO_TRACER_CONTRIBUTION_COLUMNS)[number], string>>): string {
  return COLORADO_TRACER_CONTRIBUTION_COLUMNS.map((column) => {
    const value = values[column] ?? "";
    return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
  }).join(",");
}

function contributionCsv(rows: readonly Partial<Record<(typeof COLORADO_TRACER_CONTRIBUTION_COLUMNS)[number], string>>[]): string {
  return [
    COLORADO_TRACER_CONTRIBUTION_COLUMNS.join(","),
    ...rows.map(csvRow),
  ].join("\n");
}

describe("Colorado TRACER contribution reader", () => {
  afterEach(async () => {
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("builds the official contribution CSV filename", () => {
    expect(coloradoTracerContributionCsvFileName(2024)).toBe("2024_ContributionData.csv");
  });

  it("parses quoted contribution CSV rows", () => {
    expect(
      parseColoradoTracerContributionCsv(
        contributionCsv([
          {
            CO_ID: "202450001",
            ContributionAmount: "125.50",
            ContributionDate: "05/01/2024",
            LastName: "Doe",
            FirstName: "Jane",
            Employer: "Acme, Inc.",
            Occupation: "Engineer",
            CommitteeName: "Jane Doe for Colorado",
            CandidateName: "Jane Doe",
            OccupationComments: "Software, platform team",
          },
        ])
      )
    ).toEqual([
      expect.objectContaining({
        CO_ID: "202450001",
        ContributionAmount: "125.50",
        Employer: "Acme, Inc.",
        OccupationComments: "Software, platform team",
      }),
    ]);
  });

  it("reads deflated contribution rows from the expected yearly CSV entry", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "2024_ContributionData.csv",
        compressionMethod: 8,
        content: contributionCsv([
          {
            CO_ID: "202450001",
            ContributionAmount: "100.00",
            ContributionDate: "01/10/2024",
            CommitteeName: "Candidate Committee",
            CandidateName: "Alex Example",
            Employer: "Acme",
            Occupation: "Attorney",
          },
          {
            CO_ID: "202450002",
            ContributionAmount: "200.00",
            ContributionDate: "02/10/2024",
            CommitteeName: "Other Committee",
            CandidateName: "Other Candidate",
            Employer: "Beta",
            Occupation: "Teacher",
          },
        ]),
      },
    ]);

    await expect(
      readColoradoTracerContributionRows({
        zipPath,
        year: 2024,
        predicate: (row) => row.CandidateName === "Alex Example",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        CO_ID: "202450001",
        CandidateName: "Alex Example",
        Employer: "Acme",
        Occupation: "Attorney",
      }),
    ]);
  });

  it("streams quoted multiline fields without splitting a contribution row", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "2024_ContributionData.csv",
        compressionMethod: 8,
        content: contributionCsv([
          {
            CO_ID: "202450001",
            ContributionAmount: "100.00",
            ContributionDate: "01/10/2024",
            CommitteeName: "Candidate Committee",
            CandidateName: "Alex Example",
            Employer: "Acme",
            Occupation: "Attorney",
            OccupationComments: "line one\nline two",
          },
          {
            CO_ID: "202450002",
            ContributionAmount: "200.00",
            ContributionDate: "02/10/2024",
            CommitteeName: "Other Committee",
            CandidateName: "Other Candidate",
            Employer: "Beta",
            Occupation: "Teacher",
          },
        ]),
      },
    ]);

    await expect(
      readColoradoTracerContributionRows({
        zipPath,
        year: 2024,
        predicate: (row) => row.CandidateName === "Alex Example",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        CO_ID: "202450001",
        CandidateName: "Alex Example",
        OccupationComments: "line one\nline two",
      }),
    ]);
  });

  it("supports maxRows after filtering", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "2024_ContributionData.csv",
        content: contributionCsv([
          { CO_ID: "1", CandidateName: "Alex Example" },
          { CO_ID: "2", CandidateName: "Alex Example" },
          { CO_ID: "3", CandidateName: "Other Candidate" },
        ]),
      },
    ]);

    const rows = await readColoradoTracerContributionRows({
      zipPath,
      year: 2024,
      predicate: (row) => row.CandidateName === "Alex Example",
      maxRows: 1,
    });

    expect(rows).toEqual([expect.objectContaining({ CO_ID: "1" })]);
  });

  it("throws when the expected yearly CSV entry is missing", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "wrong.csv",
        content: contributionCsv([]),
      },
    ]);

    await expect(readColoradoTracerContributionRows({ zipPath, year: 2024 })).rejects.toThrow(
      "Colorado TRACER contribution CSV not found in ZIP: 2024_ContributionData.csv"
    );
  });

  it("throws when a required CSV column is missing", () => {
    expect(() =>
      parseColoradoTracerContributionCsv("CO_ID,ContributionAmount\n1,100.00\n")
    ).toThrow("Missing required Colorado TRACER contribution CSV column: ContributionDate");
  });
});

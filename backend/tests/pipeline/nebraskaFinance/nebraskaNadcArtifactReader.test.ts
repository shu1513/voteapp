import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { deflateRawSync } from "node:zlib";

import { afterEach, describe, expect, it } from "vitest";

import {
  NEBRASKA_NADC_CONTRIBUTION_COLUMNS,
  NEBRASKA_NADC_EXPENDITURE_COLUMNS,
  nebraskaNadcCsvFileName,
  parseNebraskaNadcContributionCsv,
  parseNebraskaNadcExpenditureCsv,
  readNebraskaNadcContributionRows,
  readNebraskaNadcExpenditureRows,
} from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactReader.js";

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
    const content = Buffer.from(entry.content ?? "", "latin1");
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

async function writeFixtureZip(fileName: string, entries: readonly ZipFixtureEntry[]): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "ne-nadc-reader-"));
  tempDirs.push(dir);
  const zipPath = path.join(dir, fileName);
  await writeFile(zipPath, makeZip(entries));
  return zipPath;
}

function csvValue(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function contributionRow(values: Partial<Record<(typeof NEBRASKA_NADC_CONTRIBUTION_COLUMNS)[number], string>>): string {
  return NEBRASKA_NADC_CONTRIBUTION_COLUMNS.map((column) => csvValue(values[column] ?? "")).join(",");
}

function expenditureRow(values: Partial<Record<(typeof NEBRASKA_NADC_EXPENDITURE_COLUMNS)[number], string>>): string {
  return NEBRASKA_NADC_EXPENDITURE_COLUMNS.map((column) => csvValue(values[column] ?? "")).join(",");
}

function contributionCsv(
  rows: readonly Partial<Record<(typeof NEBRASKA_NADC_CONTRIBUTION_COLUMNS)[number], string>>[]
): string {
  return [NEBRASKA_NADC_CONTRIBUTION_COLUMNS.join(","), ...rows.map(contributionRow)].join("\n");
}

function expenditureCsv(
  rows: readonly Partial<Record<(typeof NEBRASKA_NADC_EXPENDITURE_COLUMNS)[number], string>>[]
): string {
  return [NEBRASKA_NADC_EXPENDITURE_COLUMNS.join(","), ...rows.map(expenditureRow)].join("\n");
}

describe("Nebraska NADC artifact reader", () => {
  afterEach(async () => {
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("builds official contribution and expenditure CSV filenames", () => {
    expect(nebraskaNadcCsvFileName({ year: 2026, artifactKind: "contribution_loan" })).toBe(
      "2026_ContributionLoanExtract.csv"
    );
    expect(nebraskaNadcCsvFileName({ year: 2026, artifactKind: "expenditure" })).toBe(
      "2026_ExpenditureExtract.csv"
    );
  });

  it("parses quoted contribution rows", () => {
    expect(
      parseNebraskaNadcContributionCsv(
        contributionCsv([
          {
            "Receipt ID": "123",
            "Org ID": "1001",
            "Filer Type": "Candidate Committee",
            "Filer Name": "LINEHAN FOR LEGISLATURE",
            "Candidate Name": "LOU ANN LINEHAN",
            "Receipt Amount": "1000.00",
            "Contributor or Source Name (Individual Last Name)": "REYNOLDS AMERICAN INC. / RAI SERVICES",
            Employer: "Acme, Inc.",
            Occupation: "Engineer",
          },
        ])
      )
    ).toEqual([
      expect.objectContaining({
        "Receipt ID": "123",
        "Candidate Name": "LOU ANN LINEHAN",
        Employer: "Acme, Inc.",
      }),
    ]);
  });

  it("parses quoted expenditure support rows", () => {
    expect(
      parseNebraskaNadcExpenditureCsv(
        expenditureCsv([
          {
            "Expenditure ID": "900",
            "Org ID": "2001",
            "Filer Type": "PAC-Independent",
            "Filer Name": "PRESERVE THE GOOD LIFE",
            "Expenditure Transaction Type": "Independent Expenditure",
            "Expenditure Amount": "6990.50",
            "Support Or Oppose": "SUPPORT",
            "Candidate Name or Ballot Issue": "JOHN FREDRICKSON",
            "Jurisdiction - Office - District or Ballot Description": "NEBRASKA - STATE LEGISLATURE - 20",
            Description: "Digital, mail, and ads",
          },
        ])
      )
    ).toEqual([
      expect.objectContaining({
        "Expenditure ID": "900",
        "Support Or Oppose": "SUPPORT",
        "Jurisdiction - Office - District or Ballot Description": "NEBRASKA - STATE LEGISLATURE - 20",
        Description: "Digital, mail, and ads",
      }),
    ]);
  });

  it("reads deflated contribution rows from the expected yearly CSV entry", async () => {
    const zipPath = await writeFixtureZip("2026_ContributionLoanExtract.csv.zip", [
      {
        fileName: "2026_ContributionLoanExtract.csv",
        compressionMethod: 8,
        content: contributionCsv([
          {
            "Receipt ID": "1",
            "Org ID": "1001",
            "Filer Name": "VOTE VEST",
            "Candidate Name": "RICK VEST",
            "Receipt Amount": "250.00",
            Employer: "Acme",
            Occupation: "Attorney",
          },
          {
            "Receipt ID": "2",
            "Org ID": "1002",
            "Filer Name": "OTHER COMMITTEE",
            "Candidate Name": "OTHER CANDIDATE",
            "Receipt Amount": "500.00",
            Employer: "Beta",
            Occupation: "Teacher",
          },
        ]),
      },
    ]);

    await expect(
      readNebraskaNadcContributionRows({
        zipPath,
        year: 2026,
        predicate: (row) => row["Candidate Name"] === "RICK VEST",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        "Receipt ID": "1",
        "Candidate Name": "RICK VEST",
        Employer: "Acme",
        Occupation: "Attorney",
      }),
    ]);
  });

  it("reads deflated expenditure rows from the expected yearly CSV entry", async () => {
    const zipPath = await writeFixtureZip("2026_ExpenditureExtract.csv.zip", [
      {
        fileName: "2026_ExpenditureExtract.csv",
        compressionMethod: 8,
        content: expenditureCsv([
          {
            "Expenditure ID": "10",
            "Org ID": "2001",
            "Filer Type": "PAC-Independent",
            "Filer Name": "PRESERVE THE GOOD LIFE",
            "Expenditure Amount": "6990.50",
            "Support Or Oppose": "SUPPORT",
            "Candidate Name or Ballot Issue": "JOHN FREDRICKSON",
          },
          {
            "Expenditure ID": "11",
            "Org ID": "2002",
            "Filer Name": "OTHER PAC",
            "Support Or Oppose": "OPPOSE",
            "Candidate Name or Ballot Issue": "OTHER CANDIDATE",
          },
        ]),
      },
    ]);

    await expect(
      readNebraskaNadcExpenditureRows({
        zipPath,
        year: 2026,
        predicate: (row) => row["Candidate Name or Ballot Issue"] === "JOHN FREDRICKSON",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        "Expenditure ID": "10",
        "Support Or Oppose": "SUPPORT",
        "Candidate Name or Ballot Issue": "JOHN FREDRICKSON",
      }),
    ]);
  });

  it("streams quoted multiline fields without splitting rows", async () => {
    const zipPath = await writeFixtureZip("2026_ExpenditureExtract.csv.zip", [
      {
        fileName: "2026_ExpenditureExtract.csv",
        compressionMethod: 8,
        content: expenditureCsv([
          {
            "Expenditure ID": "10",
            "Candidate Name or Ballot Issue": "JOHN FREDRICKSON",
            Description: "line one\nline two",
          },
          {
            "Expenditure ID": "11",
            "Candidate Name or Ballot Issue": "OTHER CANDIDATE",
            Description: "plain",
          },
        ]),
      },
    ]);

    await expect(
      readNebraskaNadcExpenditureRows({
        zipPath,
        year: 2026,
        predicate: (row) => row["Candidate Name or Ballot Issue"] === "JOHN FREDRICKSON",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        "Expenditure ID": "10",
        Description: "line one\nline two",
      }),
    ]);
  });

  it("supports maxRows after filtering", async () => {
    const zipPath = await writeFixtureZip("2026_ContributionLoanExtract.csv.zip", [
      {
        fileName: "2026_ContributionLoanExtract.csv",
        content: contributionCsv([
          { "Receipt ID": "1", "Candidate Name": "RICK VEST" },
          { "Receipt ID": "2", "Candidate Name": "RICK VEST" },
          { "Receipt ID": "3", "Candidate Name": "OTHER CANDIDATE" },
        ]),
      },
    ]);

    const rows = await readNebraskaNadcContributionRows({
      zipPath,
      year: 2026,
      predicate: (row) => row["Candidate Name"] === "RICK VEST",
      maxRows: 1,
    });

    expect(rows).toEqual([expect.objectContaining({ "Receipt ID": "1" })]);
  });

  it("throws when the expected yearly CSV entry is missing", async () => {
    const zipPath = await writeFixtureZip("2026_ContributionLoanExtract.csv.zip", [
      {
        fileName: "wrong.csv",
        content: contributionCsv([]),
      },
    ]);

    await expect(readNebraskaNadcContributionRows({ zipPath, year: 2026 })).rejects.toThrow(
      "Nebraska NADC CSV not found in ZIP: 2026_ContributionLoanExtract.csv"
    );
  });

  it("throws when a required CSV column is missing", () => {
    expect(() => parseNebraskaNadcContributionCsv('"Receipt ID","Org ID"\n"1","100"\n')).toThrow(
      "Missing required Nebraska NADC CSV column: Filer Type"
    );
  });
});

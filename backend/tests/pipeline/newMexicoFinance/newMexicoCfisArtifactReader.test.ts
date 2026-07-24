import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS,
  NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS,
  normalizeNewMexicoCfisHeader,
  parseNewMexicoCfisContributionCsvRows,
  parseNewMexicoCfisExpenditureCsvRows,
  readNewMexicoCfisContributionRows,
  readNewMexicoCfisExpenditureRows,
} from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-nm-cfis-reader-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

function csvCell(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function csvRow<TColumn extends string>(columns: readonly TColumn[], values: Partial<Record<TColumn, string>>): string {
  return columns.map((column) => csvCell(values[column] ?? "")).join(",");
}

function contributionCsv(rows: readonly Partial<Record<(typeof NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS)[number], string>>[]): string {
  return [
    NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS.join(","),
    ...rows.map((row) => csvRow(NEW_MEXICO_CFIS_CONTRIBUTION_COLUMNS, row)),
  ].join("\n");
}

function expenditureCsv(rows: readonly Partial<Record<(typeof NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS)[number], string>>[]): string {
  return [
    NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS.join(","),
    ...rows.map((row) => csvRow(NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS, row)),
  ].join("\n");
}

describe("New Mexico CFIS artifact reader", () => {
  it("normalizes headers", () => {
    expect(normalizeNewMexicoCfisHeader("\uFEFF Contributor Occupation ")).toBe("Contributor Occupation");
  });

  it("parses contribution rows with quoted commas and exact CFIS columns", () => {
    const rows = parseNewMexicoCfisContributionCsvRows(
      contributionCsv([
        {
          OrgID: "1001",
          "Transaction Amount": "125.50",
          "Transaction Date": "01/02/2026",
          "Last Name": "Doe",
          "First Name": "Jane",
          "Contributor Code": "Individual",
          "Contribution Type": "Contributions - Monetary",
          "Report Entity Type": "Candidate",
          "Committee Name": "Doe, Jane for Governor",
          "Candidate Last Name": "Doe",
          "Candidate First Name": "Jane",
          "Contributor Employer": "Acme, Inc.",
          "Contributor Occupation": "Attorney",
        },
      ])
    );

    expect(rows).toEqual([
      expect.objectContaining({
        OrgID: "1001",
        "Transaction Amount": "125.50",
        "Committee Name": "Doe, Jane for Governor",
        "Contributor Employer": "Acme, Inc.",
        "Contributor Occupation": "Attorney",
      }),
    ]);
  });

  it("parses expenditure rows with support/oppose target fields", () => {
    const rows = parseNewMexicoCfisExpenditureCsvRows(
      expenditureCsv([
        {
          OrgID: "9001",
          "Expenditure Amount": "70000.00",
          "Expenditure Date": "04/01/2026",
          Purpose: "Independent expenditure supporting/opposing others (explain)*",
          "Expenditure Type": "Independent Expenditure",
          Reason: "Haaland, Deb",
          Stance: "Oppose",
          "Report Entity Type": "PAC - Independent Expenditure",
          "Committee Name": "Accountable New Mexico",
        },
      ])
    );

    expect(rows).toEqual([
      expect.objectContaining({
        OrgID: "9001",
        "Expenditure Amount": "70000.00",
        Purpose: "Independent expenditure supporting/opposing others (explain)*",
        Reason: "Haaland, Deb",
        Stance: "Oppose",
        "Report Entity Type": "PAC - Independent Expenditure",
        "Committee Name": "Accountable New Mexico",
      }),
    ]);
  });

  it("preserves unescaped interior quotes emitted by the official export", () => {
    const csv = expenditureCsv([
      {
        OrgID: "9001",
        "Expenditure Amount": "100.00",
        Description: 'Printing for "Neighbors" mailer',
      },
    ]).replace('Printing for ""Neighbors"" mailer', 'Printing for "Neighbors" mailer');

    expect(parseNewMexicoCfisExpenditureCsvRows(csv)).toEqual([
      expect.objectContaining({
        OrgID: "9001",
        Description: 'Printing for "Neighbors" mailer',
      }),
    ]);
  });

  it("streams contribution rows from disk with predicate and maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "CON_2026.csv");
    await writeFile(
      filePath,
      contributionCsv([
        {
          OrgID: "1001",
          "Transaction Amount": "100.00",
          "Report Entity Type": "Candidate",
          "Committee Name": "Jane Doe for Governor",
          "Candidate Last Name": "Doe",
          "Candidate First Name": "Jane",
          "Contributor Occupation": "Attorney",
        },
        {
          OrgID: "1002",
          "Transaction Amount": "200.00",
          "Report Entity Type": "Candidate",
          "Committee Name": "Other Candidate",
          "Candidate Last Name": "Other",
          "Candidate First Name": "Candidate",
          "Contributor Occupation": "Teacher",
        },
        {
          OrgID: "1001",
          "Transaction Amount": "300.00",
          "Report Entity Type": "Candidate",
          "Committee Name": "Jane Doe for Governor",
          "Candidate Last Name": "Doe",
          "Candidate First Name": "Jane",
          "Contributor Occupation": "Attorney",
        },
      ]),
      "utf8"
    );

    await expect(
      readNewMexicoCfisContributionRows({
        filePath,
        predicate: (row) => row.OrgID === "1001",
        maxRows: 1,
      })
    ).resolves.toEqual([
      expect.objectContaining({
        OrgID: "1001",
        "Transaction Amount": "100.00",
        "Contributor Occupation": "Attorney",
      }),
    ]);
  });

  it("streams quoted multiline expenditure fields without splitting rows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "EXP_2026.csv");
    await writeFile(
      filePath,
      expenditureCsv([
        {
          OrgID: "9001",
          "Expenditure Amount": "70000.00",
          Description: "line one\nline two",
          Reason: "Haaland, Deb",
          Stance: "Support",
          "Report Entity Type": "PAC - Independent Expenditure",
          "Committee Name": "Accountable New Mexico",
        },
        {
          OrgID: "9002",
          "Expenditure Amount": "400.00",
          Reason: "Someone Else",
          Stance: "Support",
          "Report Entity Type": "Candidate",
          "Committee Name": "Candidate Committee",
        },
      ]),
      "utf8"
    );

    await expect(
      readNewMexicoCfisExpenditureRows({
        filePath,
        predicate: (row) => row["Report Entity Type"] === "PAC - Independent Expenditure",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        OrgID: "9001",
        Description: "line one\nline two",
        Reason: "Haaland, Deb",
        Stance: "Support",
      }),
    ]);
  });

  it("streams an unescaped interior quote split across chunks", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "EXP_2026.csv");
    const descriptionSuffix = '"Neighbors" mailer';
    const unpaddedCsv = expenditureCsv([
      {
        OrgID: "9001",
        "Expenditure Amount": "100.00",
        Description: descriptionSuffix,
      },
    ]).replace('""Neighbors"" mailer', descriptionSuffix);
    const firstInteriorQuoteIndex = unpaddedCsv.indexOf(descriptionSuffix);
    const streamChunkSize = 64 * 1024;
    const descriptionPrefix = "x".repeat(streamChunkSize - 1 - firstInteriorQuoteIndex);
    const csv = expenditureCsv([
      {
        OrgID: "9001",
        "Expenditure Amount": "100.00",
        Description: `${descriptionPrefix}${descriptionSuffix}`,
      },
    ]).replace(`${descriptionPrefix}""Neighbors"" mailer`, `${descriptionPrefix}${descriptionSuffix}`);
    expect(csv.indexOf(descriptionSuffix)).toBe(streamChunkSize - 1);
    await writeFile(filePath, csv, "utf8");

    await expect(readNewMexicoCfisExpenditureRows({ filePath })).resolves.toEqual([
      expect.objectContaining({
        OrgID: "9001",
        Description: `${descriptionPrefix}${descriptionSuffix}`,
      }),
    ]);
  });

  it("rejects duplicate headers", () => {
    expect(() => parseNewMexicoCfisContributionCsvRows("OrgID,OrgID\n1,2\n")).toThrow(
      "Duplicate New Mexico CFIS CSV header"
    );
  });

  it("rejects missing required headers", () => {
    const columns = NEW_MEXICO_CFIS_EXPENDITURE_COLUMNS.filter((column) => column !== "Stance");
    expect(() => parseNewMexicoCfisExpenditureCsvRows(`${columns.join(",")}\n${columns.map(() => "").join(",")}\n`)).toThrow(
      "Missing required New Mexico CFIS expenditure CSV column: Stance"
    );
  });

  it("rejects malformed quoted CSV", () => {
    expect(() => parseNewMexicoCfisContributionCsvRows('OrgID,Transaction Amount\n1,"unterminated\n')).toThrow(
      "unterminated quoted field"
    );
  });

  it("rejects invalid maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "CON_2026.csv");
    await writeFile(filePath, contributionCsv([]), "utf8");

    await expect(readNewMexicoCfisContributionRows({ filePath, maxRows: 0 })).rejects.toThrow(
      "Invalid New Mexico CFIS maxRows"
    );
  });
});

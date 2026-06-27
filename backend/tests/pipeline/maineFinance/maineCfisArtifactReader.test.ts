import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  MAINE_CFIS_CONTRIBUTION_COLUMNS,
  MAINE_CFIS_EXPENDITURE_COLUMNS,
  normalizeMaineCfisHeader,
  parseMaineCfisContributionCsvRows,
  parseMaineCfisExpenditureCsvRows,
  parseMaineCfisMoney,
  readMaineCfisContributionRows,
  readMaineCfisExpenditureRows,
} from "../../../src/pipeline/maineFinance/maineCfisArtifactReader.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-me-cfis-reader-"));
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

function contributionCsv(
  rows: readonly Partial<Record<(typeof MAINE_CFIS_CONTRIBUTION_COLUMNS)[number], string>>[]
): string {
  return [
    MAINE_CFIS_CONTRIBUTION_COLUMNS.join(","),
    ...rows.map((row) => csvRow(MAINE_CFIS_CONTRIBUTION_COLUMNS, row)),
  ].join("\n");
}

function expenditureHeader(): string {
  return MAINE_CFIS_EXPENDITURE_COLUMNS.map((column) =>
    column === "Candidate Jurisdiction" ? "Jurisdiction" : column
  ).join(",");
}

function expenditureCsv(
  rows: readonly Partial<Record<(typeof MAINE_CFIS_EXPENDITURE_COLUMNS)[number], string>>[]
): string {
  return [expenditureHeader(), ...rows.map((row) => csvRow(MAINE_CFIS_EXPENDITURE_COLUMNS, row))].join("\n");
}

describe("Maine CFIS artifact reader", () => {
  it("normalizes headers and money values", () => {
    expect(normalizeMaineCfisHeader("\uFEFFOrgID ")).toBe("OrgID");
    expect(parseMaineCfisMoney("1,234.5600")).toBe(1234.56);
    expect(parseMaineCfisMoney("$42.50")).toBe(42.5);
    expect(parseMaineCfisMoney("($10.00)")).toBe(-10);
    expect(parseMaineCfisMoney("not-money")).toBeNull();
  });

  it("parses contribution rows with employer and occupation fields", () => {
    const rows = parseMaineCfisContributionCsvRows(
      contributionCsv([
        {
          OrgID: "249",
          LegacyID: "618",
          "Committee Name": "CITIZENS FOR JUSTICE IN MAINE, INC.",
          "Receipt Amount": "40.0000",
          "Receipt Date": "03/11/2024",
          "Receipt Source Type": "Individual",
          "Receipt Type": "Monetary (Itemized)",
          "Committee Type": "Political Action Committee",
          Employer: "LARGAY LAW OFFICES, P.A.",
          Occupation: "Attorney/Legal",
          ElectionType: "General",
        },
      ])
    );

    expect(rows).toEqual([
      expect.objectContaining({
        OrgID: "249",
        LegacyID: "618",
        Employer: "LARGAY LAW OFFICES, P.A.",
        Occupation: "Attorney/Legal",
        "Receipt Amount": "40.0000",
      }),
    ]);
  });

  it("parses expenditure rows with the duplicated Jurisdiction header mapped to Candidate Jurisdiction", () => {
    const rows = parseMaineCfisExpenditureCsvRows(
      expenditureCsv([
        {
          "Election Year": "2024",
          OrgID: "242",
          LegacyID: "611",
          "Committee Type": "Political Action Committee",
          "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
          Jurisdiction: "LOCAL",
          "Expenditure ID": "1092270",
          "Expenditure Date": "10/03/2024",
          "Expenditure Amount": "1582.5000",
          "IE Report": "Y",
          "Support/Oppose Candidate": "Oppose",
          Candidate: "Reagan LeeAnn Paul",
          "Candidate ID": "481737",
          "Candidate Jurisdiction": "STATE",
          "Candidate Office": "Representative",
          "Candidate District": "37",
        },
      ])
    );

    expect(rows).toEqual([
      expect.objectContaining({
        OrgID: "242",
        Jurisdiction: "LOCAL",
        "IE Report": "Y",
        "Support/Oppose Candidate": "Oppose",
        Candidate: "Reagan LeeAnn Paul",
        "Candidate Jurisdiction": "STATE",
        "Candidate Office": "Representative",
      }),
    ]);
  });

  it("streams contribution rows from disk with predicate and maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "CON_2024.csv");
    await writeFile(
      filePath,
      contributionCsv([
        { OrgID: "1001", "Committee Name": "Jane Doe for Governor", "Receipt Amount": "100.0000" },
        { OrgID: "1002", "Committee Name": "Other Committee", "Receipt Amount": "200.0000" },
        { OrgID: "1001", "Committee Name": "Jane Doe for Governor", "Receipt Amount": "300.0000" },
      ]),
      "utf8"
    );

    await expect(
      readMaineCfisContributionRows({
        filePath,
        predicate: (row) => row.OrgID === "1001",
        maxRows: 1,
      })
    ).resolves.toEqual([
      expect.objectContaining({
        OrgID: "1001",
        "Receipt Amount": "100.0000",
      }),
    ]);
  });

  it("streams quoted multiline expenditure fields without splitting rows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "EXP_2024.csv");
    await writeFile(
      filePath,
      expenditureCsv([
        {
          OrgID: "9001",
          "Committee Name": "Maine Example PAC",
          Explanation: "line one\nline two",
          Candidate: "Doe, Jane",
          "IE Report": "Y",
        },
      ]),
      "utf8"
    );

    await expect(readMaineCfisExpenditureRows({ filePath })).resolves.toEqual([
      expect.objectContaining({
        Explanation: "line one\nline two",
        Candidate: "Doe, Jane",
      }),
    ]);
  });

  it("rejects duplicate non-Maine headers, missing headers, malformed quotes, and invalid maxRows", async () => {
    expect(() => parseMaineCfisContributionCsvRows("OrgID,OrgID\n1,2\n")).toThrow("Duplicate Maine CFIS CSV header");

    const contributionColumns = MAINE_CFIS_CONTRIBUTION_COLUMNS.filter((column) => column !== "Occupation");
    expect(() =>
      parseMaineCfisContributionCsvRows(`${contributionColumns.join(",")}\n${contributionColumns.map(() => "").join(",")}\n`)
    ).toThrow("Missing required Maine CFIS contribution CSV column: Occupation");

    expect(() => parseMaineCfisExpenditureCsvRows('OrgID,Committee Name\n1,"unterminated\n')).toThrow(
      "unterminated quoted field"
    );

    const dir = await makeTempDir();
    const filePath = join(dir, "CON_2024.csv");
    await writeFile(filePath, contributionCsv([{ OrgID: "1001" }]), "utf8");
    await expect(readMaineCfisContributionRows({ filePath, maxRows: 0 })).rejects.toThrow(
      "Invalid Maine CFIS maxRows"
    );
  });
});

import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS,
  LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME,
  LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS,
  LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURES_CSV_FILE_NAME,
  normalizeLouisianaCampaignFinanceHeader,
  parseLouisianaCampaignFinanceContributionCsvRows,
  parseLouisianaCampaignFinanceExpenditureCsvRows,
  readLouisianaCampaignFinanceContributionRows,
  readLouisianaCampaignFinanceExpenditureRows,
} from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-la-cf-reader-"));
  tempDirs.push(dir);
  return dir;
}

function csvCell(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function csvRow(columns: readonly string[], values: Record<string, string>): string {
  return columns.map((column) => csvCell(values[column] ?? "")).join(",");
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Louisiana campaign finance artifact reader", () => {
  it("normalizes headers", () => {
    expect(normalizeLouisianaCampaignFinanceHeader("\uFEFFFilerNumber ")).toBe("FilerNumber");
  });

  it("parses contribution rows with the official headers", () => {
    const csv = [
      LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS.join(","),
      csvRow(LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS, {
        FilerNumber: "12345",
        FilerLastName: "Example",
        FilerFirstName: "Candidate",
        ReportCode: "10-G",
        ReportType: "10-G",
        ReportNumber: "1",
        ContributorTypeCode: "IND",
        ContributorName: "Doe, Jane",
        ContributorAddr1: "100 Main St",
        ContributorAddr2: "Suite 2",
        ContributorCity: "Baton Rouge",
        ContributorrState: "LA",
        ContributorZip: "70801",
        ContributionType: "MONETARY",
        ContributionDescription: "line one\nline two",
        ContributionDate: "01/15/2026",
        ContributionAmt: "125.50",
        ContributionDesignatedElectionAdditionInfo: "Primary",
      }),
    ].join("\n");

    expect(parseLouisianaCampaignFinanceContributionCsvRows(csv)).toEqual([
      expect.objectContaining({
        FilerNumber: "12345",
        ContributorName: "Doe, Jane",
        ContributorrState: "LA",
        ContributionDescription: "line one\nline two",
        ContributionAmt: "125.50",
      }),
    ]);
  });

  it("parses expenditure rows with the official headers", () => {
    const csv = [
      LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS.join(","),
      csvRow(LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS, {
        FilerNumber: "54321",
        FilerLastName: "Better Louisiana PAC",
        FilerFirstName: "",
        ReportCode: "40-G",
        ReportType: "40-G",
        ReportNumber: "2",
        Schedule: "E-1",
        RecipientName: "Mail House LLC",
        RecipientAddr1: "200 Print Ave",
        RecipientCity: "New Orleans",
        RecipientState: "LA",
        RecipientZip: "70112",
        ExpenditureDescription: "Support ad, mailer",
        CandidateBeneficiary: "Candidate Example",
        ExpenditureDate: "10/01/2026",
        ExpenditureAmt: "70000.00",
      }),
    ].join("\n");

    expect(parseLouisianaCampaignFinanceExpenditureCsvRows(csv)).toEqual([
      expect.objectContaining({
        FilerNumber: "54321",
        RecipientName: "Mail House LLC",
        CandidateBeneficiary: "Candidate Example",
        ExpenditureDescription: "Support ad, mailer",
        ExpenditureAmt: "70000.00",
      }),
    ]);
  });

  it("streams and filters contribution rows from disk", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME);
    await writeFile(
      filePath,
      [
        LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS.join(","),
        csvRow(LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS, {
          FilerNumber: "12345",
          ContributorName: "Acme, Inc.",
          ContributorTypeCode: "BUS",
          ContributionAmt: "500.00",
        }),
        csvRow(LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS, {
          FilerNumber: "99999",
          ContributorName: "Other Donor",
          ContributorTypeCode: "IND",
          ContributionAmt: "100.00",
        }),
      ].join("\n"),
      "utf8"
    );

    await expect(
      readLouisianaCampaignFinanceContributionRows({
        filePath,
        predicate: (row) => row.FilerNumber === "12345",
        maxRows: 1,
      })
    ).resolves.toEqual([
      expect.objectContaining({
        FilerNumber: "12345",
        ContributorName: "Acme, Inc.",
        ContributionAmt: "500.00",
      }),
    ]);
  });

  it("streams and filters expenditure rows from disk", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURES_CSV_FILE_NAME);
    await writeFile(
      filePath,
      [
        LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS.join(","),
        csvRow(LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS, {
          FilerNumber: "54321",
          RecipientName: "Mail House LLC",
          CandidateBeneficiary: "Candidate Example",
          ExpenditureAmt: "70000.00",
        }),
        csvRow(LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURE_HEADERS, {
          FilerNumber: "88888",
          RecipientName: "Consultant",
          CandidateBeneficiary: "Other Candidate",
          ExpenditureAmt: "2500.00",
        }),
      ].join("\n"),
      "utf8"
    );

    await expect(
      readLouisianaCampaignFinanceExpenditureRows({
        filePath,
        predicate: (row) => row.CandidateBeneficiary === "Candidate Example",
      })
    ).resolves.toEqual([
      expect.objectContaining({
        FilerNumber: "54321",
        RecipientName: "Mail House LLC",
        ExpenditureAmt: "70000.00",
      }),
    ]);
  });

  it("rejects missing official contribution headers", () => {
    const headers = LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS.filter((header) => header !== "ContributorrState");
    expect(() => parseLouisianaCampaignFinanceContributionCsvRows(`${headers.join(",")}\n1\n`)).toThrow(
      "Louisiana campaign finance contribution CSV missing required headers: ContributorrState"
    );
  });

  it("rejects duplicate headers and malformed quoted csv", () => {
    expect(() => parseLouisianaCampaignFinanceContributionCsvRows("FilerNumber,FilerNumber\n1,2\n")).toThrow(
      "Duplicate Louisiana campaign finance CSV header"
    );
    expect(() => parseLouisianaCampaignFinanceExpenditureCsvRows('FilerNumber,RecipientName\n"unterminated\n')).toThrow(
      "unterminated quoted field"
    );
  });

  it("rejects invalid maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME);
    await writeFile(filePath, `${LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTION_HEADERS.join(",")}\n`, "utf8");

    await expect(readLouisianaCampaignFinanceContributionRows({ filePath, maxRows: 0 })).rejects.toThrow(
      "Invalid Louisiana campaign finance maxRows"
    );
  });
});

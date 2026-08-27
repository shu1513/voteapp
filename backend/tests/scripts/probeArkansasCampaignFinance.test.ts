import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  ARKANSAS_EXPENDITURE_CSV_COLUMNS,
  ARKANSAS_RECEIPT_CSV_COLUMNS,
} from "../../src/pipeline/arkansasFinance/arkansasCfisCsv.js";
import {
  parseProbeArkansasCampaignFinanceArgs,
  runProbeArkansasCampaignFinance,
  type ArkansasPhaseZeroClient,
} from "../../src/scripts/probeArkansasCampaignFinance.js";

const GUID = "689c554c-5120-46a4-828e-6798f3298f22";

describe("parseProbeArkansasCampaignFinanceArgs", () => {
  it("applies defaults", () => {
    const args = parseProbeArkansasCampaignFinanceArgs([]);
    expect(args.filingYears).toEqual([2022, 2023, 2024, 2025, 2026]);
    expect(args.goldEntityIds).toEqual([1004, 11847]);
    expect(args.reuseArtifacts).toBe(false);
    expect(args.dnsFallback).toBe(false);
    expect(args.pageSize).toBe(1_000);
  });

  it("parses overrides and boolean flags", () => {
    const args = parseProbeArkansasCampaignFinanceArgs([
      "--filing-years",
      "2025,2026",
      "--gold-entity-ids=1004",
      "--reuse-artifacts",
      "--dns-fallback",
      "--page-size",
      "500",
    ]);
    expect(args.filingYears).toEqual([2025, 2026]);
    expect(args.goldEntityIds).toEqual([1004]);
    expect(args.reuseArtifacts).toBe(true);
    expect(args.dnsFallback).toBe(true);
    expect(args.pageSize).toBe(500);
  });

  it("rejects unknown arguments and duplicates", () => {
    expect(() => parseProbeArkansasCampaignFinanceArgs(["--nope"])).toThrow(/Unknown argument/);
    expect(() => parseProbeArkansasCampaignFinanceArgs(["--filing-years", "2026,2026"])).toThrow(/duplicates/);
  });
});

function receiptCsv(rows: string[]): string {
  return [ARKANSAS_RECEIPT_CSV_COLUMNS.join(","), ...rows].join("\n") + "\n";
}

function expenditureCsv(rows: string[]): string {
  return [ARKANSAS_EXPENDITURE_CSV_COLUMNS.join(","), ...rows].join("\n") + "\n";
}

describe("runProbeArkansasCampaignFinance", () => {
  it("runs the gates against a stub client and seeded artifacts", async () => {
    const artifactDir = await mkdtemp(join(tmpdir(), "ar-phase0-"));
    await writeFile(
      join(artifactDir, "TCON_2026.csv"),
      receiptCsv([
        '1004,"Sanders, Sarah",Candidate,Contribution,Itemized Monetary,Individual,"Walton, Thomas","Bentonville, AR",Runway Group LLC,Financial / Investment,,07/31/2026,"$300.00",,1,General,2026,,,08/20/2026,2026 July Monthly Report,N',
        '1004,"Sanders, Sarah",Candidate,Contribution,Non-Itemized Monetary,Individual,,,,,,07/31/2026,"$50.00",,2,General,2026,,,08/20/2026,2026 July Monthly Report,N',
      ])
    );
    await writeFile(
      join(artifactDir, "TEXP_2026.csv"),
      expenditureCsv([
        '1004,"Sanders, Sarah",Candidate,Expenditure,Itemized Monetary,Business/Organization/Unlisted PAC,Vendor,"Little Rock, AR",03/01/2026,"$100.00",,3,Other(list),,General,2026,04/23/2026,2026 Q1 Quarterly Report,N',
      ])
    );

    const registrationRow = {
      registrationGuid: GUID,
      filerEntityId: 1004,
      filerEntityVersionId: 1,
      filerType: "Candidate",
      filerTypeCode: "CAN",
      filerStatus: "Active",
      firstName: "Sarah",
      lastName: "Sanders",
      suffix: null,
      committeeName: null,
      office: "Governor",
      officeDistrictName: null,
      jurisdictionName: "Arkansas",
      politicalParty: "Republican Party",
      electionYear: 2026,
      filingYear: 2026,
      isPaperFiler: false,
      totalRaised: 350,
      totalSpent: 100,
      balanceOfFunds: 250,
    };
    const transactionRow = {
      guid: "2d22d67f-6a58-414a-8e7f-e9c2a1b6210b",
      filerName: "Sanders, Sarah",
      filerRegistrationGuid: GUID,
      transactionAmount: 350,
      transactionDate: "07/31/2026",
      sourceName: "Walton, Thomas",
      employerName: null,
      occupation: null,
      transactionSource: "Individual",
      reportName: "2026 July Monthly Report",
      transactionSubTypeDescription: "Itemized Monetary",
      transactionCategory: null,
      hasChild: false,
    };

    const client: ArkansasPhaseZeroClient = {
      getNextElectionYear: vi.fn(async () => 2028),
      getOfficeLookup: vi.fn(async () => [
        { value: "1", name: "Governor" },
        { value: "2", name: "Lieutenant Governor" },
        { value: "3", name: "Attorney General" },
        { value: "4", name: "Secretary Of State" },
        { value: "5", name: "State Treasurer" },
        { value: "6", name: "Auditor Of State" },
        { value: "7", name: "State Land Commissioner" },
        { value: "8", name: "State Senate" },
        { value: "9", name: "State Representative" },
        { value: "10", name: "Supreme Court" },
      ]),
      getAllFilerRegistrations: vi.fn(async () => [registrationRow]),
      getAllTransactions: vi.fn(async (input: { transactionTypeCode: string }) =>
        input.transactionTypeCode === "TCON"
          ? [transactionRow]
          : [
              {
                ...transactionRow,
                guid: "3d22d67f-6a58-414a-8e7f-e9c2a1b6210b",
                transactionAmount: 100,
              },
            ]
      ) as ArkansasPhaseZeroClient["getAllTransactions"],
      getAllFiledReports: vi.fn(async () => [
        {
          reportName: "2026 July Monthly Report",
          reportType: "Scheduled Financial Report",
          reportStatus: "Original",
          reportVersion: "Original",
          filerReportVersionId: 1,
          filerReportGuid: "e616bad7-97ee-4552-8f12-69914079be34",
          filerRegistrationGuid: GUID,
          filerEntityId: 1004,
          filerName: "Sanders, Sarah",
          filerType: "Candidate",
          officeName: "Governor",
          jurisdictionName: "Arkansas",
          startDate: null,
          endDate: null,
          dueDate: null,
          filedDate: "08/20/2026",
          isPaperFile: false,
        },
      ]),
      downloadBulkCsvToFile: vi.fn(async () => {
        throw new Error("download should not run with --reuse-artifacts and seeded artifacts");
      }),
    };

    const output = await runProbeArkansasCampaignFinance({
      args: {
        filingYears: [2026],
        goldEntityIds: [1004],
        artifactDir,
        reuseArtifacts: true,
        dnsFallback: false,
        pageSize: 1_000,
        timeoutMs: 10_000,
      },
      client,
      now: new Date("2026-08-26T00:00:00Z"),
    });

    expect(output.gate1_access).toEqual({
      defaultResolverOk: true,
      dnsDefectObserved: false,
      usedDnsFallback: false,
      nextElectionYear: 2028,
    });
    expect(output.gate2_totals.raisedExactFormulaIntersection).toContain("monetary");
    expect(output.gate2_totals.spentExactFormulaIntersection).toContain("expenditure");
    expect(output.gold).toHaveLength(1);
    const gold = output.gold[0]!;
    expect(gold.status).toBe("ok");
    if (gold.status === "ok") {
      expect(gold.completeness.csvReceipts).toEqual({ rowCount: 2, amountCents: 35_000 });
      expect(gold.completeness.apiReceipts).toMatchObject({ rowCount: 1, amountCents: 35_000 });
    }
    expect(output.gate6_occupation.individualRowCount).toBe(2);
    expect(output.gate7_offices.missingRequiredOffices).toEqual([]);
    expect(output.publication).toBe("disabled_phase_zero");
    expect(vi.mocked(client.downloadBulkCsvToFile)).not.toHaveBeenCalled();
  });
});

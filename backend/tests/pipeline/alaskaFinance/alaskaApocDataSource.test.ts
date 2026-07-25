import { mkdtemp, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { describe, expect, it, vi } from "vitest";

import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
} from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";
import { loadAlaskaApocFinanceData } from "../../../src/pipeline/alaskaFinance/alaskaApocDataSource.js";
import { createApocExportChainFetch } from "./alaskaApocClient.test.js";

describe("alaskaApocDataSource", () => {
  it("loads APOC CSV exports from explicit file paths with provenance", async () => {
    const dir = await mkdtemp(join(tmpdir(), "ak-apoc-"));
    const incomePath = join(dir, "income.csv");
    const expendituresPath = join(dir, "ie-exp.csv");
    await writeFile(
      incomePath,
      [
        "Filer,Filer Type,Name,Date,Type,Contributor/Vendor,Amount,Status",
        "Jane Doe,Candidate,Jane Doe,10/01/2026,Income,Pat Smith,$100.00,Complete",
      ].join("\n")
    );
    await writeFile(
      expendituresPath,
      [
        "Filer Name,Filer,Filer Type,Report Year,Type,Date,Position,Candidate/Proposition,Amount,Status",
        "Alaska Future PAC,8001,Group,2026,Expenditure,09/15/2026,Support,Jane Doe,$25,Complete",
      ].join("\n")
    );

    const loaded = await loadAlaskaApocFinanceData({
      mode: "csv",
      incomeCsvPath: incomePath,
      independentExpendituresCsvPath: expendituresPath,
    });

    expect(loaded.metadata).toMatchObject({
      mode: "csv",
      income_source_url: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      independent_expenditure_source_url: ALASKA_APOC_IE_EXPENDITURES_URL,
      income_csv_path: incomePath,
      independent_expenditures_csv_path: expendituresPath,
    });
    expect(loaded.apocData.incomeRows).toHaveLength(1);
    expect(loaded.apocData.independentExpenditureRows).toHaveLength(1);
    expect(loaded.apocData.independentContributionRows).toHaveLength(0);
  });

  it("loads live APOC CSV exports through the client fetch path", async () => {
    const fetchFn = createApocExportChainFetch({
      [ALASKA_APOC_CAMPAIGN_INCOME_URL]: [
        "Filer,Filer Type,Name,Date,Type,Contributor/Vendor,Amount,Status",
        "Jane Doe,Candidate,Jane Doe,10/01/2026,Income,Pat Smith,$100.00,Complete",
      ].join("\n"),
      [ALASKA_APOC_IE_EXPENDITURES_URL]: "Filer Name,Filer,Amount\n",
    });

    const loaded = await loadAlaskaApocFinanceData(
      {
        mode: "live",
        includeIndependentContributions: false,
        retryDelayMs: 0,
        requestSpacingMs: 0,
      },
      { fetchFn }
    );

    expect(loaded.metadata).toMatchObject({
      mode: "live",
      income_source_url: ALASKA_APOC_CAMPAIGN_INCOME_URL,
      retry_delay_ms: 0,
      request_spacing_ms: 0,
    });
    expect(loaded.apocData.incomeRows).toHaveLength(1);
    // Two report pages, each fetched through its four-step export chain.
    expect(fetchFn).toHaveBeenCalledTimes(8);
  });
});

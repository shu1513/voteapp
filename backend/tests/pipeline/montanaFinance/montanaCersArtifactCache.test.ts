import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  readMontanaCersArtifact,
  readMontanaCersCandidateFinanceArtifacts,
  storeMontanaCersArtifact,
} from "../../../src/pipeline/montanaFinance/montanaCersArtifactCache.js";

const fixtures = new URL("../../fixtures/montanaFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, fixtures), "utf8");
}

describe("montanaCersArtifactCache", () => {
  it("stores validated PII-bearing artifacts owner-only and verifies hashes on read", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const body = await fixture("report-inventory-sanitized.json");
    const key = { type: "report_inventory" as const, candidateId: 21020, year: 2026 };
    const manifest = await storeMontanaCersArtifact({
      cacheDir,
      key,
      sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
      body,
      retrievedAt: new Date("2026-08-27T00:00:00Z"),
    });
    expect(manifest.rowCount).toBe(5);
    const filePath = join(cacheDir, "21020/2026/report_inventory.json");
    expect((await stat(filePath)).mode & 0o777).toBe(0o600);
    expect((await stat(join(cacheDir, "21020/2026"))).mode & 0o777).toBe(0o700);
    expect((await readMontanaCersArtifact({ cacheDir, key })).body).toBe(body);
    await writeFile(filePath, `${body} `, "utf8");
    await expect(readMontanaCersArtifact({ cacheDir, key })).rejects.toThrow(
      "Stale or invalid Montana CERS artifact"
    );
  });

  it("validates report-detail artifacts against their key's report id", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const body = await fixture("report-detail-sanitized.json");
    await expect(
      storeMontanaCersArtifact({
        cacheDir,
        key: { type: "report_detail", candidateId: 21020, year: 2026, reportId: 99999 },
        sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
        body,
      })
    ).rejects.toThrow("expected 99999");
    const manifest = await storeMontanaCersArtifact({
      cacheDir,
      key: { type: "report_detail", candidateId: 21020, year: 2026, reportId: 76535 },
      sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
      body,
    });
    expect(manifest.rowCount).toBeGreaterThan(0);
  });

  it("validates before replacing a good artifact", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const body = await fixture("contributions-export-sanitized.csv");
    const key = { type: "contributions_export" as const, candidateId: 21020, year: 2026 };
    await storeMontanaCersArtifact({ cacheDir, key, sourceUrl: "https://example.test", body });
    await expect(
      storeMontanaCersArtifact({ cacheDir, key, sourceUrl: "https://example.test", body: "<html>error</html>" })
    ).rejects.toThrow();
    expect((await readMontanaCersArtifact({ cacheDir, key })).body).toBe(body);
  });

  it("rejects a mixed-vintage candidate bundle after a partial refresh", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const shared = { cacheDir, sourceUrl: "https://example.test", retrievedAt: new Date("2026-08-27T00:00:00Z") };
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "report_inventory", candidateId: 21020, year: 2026 },
      body: await fixture("report-inventory-sanitized.json"),
    });
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "contributions_export", candidateId: 21020, year: 2026 },
      body: await fixture("contributions-export-sanitized.csv"),
    });
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "expenditures_export", candidateId: 21020, year: 2026 },
      body: await fixture("expenditures-export-sanitized.csv"),
      retrievedAt: new Date("2026-08-28T00:00:00Z"),
    });
    await expect(
      readMontanaCersCandidateFinanceArtifacts({ cacheDir, candidateId: 21020, year: 2026 })
    ).rejects.toThrow("Mixed-vintage");
  });

  it("reads a complete same-vintage bundle", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const shared = { cacheDir, sourceUrl: "https://example.test", retrievedAt: new Date("2026-08-27T00:00:00Z") };
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "report_inventory", candidateId: 21020, year: 2026 },
      body: await fixture("report-inventory-sanitized.json"),
    });
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "contributions_export", candidateId: 21020, year: 2026 },
      body: await fixture("contributions-export-sanitized.csv"),
    });
    await storeMontanaCersArtifact({
      ...shared,
      key: { type: "expenditures_export", candidateId: 21020, year: 2026 },
      body: await fixture("expenditures-export-sanitized.csv"),
    });
    const bundle = await readMontanaCersCandidateFinanceArtifacts({ cacheDir, candidateId: 21020, year: 2026 });
    expect(bundle.inventory).toHaveLength(5);
    expect(bundle.contributionRows).toHaveLength(5);
    expect(bundle.expenditureRows).toHaveLength(2);
  });

  it("rejects another candidate's data stored under the key (stale-session defense)", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mt-cers-cache-"));
    const inventory = await fixture("report-inventory-sanitized.json");
    const contributions = await fixture("contributions-export-sanitized.csv");
    // Bedey's (21020) data must not store under candidate 99999.
    await expect(
      storeMontanaCersArtifact({
        cacheDir,
        key: { type: "report_inventory", candidateId: 99999, year: 2026 },
        sourceUrl: "https://example.test",
        body: inventory,
      })
    ).rejects.toThrow("stale-session cross-entity");
    await expect(
      storeMontanaCersArtifact({
        cacheDir,
        key: { type: "contributions_export", candidateId: 99999, year: 2026 },
        sourceUrl: "https://example.test",
        body: contributions,
      })
    ).rejects.toThrow("stale-session cross-entity");
  });

  it("rejects invalid keys", async () => {
    await expect(
      storeMontanaCersArtifact({
        key: { type: "report_inventory", candidateId: 0, year: 2026 },
        sourceUrl: "https://example.test",
        body: "[]",
      })
    ).rejects.toThrow("candidateId");
    await expect(
      storeMontanaCersArtifact({
        key: { type: "report_inventory", candidateId: 21020, year: 2019 },
        sourceUrl: "https://example.test",
        body: "[]",
      })
    ).rejects.toThrow("year");
  });
});

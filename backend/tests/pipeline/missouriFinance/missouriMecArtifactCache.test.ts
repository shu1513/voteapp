import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { readMissouriMecArtifact, readMissouriMecCandidateFinanceArtifacts, storeMissouriMecArtifact } from "../../../src/pipeline/missouriFinance/missouriMecArtifactCache.js";
import { MISSOURI_MEC_CONTRIBUTION_EXPORT_HEADER, MISSOURI_MEC_EXPENDITURE_EXPORT_HEADER } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

const committeeHtml = `<span id="x_lblMECID">C263985</span><span id="x_lblCommName">Jane for Missouri</span><span id="x_lblCandName">Jane Doe</span>
  <span id="x_gvElecHistory_lblElecYear_0">11/3/2026</span><span id="x_gvElecHistory_lblElectionType_0">General Election</span>
  <span id="x_gvElecHistory_lblSub_0">State Representative</span><span id="x_gvElecHistory_lblPolSub_0">Missouri House</span>`;

describe("missouriMecArtifactCache", () => {
  it("stores validated PII-bearing artifacts owner-only and verifies hashes on read", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-mec-cache-"));
    const key = { type: "committee_info" as const, mecid: "C263985", year: 2026 };
    await storeMissouriMecArtifact({ cacheDir, key, sourceUrl: "https://example.test", body: committeeHtml, retrievedAt: new Date("2026-08-19T00:00:00Z") });
    const filePath = join(cacheDir, "C263985/2026/committee_info.html");
    expect((await stat(filePath)).mode & 0o777).toBe(0o600);
    expect((await stat(join(cacheDir, "C263985/2026"))).mode & 0o777).toBe(0o700);
    expect((await readMissouriMecArtifact({ cacheDir, key })).body).toBe(committeeHtml);
    await writeFile(filePath, `${await readFile(filePath, "utf8")} `, "utf8");
    await expect(readMissouriMecArtifact({ cacheDir, key })).rejects.toThrow("Stale or invalid Missouri MEC artifact");
  });

  it("validates before replacing a good artifact", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-mec-cache-"));
    const key = { type: "committee_info" as const, mecid: "C263985", year: 2026 };
    await storeMissouriMecArtifact({ cacheDir, key, sourceUrl: "https://example.test", body: committeeHtml });
    await expect(storeMissouriMecArtifact({ cacheDir, key, sourceUrl: "https://example.test", body: "bad" })).rejects.toThrow();
    expect((await readMissouriMecArtifact({ cacheDir, key })).body).toBe(committeeHtml);
  });

  it("rejects a mixed-vintage candidate bundle after a partial refresh", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "mo-mec-cache-"));
    const mecid = "C263985";
    const year = 2026;
    const first = new Date("2026-08-19T00:00:00Z");
    const inventory = `<a id="x_grvReports_0_hlink_0" data-CPID="1"></a><span id="x_grvReports_0_lblReport_0">July Quarterly Report</span><span id="x_grvReports_0_lblDateReceived_0">7/15/2026</span>`;
    const table = (header: readonly string[]) => `<table><tr>${header.map((value) => `<th>${value}</th>`).join("")}</tr></table>`;
    for (const artifact of [
      { type: "committee_info" as const, body: committeeHtml },
      { type: "report_inventory" as const, body: inventory },
      { type: "contributions" as const, body: table(MISSOURI_MEC_CONTRIBUTION_EXPORT_HEADER) },
      { type: "expenditures" as const, body: table(MISSOURI_MEC_EXPENDITURE_EXPORT_HEADER) },
    ]) {
      await storeMissouriMecArtifact({ cacheDir, key: { type: artifact.type, mecid, year }, sourceUrl: "https://example.test", body: artifact.body, retrievedAt: first });
    }
    await expect(readMissouriMecCandidateFinanceArtifacts({ cacheDir, mecid, year })).resolves.toMatchObject({ contributionRows: [] });
    await storeMissouriMecArtifact({
      cacheDir, key: { type: "expenditures", mecid, year }, sourceUrl: "https://example.test",
      body: table(MISSOURI_MEC_EXPENDITURE_EXPORT_HEADER), retrievedAt: new Date("2026-08-20T00:00:00Z"),
    });
    await expect(readMissouriMecCandidateFinanceArtifacts({ cacheDir, mecid, year })).rejects.toThrow("Mixed-vintage");
  });
});

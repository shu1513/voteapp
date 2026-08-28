import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  readDelawareCfrsCommitteeArtifacts,
  storeDelawareCfrsCommitteeArtifacts,
} from "../../../src/pipeline/delawareFinance/delawareCfrsArtifactCache.js";

const CF_ID = "01009999";

const RECEIPTS_CSV = [
  "Contribution Date,Contributor Name,Contributor Address Line 1,Contributor Address Line 2,Contributor City,Contributor State,Contributor Zip,Contributor Type,Employer Name,Employer Occupation,Contribution Type,Contribution Amount,CF_ID,Receiving Committee,Filing Period,Office,Fixed Asset,",
  `6/30/2026,Jane Donor,1 Example St,,Wilmington,DE,19801,Individual,,,Check,500.0000,${CF_ID},Example for Delaware,2026 2026  General Election 11/03/2026 30 Day,(Attorney General),No`,
].join("\r\n");

const EXPENSES_CSV = [
  "Expenditure Date,Payee Name,Payee Address Line 1,Payee Address Line 2,Payee City,Payee State,Payee Zip,Payee Type,Amount($),CF ID,Committee Name,Expense Category,Expense Purpose,Expense Method,Filing Period,Fixed Asset,",
  `7/1/2026,Vendor,2 Example Rd,,Dover,DE,19901,Individual,300.0000,${CF_ID},Example for Delaware,Media,Ads,Check,2026 30 Day General,No,`,
].join("\r\n");

const FILED_REPORTS_HTML = `<script>jQuery('#Grid').tGrid({columns:[], pageSize:15, total:1});</script>
<div class="t-widget t-grid" id="Grid"><table><thead><tr><th>Filing Period</th></tr></thead><tbody>
<tr><td>2026 30 Day 2026 General Election 11/03/2026</td><td> <a onclick=downloadReport(&#39;report1.pdf&#39;,&#39;600001&#39;,&#39;900&#39;) > Original Financial Statement </a> </td><td>${CF_ID}</td><td>Example for Delaware</td><td>Candidate Committee</td><td>10/06/2026</td><td>2026</td><td>(Attorney General)</td><td>Active</td></tr>
</tbody></table></div>`;

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "de-cfrs-cache-"));
});
afterEach(async () => {
  await rm(cacheDir, { recursive: true, force: true });
});

function storeInput() {
  return {
    cacheDir,
    cfId: CF_ID,
    memberId: 600001,
    sourceUrl: "https://cfrs.elections.delaware.gov/",
    receiptsCsv: RECEIPTS_CSV,
    receiptsSearchTotal: 1,
    expensesCsv: EXPENSES_CSV,
    expensesSearchTotal: 1,
    filedReportsHtml: FILED_REPORTS_HTML,
    reportPdfs: [{ publicReportFileName: "report1.pdf", filingCalendarId: 900, body: Buffer.from("%PDF-fake") }],
    retrievedAt: new Date("2026-08-28T00:00:00.000Z"),
  };
}

describe("delawareCfrsArtifactCache", () => {
  it("stores a bundle with restricted permissions and reads it back verified", async () => {
    await storeDelawareCfrsCommitteeArtifacts(storeInput());
    const bundle = await readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID });
    expect(bundle.manifest.memberId).toBe(600001);
    expect(bundle.receiptRows).toHaveLength(1);
    expect(bundle.expenseRows).toHaveLength(1);
    expect(bundle.filedReportRows[0]?.document?.filingCalendarId).toBe(900);
    expect(bundle.reportPdfs[0]?.body.toString()).toBe("%PDF-fake");

    const mode = (await stat(join(cacheDir, CF_ID, "receipts.csv"))).mode & 0o777;
    expect(mode).toBe(0o600);
    const dirMode = (await stat(join(cacheDir, CF_ID))).mode & 0o777;
    expect(dirMode).toBe(0o700);
  });

  it("rejects tampered artifacts and unparseable bodies", async () => {
    const manifest = await storeDelawareCfrsCommitteeArtifacts(storeInput());
    await writeFile(join(cacheDir, CF_ID, manifest.files.receiptsCsv.path), `${RECEIPTS_CSV}\r\nextra`, { mode: 0o600 });
    await expect(readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID })).rejects.toThrow(
      /Stale or corrupted Delaware CFRS artifact: receipts CSV/
    );

    await expect(
      storeDelawareCfrsCommitteeArtifacts({ ...storeInput(), receiptsCsv: "<html>Session expired</html>" })
    ).rejects.toThrow();
  });

  it("replaces an existing bundle cleanly, leaving no staging or previous directories", async () => {
    await storeDelawareCfrsCommitteeArtifacts(storeInput());
    await storeDelawareCfrsCommitteeArtifacts({
      ...storeInput(),
      reportPdfs: [{ publicReportFileName: "report1.pdf", filingCalendarId: 900, body: Buffer.from("%PDF-v2") }],
    });
    const bundle = await readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID });
    expect(bundle.reportPdfs[0]?.body.toString()).toBe("%PDF-v2");
    const { readdir } = await import("node:fs/promises");
    expect(await readdir(cacheDir)).toEqual([CF_ID]);
  });

  it("leaves the previous bundle byte-identical when a re-store fails mid-write", async () => {
    await storeDelawareCfrsCommitteeArtifacts(storeInput());
    const before = await readFile(join(cacheDir, CF_ID, "receipts.csv"), "utf8");

    // Duplicate PDF file names throw AFTER the first PDF is staged — the
    // staging directory must be discarded and the live bundle untouched.
    const duplicate = { publicReportFileName: "report1.pdf", filingCalendarId: 900, body: Buffer.from("%PDF-v2") };
    await expect(
      storeDelawareCfrsCommitteeArtifacts({
        ...storeInput(),
        receiptsCsv: `${RECEIPTS_CSV}\r\n${RECEIPTS_CSV.split("\r\n")[1]}`,
        receiptsSearchTotal: 2,
        reportPdfs: [duplicate, duplicate],
      })
    ).rejects.toThrow(/duplicate report PDF/);

    const bundle = await readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID });
    expect(bundle.receiptRows).toHaveLength(1);
    expect(await readFile(join(cacheDir, CF_ID, "receipts.csv"), "utf8")).toBe(before);
    const { readdir } = await import("node:fs/promises");
    expect((await readdir(cacheDir)).filter((name) => name.includes("staging"))).toEqual([]);
  });

  it("fails on missing bundles and parser-version drift", async () => {
    await expect(readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID })).rejects.toThrow(
      /No Delaware CFRS artifact bundle cached/
    );
    await storeDelawareCfrsCommitteeArtifacts(storeInput());
    const manifestPath = join(cacheDir, CF_ID, "manifest.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    manifest.parserVersion = 999;
    await writeFile(manifestPath, JSON.stringify(manifest), { mode: 0o600 });
    await expect(readDelawareCfrsCommitteeArtifacts({ cacheDir, cfId: CF_ID })).rejects.toThrow(/re-acquire/);
  });
});

import { readFileSync } from "node:fs";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import {
  getNcsbeArtifactPaths,
  getNcsbeArtifactStatus,
  ncsbeArtifactRelativePath,
  readNcsbeArtifact,
  readNcsbeArtifactManifest,
  storeNcsbeArtifact,
  validateNcsbeArtifactBody,
  NCSBE_ARTIFACT_SCHEMA_VERSION,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeArtifactCache.js";
import { NCSBE_PARSER_VERSION } from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

const NOW = new Date("2026-08-07T17:00:00Z");

function fixture(name: string): string {
  return readFileSync(
    fileURLToPath(new URL(`../../fixtures/northCarolinaFinance/${name}`, import.meta.url)),
    "utf8"
  );
}

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ncsbe-cache-"));
});

describe("ncsbeArtifactRelativePath", () => {
  it("maps every artifact type to a stable relative path", () => {
    expect(ncsbeArtifactRelativePath({ type: "committee_search", query: "Gadson, Marcus" })).toBe(
      "committee-search/gadson-marcus.html"
    );
    expect(ncsbeArtifactRelativePath({ type: "document_inventory", sboeId: "STA-JV516O-C-001" })).toBe(
      "document-inventory/STA-JV516O-C-001.html"
    );
    expect(ncsbeArtifactRelativePath({ type: "ie_doc_type_inventory", year: 2026 })).toBe(
      "ie-doc-type-inventory/2026.html"
    );
    expect(ncsbeArtifactRelativePath({ type: "report_cover", reportId: "229931" })).toBe(
      "report/229931/cover.html"
    );
    expect(
      ncsbeArtifactRelativePath({ type: "report_transactions", reportId: "229931", kind: "receipts", page: 1 })
    ).toBe("report/229931/receipts-p1.json");
  });

  it("refuses ids that are not filesystem-safe", () => {
    expect(() => ncsbeArtifactRelativePath({ type: "document_inventory", sboeId: "../etc" })).toThrow(
      /not filesystem-safe/
    );
    expect(() => ncsbeArtifactRelativePath({ type: "report_cover", reportId: "a/b" })).toThrow(
      /not filesystem-safe/
    );
  });
});

describe("validateNcsbeArtifactBody", () => {
  it("validates each artifact type with its pinned parser", () => {
    expect(
      validateNcsbeArtifactBody({ type: "committee_search", query: "gadson" }, fixture("committee-search-gadson.html"))
    ).toEqual({ rowCount: 1, recordCountKey: null });
    expect(
      validateNcsbeArtifactBody(
        { type: "report_transactions", reportId: "229931", kind: "receipts", page: 0 },
        fixture("receipts-gadson-229931-p0.json")
      )
    ).toEqual({ rowCount: 19, recordCountKey: 19 });
    expect(
      validateNcsbeArtifactBody({ type: "report_cover", reportId: "229931" }, fixture("report-cover-gadson-229931.html"))
    ).toEqual({ rowCount: 34, recordCountKey: null });
  });

  it("rejects a body that does not match the artifact type", () => {
    expect(() =>
      validateNcsbeArtifactBody({ type: "committee_search", query: "gadson" }, "<html>error</html>")
    ).toThrow(/marker/);
  });
});

describe("storeNcsbeArtifact", () => {
  it("validates, hashes, and installs bytes plus manifest atomically", async () => {
    const body = fixture("receipts-gadson-229931-p0.json");
    const key = { type: "report_transactions", reportId: "229931", kind: "receipts", page: 0 } as const;
    const url = "https://cf.ncsbe.gov/CFOrgLkup/GetReceipts?ReportID=229931&page=0&pageSize=300";
    const manifest = await storeNcsbeArtifact({
      cacheDir,
      key,
      url,
      body,
      sourceDocument: {
        committeeName: "GADSON FOR NORTH CAROLINA",
        sboeId: "STA-JV516O-C-001",
        reportYear: 2026,
        documentType: "Disclosure Report",
        reportType: "First Quarter",
        isAmendment: false,
        imageReceiptDate: "02/24/2026",
        dataImportDate: "02/24/2026",
        periodStartDate: "01/01/2026",
        periodEndDate: "02/14/2026",
      },
      retrievedAt: NOW,
    });

    expect(manifest.version).toBe(NCSBE_ARTIFACT_SCHEMA_VERSION);
    expect(manifest.parserVersion).toBe(NCSBE_PARSER_VERSION);
    expect(manifest.url).toBe(url);
    expect(manifest.rowCount).toBe(19);
    expect(manifest.recordCountKey).toBe(19);
    expect(manifest.retrievedAt).toBe(NOW.toISOString());
    expect(manifest.sourceDocument?.dataImportDate).toBe("02/24/2026");

    const paths = getNcsbeArtifactPaths({ cacheDir, key });
    expect(await readFile(paths.filePath, "utf8")).toBe(body);
    expect(await readNcsbeArtifactManifest(paths.manifestPath)).toEqual(manifest);

    const status = await getNcsbeArtifactStatus({ cacheDir, key });
    expect(status.status).toBe("ready");

    const read = await readNcsbeArtifact({ cacheDir, key });
    expect(read.body).toBe(body);
  });

  it("refuses to install an invalid body and leaves the good snapshot intact", async () => {
    const key = { type: "document_inventory", sboeId: "STA-JV516O-C-001" } as const;
    const body = fixture("document-inventory-gadson.html");
    await storeNcsbeArtifact({ cacheDir, key, url: "u", body, retrievedAt: NOW });
    await expect(
      storeNcsbeArtifact({ cacheDir, key, url: "u", body: "<html>error page</html>", retrievedAt: NOW })
    ).rejects.toThrow(/marker/);
    const status = await getNcsbeArtifactStatus({ cacheDir, key });
    expect(status.status).toBe("ready");
    expect((await readNcsbeArtifact({ cacheDir, key })).body).toBe(body);
  });
});

describe("getNcsbeArtifactStatus", () => {
  it("reports missing before any install", async () => {
    const status = await getNcsbeArtifactStatus({ cacheDir, key: { type: "ie_doc_type_inventory", year: 2026 } });
    expect(status.status).toBe("missing");
  });

  it("reports stale when the bytes no longer match the manifest", async () => {
    const key = { type: "committee_search", query: "gadson" } as const;
    await storeNcsbeArtifact({ cacheDir, key, url: "u", body: fixture("committee-search-gadson.html"), retrievedAt: NOW });
    const paths = getNcsbeArtifactPaths({ cacheDir, key });
    await writeFile(paths.filePath, "tampered", "utf8");
    const status = await getNcsbeArtifactStatus({ cacheDir, key });
    expect(status.status).toBe("stale");
    await expect(readNcsbeArtifact({ cacheDir, key })).rejects.toThrow(/is stale/);
  });

  it("reports stale when the manifest was written by another parser version", async () => {
    const key = { type: "committee_search", query: "gadson" } as const;
    await storeNcsbeArtifact({ cacheDir, key, url: "u", body: fixture("committee-search-gadson.html"), retrievedAt: NOW });
    const paths = getNcsbeArtifactPaths({ cacheDir, key });
    const manifest = JSON.parse(await readFile(paths.manifestPath, "utf8"));
    manifest.parserVersion = NCSBE_PARSER_VERSION - 1;
    await writeFile(paths.manifestPath, JSON.stringify(manifest), "utf8");
    const status = await getNcsbeArtifactStatus({ cacheDir, key });
    expect(status.status).toBe("stale");
  });
});

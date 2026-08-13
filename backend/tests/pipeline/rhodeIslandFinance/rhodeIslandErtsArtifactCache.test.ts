import { readFileSync } from "node:fs";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeEach, describe, expect, it } from "vitest";

import {
  ertsArtifactRelativePath,
  getErtsArtifactPaths,
  getErtsArtifactStatus,
  readErtsArtifact,
  readErtsTextArtifact,
  storeErtsArtifact,
  validateErtsArtifactBody,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactCache.js";
import { makeMinimalPdf } from "../../helpers/minimalPdf.js";

const NOW = new Date("2026-08-13T17:00:00Z");

function fixture(name: string): string {
  return readFileSync(fileURLToPath(new URL(`../../fixtures/rhodeIslandFinance/${name}`, import.meta.url)), "utf8");
}

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ri-erts-cache-"));
});

describe("ertsArtifactRelativePath", () => {
  it("maps every artifact type to a stable relative path", () => {
    expect(ertsArtifactRelativePath({ type: "organization_search", query: "McKee" })).toMatch(
      /^organization-search\/mckee-[0-9a-f]{8}\.html$/
    );
    expect(ertsArtifactRelativePath({ type: "organization_filings", orgId: "2235" })).toBe("org/2235/filings.html");
    expect(ertsArtifactRelativePath({ type: "filing_versions", filingId: "230557" })).toBe(
      "filing/230557/versions.html"
    );
    expect(
      ertsArtifactRelativePath({
        type: "filing_pdf",
        filingId: "230557",
        guid: "BEEDA139-991A-4E93-A669-749650A921A1",
      })
    ).toBe("filing/230557/beeda139-991a-4e93-a669-749650a921a1.pdf");
    expect(
      ertsArtifactRelativePath({ type: "contribution_report", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" })
    ).toBe("org/2235/report/2026-04-01_2026-06-30/contributions.html");
    expect(
      ertsArtifactRelativePath({ type: "contribution_export", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" })
    ).toBe("org/2235/report/2026-04-01_2026-06-30/contributions.csv");
    expect(ertsArtifactRelativePath({ type: "cf8_index_page", page: 3 })).toBe("cf8-index/page-3.html");
  });

  it("refuses ids that are not filesystem-safe", () => {
    expect(() => ertsArtifactRelativePath({ type: "organization_filings", orgId: "../etc" })).toThrow(/not numeric/);
    expect(() => ertsArtifactRelativePath({ type: "filing_pdf", filingId: "1", guid: "../x" })).toThrow(/not a GUID/);
    expect(() =>
      ertsArtifactRelativePath({ type: "contribution_report", orgId: "2235", beginIso: "04/01/2026", endIso: "2026-06-30" })
    ).toThrow(/not an ISO date/);
    expect(() => ertsArtifactRelativePath({ type: "cf8_index_page", page: 0 })).toThrow(/page is invalid/);
  });
});

describe("validateErtsArtifactBody", () => {
  it("counts rows for each pinned artifact type", async () => {
    await expect(
      validateErtsArtifactBody({ type: "organization_filings", orgId: "2235" }, fixture("organization-filings.html"))
    ).resolves.toBe(4);
    await expect(
      validateErtsArtifactBody({ type: "filing_versions", filingId: "230557" }, fixture("filing-amendments-230557.html"))
    ).resolves.toBe(2);
    await expect(
      validateErtsArtifactBody(
        { type: "contribution_report", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" },
        `${fixture("contribution-report-summary.html")}<table id="dgrContribution"></table>`
      )
    ).resolves.toBe(5);
    await expect(
      validateErtsArtifactBody(
        { type: "contribution_export", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" },
        fixture("contribution-export-sample.csv")
      )
    ).resolves.toBe(4);
    await expect(
      validateErtsArtifactBody({ type: "cf8_index_page", page: 1 }, fixture("cf8-index-page1.html"))
    ).resolves.toBeGreaterThan(0);
  });

  it("accepts a no-rows report page as a valid zero-grouping state", async () => {
    await expect(
      validateErtsArtifactBody(
        { type: "contribution_report", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" },
        "<p>No Contributions were found for the Search criteria you entered.</p>"
      )
    ).resolves.toBe(0);
  });

  it("fails closed on a body that lost its grid", async () => {
    await expect(
      validateErtsArtifactBody({ type: "organization_filings", orgId: "2235" }, "<html>login</html>")
    ).rejects.toThrow(/does not contain/);
    await expect(
      validateErtsArtifactBody(
        { type: "contribution_report", orgId: "2235", beginIso: "2026-04-01", endIso: "2026-06-30" },
        "<html>Checking your browser…</html>"
      )
    ).rejects.toThrow(/neither a result grid nor a no-rows page/);
  });

  it("requires a text layer on a filing PDF", async () => {
    const pdf = makeMinimalPdf([{ text: "2. Individuals", x: 48, y: 700 }]);
    await expect(
      validateErtsArtifactBody({ type: "filing_pdf", filingId: "230557", guid: "beeda139-991a-4e93-a669-749650a921a1" }, pdf)
    ).resolves.toBeGreaterThan(0);
    await expect(
      validateErtsArtifactBody(
        { type: "filing_pdf", filingId: "230557", guid: "beeda139-991a-4e93-a669-749650a921a1" },
        "not a pdf"
      )
    ).rejects.toThrow(/%PDF-/);
  });
});

describe("store / status / read round-trip", () => {
  const key = { type: "organization_filings", orgId: "2235" } as const;

  it("installs bytes plus manifest and reads them back", async () => {
    const body = fixture("organization-filings.html");
    const manifest = await storeErtsArtifact({
      cacheDir,
      key,
      url: "https://www.ricampaignfinance.com/RIPublic/Filings.aspx",
      body,
      source: { organizationName: "DANIEL J MCKEE" },
      retrievedAt: NOW,
    });
    expect(manifest.rowCount).toBe(4);
    expect(manifest.retrievedAt).toBe(NOW.toISOString());

    const status = await getErtsArtifactStatus({ cacheDir, key });
    expect(status.status).toBe("ready");

    const { text, manifest: readManifest } = await readErtsTextArtifact({ cacheDir, key });
    expect(text).toBe(body);
    expect(readManifest.sha256).toBe(manifest.sha256);
  });

  it("round-trips a binary PDF artifact byte-exact", async () => {
    const pdfKey = { type: "filing_pdf", filingId: "230557", guid: "beeda139-991a-4e93-a669-749650a921a1" } as const;
    const pdf = makeMinimalPdf([{ text: "5. Ending Cash Balance", x: 48, y: 700 }]);
    await storeErtsArtifact({ cacheDir, key: pdfKey, url: "https://ricampaignfinance.com/ExportDocs/x.pdf", body: pdf });
    const { bytes } = await readErtsArtifact({ cacheDir, key: pdfKey });
    expect(new Uint8Array(bytes)).toEqual(pdf);
    await expect(readErtsTextArtifact({ cacheDir, key: pdfKey })).rejects.toThrow(/binary/);
  });

  it("never installs an invalid body over a good snapshot", async () => {
    const body = fixture("organization-filings.html");
    await storeErtsArtifact({ cacheDir, key, url: "https://x", body, retrievedAt: NOW });
    await expect(storeErtsArtifact({ cacheDir, key, url: "https://x", body: "<html>login</html>" })).rejects.toThrow(
      /does not contain/
    );
    expect((await readErtsTextArtifact({ cacheDir, key })).text).toBe(body);
  });

  it("reads corrupted bytes as stale, never ready", async () => {
    await storeErtsArtifact({ cacheDir, key, url: "https://x", body: fixture("organization-filings.html") });
    const { filePath } = getErtsArtifactPaths({ cacheDir, key });
    // Same-length corruption — only the hash can catch it.
    const bytes = await readFile(filePath, "utf8");
    await writeFile(filePath, bytes.replace("230557", "999999"), "utf8");
    expect((await getErtsArtifactStatus({ cacheDir, key })).status).toBe("stale");
    await expect(readErtsArtifact({ cacheDir, key })).rejects.toThrow(/stale/);
  });

  it("reads a manifest copied under another key as stale", async () => {
    await storeErtsArtifact({ cacheDir, key, url: "https://x", body: fixture("organization-filings.html") });
    const otherKey = { type: "organization_filings", orgId: "9999" } as const;
    const from = getErtsArtifactPaths({ cacheDir, key });
    const to = getErtsArtifactPaths({ cacheDir, key: otherKey });
    await storeErtsArtifact({ cacheDir, key: otherKey, url: "https://x", body: fixture("organization-filings.html") });
    await writeFile(to.manifestPath, await readFile(from.manifestPath, "utf8"));
    expect((await getErtsArtifactStatus({ cacheDir, key: otherKey })).status).toBe("stale");
  });

  it("reports a never-fetched artifact as missing", async () => {
    expect((await getErtsArtifactStatus({ cacheDir, key: { type: "cf8_index_page", page: 1 } })).status).toBe("missing");
  });
});

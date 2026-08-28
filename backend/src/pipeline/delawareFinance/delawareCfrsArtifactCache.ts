// Delaware CFRS artifact cache (plan-delaware-finance.md architecture).
//
// The sync is cache-only: it never touches the live portal. This module owns
// the restricted on-disk artifact store the acquisition layer (Phase 2)
// writes and the sync reads. Shape, per committee (keyed by canonical
// CF_ID):
//
//   <cacheDir>/<CF_ID>/receipts.csv          full receipts export
//   <cacheDir>/<CF_ID>/expenses.csv          full expenses export
//   <cacheDir>/<CF_ID>/filed_reports.html    rendered filed-reports grid
//   <cacheDir>/<CF_ID>/reports/<sha16>.pdf   one per report document
//   <cacheDir>/<CF_ID>/manifest.json         the commit marker
//
// A store builds the ENTIRE bundle (manifest included) in a staging
// directory beside the final one and swaps it in only when complete — a
// failure mid-store leaves the previous bundle byte-identical, never a
// mixed-vintage directory whose old manifest points at overwritten files.
// The manifest records SHA-256 + byte size for every file, the parser
// version, the portal's stored-search totals (the sync's count==total gate
// re-checks them), the acquisition MemberID, and one retrievedAt for the
// whole bundle. Reads recompute every hash and re-run the parsers; any
// disagreement throws.
//
// PII: receipts artifacts carry contributor street addresses — directories
// are 0700, files 0600, and the cache directory must never enter git.

import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import {
  parseDelawareExpensesCsv,
  parseDelawareFiledReportsHtml,
  parseDelawareReceiptsCsv,
  type DelawareExpenseCsvRow,
  type DelawareFiledReportRow,
  type DelawareReceiptCsvRow,
} from "./delawareCfrsParsers.js";

export const DELAWARE_CFRS_ARTIFACT_SCHEMA_VERSION = 1;
/** Bump when a parser change invalidates previously cached artifacts. */
export const DELAWARE_CFRS_PARSER_VERSION = 1;

export const DEFAULT_DELAWARE_CFRS_CACHE_DIR = "artifact-cache/delaware-cfrs";

export type DelawareCfrsReportPdfInput = {
  /** The portal's opaque public report file name (manifest identity). */
  publicReportFileName: string;
  filingCalendarId: number;
  body: Buffer;
};

type ManifestFileEntry = { path: string; sha256: string; byteSize: number };

export type DelawareCfrsCommitteeArtifactManifest = {
  version: typeof DELAWARE_CFRS_ARTIFACT_SCHEMA_VERSION;
  parserVersion: number;
  cfId: string;
  /** Portal acquisition key — never written to product tables. */
  memberId: number;
  sourceUrl: string;
  retrievedAt: string;
  receiptsSearchTotal: number;
  expensesSearchTotal: number;
  files: {
    receiptsCsv: ManifestFileEntry;
    expensesCsv: ManifestFileEntry;
    filedReportsHtml: ManifestFileEntry;
    reportPdfs: (ManifestFileEntry & { publicReportFileName: string; filingCalendarId: number })[];
  };
};

function requireCfId(value: string): string {
  const normalized = value.trim();
  if (!/^\d{8}$/.test(normalized)) {
    throw new Error(`Invalid Delaware CF_ID for artifact cache: ${value}`);
  }
  return normalized;
}

function sha256(body: Buffer): string {
  return createHash("sha256").update(body).digest("hex");
}

function pdfRelativePath(publicReportFileName: string): string {
  return join("reports", `${sha256(Buffer.from(publicReportFileName, "utf8")).slice(0, 16)}.pdf`);
}

async function writeRestricted(path: string, body: Buffer | string): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await chmod(dirname(path), 0o700);
  const temporary = `${path}.tmp-${process.pid}`;
  try {
    await writeFile(temporary, body, { mode: 0o600 });
    await rename(temporary, path);
    await chmod(path, 0o600);
  } catch (error) {
    await rm(temporary, { force: true });
    throw error;
  }
}

export async function storeDelawareCfrsCommitteeArtifacts(input: {
  cacheDir?: string;
  cfId: string;
  memberId: number;
  sourceUrl: string;
  receiptsCsv: string;
  receiptsSearchTotal: number;
  expensesCsv: string;
  expensesSearchTotal: number;
  filedReportsHtml: string;
  reportPdfs: readonly DelawareCfrsReportPdfInput[];
  retrievedAt?: Date;
}): Promise<DelawareCfrsCommitteeArtifactManifest> {
  const cfId = requireCfId(input.cfId);
  if (!Number.isSafeInteger(input.memberId) || input.memberId <= 0) {
    throw new Error(`Invalid Delaware MemberID for artifact cache: ${input.memberId}`);
  }
  // Validate before storing: a body the parser rejects is never cached.
  parseDelawareReceiptsCsv(input.receiptsCsv);
  parseDelawareExpensesCsv(input.expensesCsv);
  parseDelawareFiledReportsHtml(input.filedReportsHtml);

  const cacheDir = input.cacheDir ?? DEFAULT_DELAWARE_CFRS_CACHE_DIR;
  const committeeDir = join(cacheDir, cfId);
  // Stage beside the final directory (same filesystem, so the rename below
  // is a plain directory move) and swap only once the bundle is complete —
  // a failure anywhere in here must leave the previous bundle untouched.
  const stagingDir = join(cacheDir, `${cfId}.staging-${process.pid}`);
  try {
    const entry = async (relativePath: string, body: Buffer): Promise<ManifestFileEntry> => {
      await writeRestricted(join(stagingDir, relativePath), body);
      return { path: relativePath, sha256: sha256(body), byteSize: body.byteLength };
    };

    const seenPdfPaths = new Set<string>();
    const reportPdfs: DelawareCfrsCommitteeArtifactManifest["files"]["reportPdfs"] = [];
    for (const pdf of input.reportPdfs) {
      const relativePath = pdfRelativePath(pdf.publicReportFileName);
      if (seenPdfPaths.has(relativePath)) {
        throw new Error(`duplicate report PDF in bundle: ${pdf.publicReportFileName}`);
      }
      seenPdfPaths.add(relativePath);
      reportPdfs.push({
        ...(await entry(relativePath, pdf.body)),
        publicReportFileName: pdf.publicReportFileName,
        filingCalendarId: pdf.filingCalendarId,
      });
    }

    const manifest: DelawareCfrsCommitteeArtifactManifest = {
      version: DELAWARE_CFRS_ARTIFACT_SCHEMA_VERSION,
      parserVersion: DELAWARE_CFRS_PARSER_VERSION,
      cfId,
      memberId: input.memberId,
      sourceUrl: input.sourceUrl,
      retrievedAt: (input.retrievedAt ?? new Date()).toISOString(),
      receiptsSearchTotal: input.receiptsSearchTotal,
      expensesSearchTotal: input.expensesSearchTotal,
      files: {
        receiptsCsv: await entry("receipts.csv", Buffer.from(input.receiptsCsv, "utf8")),
        expensesCsv: await entry("expenses.csv", Buffer.from(input.expensesCsv, "utf8")),
        filedReportsHtml: await entry("filed_reports.html", Buffer.from(input.filedReportsHtml, "utf8")),
        reportPdfs,
      },
    };
    await writeRestricted(join(stagingDir, "manifest.json"), JSON.stringify(manifest, null, 1));

    // Swap via move-aside: the previous bundle is never deleted until the
    // new one is installed. A failed install renames it back; a crash
    // mid-swap leaves it on disk under the .previous name (a read then says
    // "no bundle cached" — fail-closed, and the data remains recoverable).
    const previousDir = join(cacheDir, `${cfId}.previous-${process.pid}`);
    await rm(previousDir, { recursive: true, force: true });
    let hadPrevious = true;
    try {
      await rename(committeeDir, previousDir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
      hadPrevious = false;
    }
    try {
      await rename(stagingDir, committeeDir);
    } catch (error) {
      if (hadPrevious) {
        await rename(previousDir, committeeDir).catch(() => {});
      }
      throw error;
    }
    if (hadPrevious) {
      await rm(previousDir, { recursive: true, force: true });
    }
    return manifest;
  } catch (error) {
    await rm(stagingDir, { recursive: true, force: true });
    throw error;
  }
}

export type DelawareCfrsCommitteeArtifacts = {
  manifest: DelawareCfrsCommitteeArtifactManifest;
  receiptRows: DelawareReceiptCsvRow[];
  receiptsMalformedRowCount: number;
  expenseRows: DelawareExpenseCsvRow[];
  expensesMalformedRowCount: number;
  filedReportRows: DelawareFiledReportRow[];
  filedReportsGridTotal: number | null;
  reportPdfs: { publicReportFileName: string; filingCalendarId: number; body: Buffer }[];
};

async function readVerified(committeeDir: string, entry: ManifestFileEntry, label: string): Promise<Buffer> {
  let body: Buffer;
  try {
    body = await readFile(join(committeeDir, entry.path));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Missing Delaware CFRS artifact: ${label} (${entry.path})`);
    }
    throw error;
  }
  if (body.byteLength !== entry.byteSize || sha256(body) !== entry.sha256) {
    throw new Error(`Stale or corrupted Delaware CFRS artifact: ${label} (${entry.path})`);
  }
  return body;
}

export async function readDelawareCfrsCommitteeArtifacts(input: {
  cacheDir?: string;
  cfId: string;
}): Promise<DelawareCfrsCommitteeArtifacts> {
  const cfId = requireCfId(input.cfId);
  const committeeDir = join(input.cacheDir ?? DEFAULT_DELAWARE_CFRS_CACHE_DIR, cfId);
  let manifestBody: string;
  try {
    manifestBody = await readFile(join(committeeDir, "manifest.json"), "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`No Delaware CFRS artifact bundle cached for CF_ID ${cfId}`);
    }
    throw error;
  }
  const manifest = JSON.parse(manifestBody) as DelawareCfrsCommitteeArtifactManifest;
  if (manifest.version !== DELAWARE_CFRS_ARTIFACT_SCHEMA_VERSION) {
    throw new Error(`Unsupported Delaware CFRS artifact schema version ${manifest.version} for CF_ID ${cfId}`);
  }
  if (manifest.parserVersion !== DELAWARE_CFRS_PARSER_VERSION) {
    throw new Error(
      `Delaware CFRS artifact bundle for CF_ID ${cfId} was written by parser version ${manifest.parserVersion}, ` +
        `current is ${DELAWARE_CFRS_PARSER_VERSION} — re-acquire`
    );
  }
  if (manifest.cfId !== cfId) {
    throw new Error(`Delaware CFRS artifact bundle identity mismatch: requested ${cfId}, stored ${manifest.cfId}`);
  }

  const receipts = parseDelawareReceiptsCsv(
    (await readVerified(committeeDir, manifest.files.receiptsCsv, "receipts CSV")).toString("utf8")
  );
  const expenses = parseDelawareExpensesCsv(
    (await readVerified(committeeDir, manifest.files.expensesCsv, "expenses CSV")).toString("utf8")
  );
  const filedReports = parseDelawareFiledReportsHtml(
    (await readVerified(committeeDir, manifest.files.filedReportsHtml, "filed-reports grid")).toString("utf8")
  );
  const reportPdfs = [];
  for (const pdfEntry of manifest.files.reportPdfs) {
    reportPdfs.push({
      publicReportFileName: pdfEntry.publicReportFileName,
      filingCalendarId: pdfEntry.filingCalendarId,
      body: await readVerified(committeeDir, pdfEntry, `report PDF ${pdfEntry.publicReportFileName}`),
    });
  }
  return {
    manifest,
    receiptRows: receipts.rows,
    receiptsMalformedRowCount: receipts.malformedRowCount,
    expenseRows: expenses.rows,
    expensesMalformedRowCount: expenses.malformedRowCount,
    filedReportRows: filedReports.rows,
    filedReportsGridTotal: filedReports.total,
    reportPdfs,
  };
}

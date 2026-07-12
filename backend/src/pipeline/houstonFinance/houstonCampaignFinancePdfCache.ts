import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import type { HoustonFinanceReportIndexRecord } from "./houstonFinanceTypes.js";

export const DEFAULT_HOUSTON_FINANCE_PDF_CACHE_DIR = "scratch/houston-campaign-finance/pdfs";
const MAX_PDF_BYTES = 50 * 1024 * 1024;

function safePart(value: string): string {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new Error("Invalid Houston finance cache identity");
  return value;
}

export function houstonFinancePdfCachePath(cacheDir: string, report: HoustonFinanceReportIndexRecord): string {
  return resolve(cacheDir, `${safePart(report.sourceSystem)}_${safePart(report.reportId)}.pdf`);
}

export function validateHoustonFinancePdf(data: Uint8Array): void {
  if (data.length < 5 || data.length > MAX_PDF_BYTES || new TextDecoder().decode(data.slice(0, 5)) !== "%PDF-") {
    throw new Error("Invalid Houston campaign-finance PDF");
  }
}

export async function readCachedHoustonFinancePdf(cacheDir: string, report: HoustonFinanceReportIndexRecord): Promise<Uint8Array | null> {
  const path = houstonFinancePdfCachePath(cacheDir, report);
  try {
    const fileStat = await stat(path);
    if (!fileStat.isFile() || fileStat.size > MAX_PDF_BYTES) return null;
    const data = new Uint8Array(await readFile(path));
    validateHoustonFinancePdf(data);
    return data;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export async function cacheHoustonFinancePdf(cacheDir: string, report: HoustonFinanceReportIndexRecord, data: Uint8Array): Promise<string> {
  validateHoustonFinancePdf(data);
  await mkdir(resolve(cacheDir), { recursive: true });
  const path = houstonFinancePdfCachePath(cacheDir, report);
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, data, { flag: "wx" });
  await rename(temporary, path);
  return path;
}

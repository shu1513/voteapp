import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import {
  parseNevadaContributionCsv,
  parseNevadaCsvDate,
  parseNevadaExpenditureCsv,
  type NevadaContributionCsvRow,
  type NevadaExpenditureCsvRow,
} from "./nevadaAuroraCsv.js";
import type { NevadaReportListRow } from "./nevadaReportSummary.js";

// Browser-harvested AURORA artifacts (the WAF blocks server-side fetch; see
// docs/plans/nevada-finance.md). Layout under the artifact dir:
//   contributions/YYYY-MM.csv           date-only statewide export for a month
//   contributions/YYYY-MM-a.csv, -b.csv the same month split when the export
//                                       cap fired (suffixes must cover the
//                                       month; a plain file plus splits is an
//                                       error - it would double count)
//   expenditures/...                    same shapes
//   candidates/<year>/roster.json       individual-search roster (NV SOS)
//   candidates/<year>/<slug>/reports.json        detail-page report list
//   candidates/<year>/<slug>/reports/<md5>.html  raw ViewCCEReport pages

export const DEFAULT_NEVADA_AURORA_ARTIFACT_DIR = "scratch/nevada-campaign-finance/aurora";

export function nevadaSlugForFilerName(filerName: string): string {
  const slug = filerName
    .toLocaleLowerCase("en-US")
    .normalize("NFKD")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!slug) {
    throw new Error(`Cannot derive Nevada artifact slug from ${JSON.stringify(filerName)}`);
  }
  return slug;
}

export function nevadaReportHtmlFileName(syn: string): string {
  return `${createHash("md5").update(syn).digest("hex")}.html`;
}

export type NevadaRosterArtifactEntry = {
  name: string;
  party: string;
  detailToken: string;
  slug: string;
};

function requireString(value: unknown, context: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Nevada artifact field ${context} must be a non-empty string`);
  }
  return value.trim();
}

export async function readNevadaRosterArtifact(
  artifactDir: string,
  electionYear: number
): Promise<NevadaRosterArtifactEntry[]> {
  const path = join(artifactDir, "candidates", String(electionYear), "roster.json");
  const parsed: unknown = JSON.parse(await readFile(path, "utf8"));
  const entries = (parsed as { entries?: unknown }).entries;
  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error(`Nevada roster artifact ${path} has no entries`);
  }
  const seenSlugs = new Set<string>();
  return entries.map((entry, index) => {
    const record = entry as Record<string, unknown>;
    const name = requireString(record.name, `roster entry ${index} name`);
    const slug = requireString(record.slug, `roster entry ${index} slug`);
    if (seenSlugs.has(slug)) {
      throw new Error(`Nevada roster artifact ${path} repeats slug ${slug}`);
    }
    seenSlugs.add(slug);
    return {
      name,
      party: typeof record.party === "string" ? record.party : "",
      detailToken: requireString(record.detail_token, `roster entry ${index} detail_token`),
      slug,
    };
  });
}

export type NevadaCandidateReportsArtifact = {
  name: string;
  detailToken: string;
  reportRows: NevadaReportListRow[];
};

export async function readNevadaCandidateReportsArtifact(
  artifactDir: string,
  electionYear: number,
  slug: string
): Promise<NevadaCandidateReportsArtifact> {
  const path = join(artifactDir, "candidates", String(electionYear), slug, "reports.json");
  const parsed = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
  const reports = parsed.reports;
  if (!Array.isArray(reports)) {
    throw new Error(`Nevada reports artifact ${path} has no reports array`);
  }
  return {
    name: requireString(parsed.name, `${path} name`),
    detailToken: requireString(parsed.detail_token, `${path} detail_token`),
    reportRows: reports.map((row, index) => {
      const record = row as Record<string, unknown>;
      const year = Number(record.year);
      if (!Number.isInteger(year) || year < 2004 || year > 2100) {
        throw new Error(`Nevada reports artifact ${path} row ${index} has invalid year ${String(record.year)}`);
      }
      return {
        reportName: requireString(record.report_name, `${path} row ${index} report_name`),
        year,
        fileDate: parseNevadaCsvDate(
          requireString(record.file_date, `${path} row ${index} file_date`),
          `${path} row ${index}`
        ),
        office: requireString(record.office, `${path} row ${index} office`),
        syn: requireString(record.syn, `${path} row ${index} syn`),
      };
    }),
  };
}

export async function readNevadaReportHtmlArtifact(
  artifactDir: string,
  electionYear: number,
  slug: string,
  syn: string
): Promise<string> {
  const path = join(
    artifactDir,
    "candidates",
    String(electionYear),
    slug,
    "reports",
    nevadaReportHtmlFileName(syn)
  );
  return readFile(path, "utf8");
}

export type NevadaMonthlyCsvKind = "contributions" | "expenditures";

export type NevadaMonthlyCsvLoad<TRow> = {
  rows: TRow[];
  monthsLoaded: string[];
  fileCount: number;
};

function monthRange(startMonth: string, endMonth: string): string[] {
  if (!/^\d{4}-\d{2}$/.test(startMonth) || !/^\d{4}-\d{2}$/.test(endMonth) || startMonth > endMonth) {
    throw new Error(`Invalid Nevada month range ${startMonth}..${endMonth}`);
  }
  const months: string[] = [];
  let [year, month] = startMonth.split("-").map(Number);
  for (;;) {
    const key = `${year}-${String(month).padStart(2, "0")}`;
    months.push(key);
    if (key === endMonth) break;
    month += 1;
    if (month > 12) {
      month = 1;
      year += 1;
    }
    if (months.length > 60) throw new Error(`Nevada month range ${startMonth}..${endMonth} too long`);
  }
  return months;
}

async function readMonthlyKind<TRow>(
  artifactDir: string,
  kind: NevadaMonthlyCsvKind,
  startMonth: string,
  endMonth: string,
  parse: (text: string) => TRow[]
): Promise<NevadaMonthlyCsvLoad<TRow>> {
  const dir = join(artifactDir, kind);
  const names = await readdir(dir);
  const byMonth = new Map<string, string[]>();
  for (const name of names) {
    const match = name.match(/^(\d{4}-\d{2})(-[a-z])?\.csv$/);
    if (!match) continue;
    const list = byMonth.get(match[1]) ?? [];
    list.push(name);
    byMonth.set(match[1], list);
  }
  const months = monthRange(startMonth, endMonth);
  const rows: TRow[] = [];
  let fileCount = 0;
  for (const month of months) {
    const files = (byMonth.get(month) ?? []).sort();
    if (files.length === 0) {
      throw new Error(`Nevada ${kind} artifact for ${month} is missing in ${dir}`);
    }
    const hasPlain = files.includes(`${month}.csv`);
    if (hasPlain && files.length > 1) {
      throw new Error(
        `Nevada ${kind} artifacts for ${month} mix a full-month file with split files (${files.join(", ")}); that would double count`
      );
    }
    if (!hasPlain) {
      // Split files must be a contiguous -a, -b, ... run of at least two: a
      // lone -a (the cap fired, so a sibling must exist) or a gap means part
      // of the month's export was never saved and would silently undercount.
      const expected = files.map((_, index) => `${month}-${String.fromCharCode(97 + index)}.csv`);
      if (files.length < 2 || files.some((file, index) => file !== expected[index])) {
        throw new Error(
          `Nevada ${kind} artifacts for ${month} are an incomplete split set (${files.join(", ")}); ` +
            `expected ${month}.csv or a contiguous ${month}-a.csv, ${month}-b.csv, ... run`
        );
      }
    }
    for (const file of files) {
      rows.push(...parse(await readFile(join(dir, file), "utf8")));
      fileCount += 1;
    }
  }
  return { rows, monthsLoaded: months, fileCount };
}

export async function readNevadaMonthlyContributions(
  artifactDir: string,
  startMonth: string,
  endMonth: string
): Promise<NevadaMonthlyCsvLoad<NevadaContributionCsvRow>> {
  return readMonthlyKind(artifactDir, "contributions", startMonth, endMonth, parseNevadaContributionCsv);
}

export async function readNevadaMonthlyExpenditures(
  artifactDir: string,
  startMonth: string,
  endMonth: string
): Promise<NevadaMonthlyCsvLoad<NevadaExpenditureCsvRow>> {
  return readMonthlyKind(artifactDir, "expenditures", startMonth, endMonth, parseNevadaExpenditureCsv);
}

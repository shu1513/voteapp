import type {
  HoustonFinanceContribution,
  HoustonFinanceParsedReport,
  HoustonFinanceReportIndexRecord,
} from "./houstonFinanceTypes.js";
import {
  parseHoustonDisclosureOfficeTarget,
  type HoustonFinanceOfficeTarget,
} from "./houstonFinanceOfficeTargets.js";

export type HoustonPdfCell = { text: string; x: number };
export type HoustonPdfLine = { text: string; cells: HoustonPdfCell[] };
export type HoustonPdfPage = { pageNumber: number; lines: HoustonPdfLine[] };

const MAX_PAGES = 1_000;
const DATE_AMOUNT = /^(\d{2}\/\d{2}\/\d{4})\s+(.+?)\s+\$([\d,]+\.\d{2})$/;
const LEGACY_DATE_AMOUNT = /^(\d{1,2}\/\d{1,2}\/\d{4})\s+.+?\s+\$?([\d,]+\.\d{2})$/;

function normalizeText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function isoDate(value: string): string {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value);
  if (!match) throw new Error(`Invalid Houston finance date: ${value}`);
  return `${match[3]}-${match[1].padStart(2, "0")}-${match[2].padStart(2, "0")}`;
}

function amount(value: string): number {
  const parsed = Number(value.replaceAll(",", ""));
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`Invalid Houston finance amount: ${value}`);
  return Math.round(parsed * 100) / 100;
}

function totalAmount(value: string): number {
  const parsed = Number(value.replaceAll(",", ""));
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`Invalid Houston finance total amount: ${value}`);
  return Math.round(parsed * 100) / 100;
}

type HoustonPdfTextItem = {
  str: string;
  transform: ArrayLike<number>;
};

function groupTextItems(items: HoustonPdfTextItem[]): HoustonPdfLine[] {
  const groups: Array<{ y: number; items: Array<{ text: string; x: number }> }> = [];
  for (const item of items) {
    const text = normalizeText(item.str);
    if (!text) continue;
    const x = item.transform[4] ?? 0;
    const y = item.transform[5] ?? 0;
    let group = groups.find((candidate) => Math.abs(candidate.y - y) < 2);
    if (!group) {
      group = { y, items: [] };
      groups.push(group);
    }
    group.items.push({ text, x });
  }
  return groups
    .sort((left, right) => right.y - left.y)
    .map((group) => {
      const cells = group.items.sort((left, right) => left.x - right.x);
      return { cells, text: normalizeText(cells.map((cell) => cell.text).join(" ")) };
    });
}

export async function extractHoustonFinancePdfPages(data: Uint8Array): Promise<HoustonPdfPage[]> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const loadingTask = getDocument({ data });
  const pdf = await loadingTask.promise;
  if (pdf.numPages <= 0 || pdf.numPages > MAX_PAGES) {
    throw new Error(`Houston finance PDF page count is outside bounds: ${pdf.numPages}`);
  }
  const pages: HoustonPdfPage[] = [];
  try {
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      const items = content.items
        .filter((item) => "str" in item)
        .map((item) => ({ str: item.str, transform: item.transform }));
      pages.push({ pageNumber, lines: groupTextItems(items) });
    }
    return pages;
  } finally {
    await loadingTask.destroy();
  }
}

function findCoverValue(lines: readonly HoustonPdfLine[], pattern: RegExp, label: string): string {
  for (let index = 0; index < lines.length; index += 1) {
    const direct = pattern.exec(lines[index]!.text)?.[1];
    if (direct) return normalizeText(direct);
  }
  throw new Error(`Houston finance PDF is missing ${label}`);
}

function candidateName(lines: readonly HoustonPdfLine[], fallback: string): string {
  for (const line of lines) {
    const match = /13 C\s*\/\s*OH NAME\s+(.+?)\s+14 Filer ID/i.exec(line.text);
    if (match?.[1]) return normalizeText(match[1]);
    const legacyMatch = /(?:14|19|2) FILER NAME\s+(.+?)\s+(?:15|20|3) Filer ID/i.exec(line.text);
    if (legacyMatch?.[1]) return normalizeText(legacyMatch[1]);
  }
  throw new Error(`Houston finance PDF is missing candidate identity for ${normalizeText(fallback)}`);
}

function electionDate(lines: readonly HoustonPdfLine[]): string {
  const electionHeader = lines.findIndex((line) => /(?:10 ELECTION\s+ELECTION DATE|11 ELECTION)/i.test(line.text));
  if (electionHeader < 0) throw new Error("Houston finance PDF is missing the election date");
  for (let index = electionHeader + 1; index < Math.min(lines.length, electionHeader + 12); index += 1) {
    const match = /\b(\d{1,2}\/\d{1,2}\/\d{4})\b/.exec(lines[index]!.text);
    if (match?.[1]) return isoDate(match[1]);
  }
  throw new Error("Houston finance PDF is missing the election date");
}

function officeTargetAfterHeader(
  lines: readonly HoustonPdfLine[],
  headerIndex: number,
  maxFollowingLines: number
): HoustonFinanceOfficeTarget | null {
  if (headerIndex < 0) return null;
  const soughtX = lines[headerIndex]!.cells.find((cell) => /OFFICE SOUGHT/i.test(cell.text))?.x;
  for (let index = headerIndex + 1; index < Math.min(lines.length, headerIndex + maxFollowingLines); index += 1) {
    const line = lines[index]!;
    const soughtText = soughtX === undefined
      ? line.text
      : normalizeText(line.cells.filter((cell) => cell.x >= soughtX - 2).map((cell) => cell.text).join(" "));
    const target = parseHoustonDisclosureOfficeTarget(soughtText) ??
      (soughtText === line.text ? null : parseHoustonDisclosureOfficeTarget(line.text));
    if (target) return target;
  }
  return null;
}

function officeSought(lines: readonly HoustonPdfLine[]): HoustonFinanceOfficeTarget {
  const currentHeader = lines.findIndex((line) => /12 OFFICE SOUGHT/i.test(line.text));
  const current = officeTargetAfterHeader(lines, currentHeader, 6);
  if (current) return current;
  const legacyHeader = lines.findIndex((line) => /OFFICE HELD.*OFFICE SOUGHT/i.test(line.text));
  const legacy = officeTargetAfterHeader(lines, legacyHeader, 5);
  if (legacy) return legacy;
  throw new Error("Houston finance PDF has an unsupported or ambiguous office sought");
}

function reportPeriod(lines: readonly HoustonPdfLine[]): { periodStart: string; periodEnd: string } {
  for (const line of lines) {
    const match = /(\d{1,2}\/\d{1,2}\/\d{4})\s+THROUGH\s+(\d{1,2}\/\d{1,2}\/\d{4})/i.exec(line.text);
    if (match?.[1] && match[2]) return { periodStart: isoDate(match[1]), periodEnd: isoDate(match[2]) };
  }
  throw new Error("Houston finance PDF is missing the reporting period");
}

function occupationFromBlock(lines: readonly HoustonPdfLine[]): string | null {
  const labelIndex = lines.findIndex((line) => /Principal occupation\s*\/\s*Job title/i.test(line.text));
  if (labelIndex < 0) return null;
  const label = lines[labelIndex]!;
  const employerX = label.cells.find((cell) => /Employer/i.test(cell.text))?.x ?? 300;
  for (let index = labelIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]!;
    if (DATE_AMOUNT.test(line.text) || /^Forms provided by/i.test(line.text)) return null;
    if (/^(Date|Contributor address|Principal occupation)/i.test(line.text)) continue;
    const occupation = normalizeText(line.cells.filter((cell) => cell.x < employerX).map((cell) => cell.text).join(" "));
    if (occupation && !/^(N\/?A|NONE|NOT PROVIDED)$/i.test(occupation)) return occupation;
    return null;
  }
  return null;
}

function contributionsFromPage(page: HoustonPdfPage, sourceUrl: string): HoustonFinanceContribution[] {
  const title = page.lines.map((line) => line.text).join(" ");
  if (!/SCHEDULE A[12]/i.test(title) || !/(MONETARY|NON-MONETARY).*POLITICAL CONTRIBUTIONS/i.test(title)) {
    return [];
  }
  const starts = page.lines.flatMap((line, index) => (DATE_AMOUNT.test(line.text) ? [index] : []));
  const contributions: HoustonFinanceContribution[] = [];
  for (let position = 0; position < starts.length; position += 1) {
    const start = starts[position]!;
    const match = DATE_AMOUNT.exec(page.lines[start]!.text);
    if (!match?.[1] || !match[2] || !match[3]) continue;
    const next = starts[position + 1] ?? page.lines.length;
    contributions.push({
      contributionDate: isoDate(match[1]),
      contributorName: normalizeText(match[2].replace(/out-of-state PAC.*$/i, "")),
      amount: amount(match[3]),
      occupation: occupationFromBlock(page.lines.slice(start + 1, next)),
      sourceUrl,
    });
  }
  return contributions;
}

function legacyContributionsFromPage(page: HoustonPdfPage, sourceUrl: string): HoustonFinanceContribution[] {
  const title = page.lines.map((line) => line.text).join(" ");
  if (!/SCHEDULE A[12]/i.test(title) || !/(MONETARY|NON-MONETARY).*POLITICAL CONTRIBUTIONS/i.test(title)) return [];
  const starts = page.lines.flatMap((line, index) => (/^4 Date 5 Full name of contributor/i.test(line.text) ? [index] : []));
  const contributions: HoustonFinanceContribution[] = [];
  for (let position = 0; position < starts.length; position += 1) {
    const start = starts[position]!;
    const block = page.lines.slice(start + 1, starts[position + 1] ?? page.lines.length);
    const addressIndex = block.findIndex((line) => /^6 Contributor address/i.test(line.text));
    if (addressIndex <= 0) continue;
    const contributorName = normalizeText(
      block.slice(0, addressIndex).map((line) => line.text.replace(/\s+7 Amount of contributions.*$/i, "")).join(" ")
    );
    const dateAmountLine = block.slice(addressIndex + 1).find((line) => LEGACY_DATE_AMOUNT.test(line.text));
    const match = dateAmountLine ? LEGACY_DATE_AMOUNT.exec(dateAmountLine.text) : null;
    if (!contributorName || !match?.[1] || !match[2]) continue;
    contributions.push({
      contributionDate: isoDate(match[1]),
      contributorName,
      amount: amount(match[2]),
      occupation: occupationFromBlock(block),
      sourceUrl,
    });
  }
  return contributions;
}

function directContributionTotal(lines: readonly HoustonPdfLine[]): number | null {
  const index = lines.findIndex((line) => /2\.? TOTAL POLITICAL CONTRIBUTIONS/i.test(line.text));
  for (let offset = index; index >= 0 && offset < Math.min(lines.length, index + 4); offset += 1) {
    const values = [...lines[offset]!.text.matchAll(/\$\s*([\d,]+\.\d{2})/g)];
    const value = values.at(-1)?.[1];
    if (value) return totalAmount(value);
  }
  return null;
}

export async function parseHoustonCandidateFinancePdf(input: {
  data: Uint8Array;
  index: HoustonFinanceReportIndexRecord;
}): Promise<HoustonFinanceParsedReport> {
  const pages = await extractHoustonFinancePdfPages(input.data);
  const allLines = pages.flatMap((page) => page.lines);
  const period = reportPeriod(allLines);
  const pdfUrl = input.index.pdfUrl ??
    (input.index.sourceSystem === "legacy_webforms"
      ? "https://cohweb.houstontx.gov/CampaignFinanceWeb/CFRwebsiteSimpleSearch.aspx"
      : input.index.reportId);
  return {
    index: { ...input.index, periodStart: period.periodStart, periodEnd: period.periodEnd },
    candidateName: candidateName(allLines, input.index.filerName),
    electionDate: electionDate(allLines),
    officeSought: officeSought(allLines),
    periodStart: period.periodStart,
    periodEnd: period.periodEnd,
    directContributionTotal: directContributionTotal(allLines),
    contributions: pages.flatMap((page) =>
      input.index.sourceSystem === "legacy_webforms"
        ? legacyContributionsFromPage(page, pdfUrl)
        : contributionsFromPage(page, pdfUrl)
    ),
  };
}

export function selectEffectiveHoustonCandidateReports(
  reports: readonly HoustonFinanceParsedReport[]
): HoustonFinanceParsedReport[] {
  const selected = new Map<string, HoustonFinanceParsedReport>();
  const filedTime = (value: string): number => {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? 0 : parsed;
  };
  for (const report of reports) {
    const key = `${report.periodStart}\u0000${report.periodEnd}`;
    const existing = selected.get(key);
    if (!existing) {
      selected.set(key, report);
      continue;
    }
    const reportCorrection = /^COR/i.test(report.index.reportType);
    const existingCorrection = /^COR/i.test(existing.index.reportType);
    const reportPriority = report.index.sourceSystem === "ethics_efile" ? 2 : 1;
    const existingPriority = existing.index.sourceSystem === "ethics_efile" ? 2 : 1;
    if (
      (reportCorrection && !existingCorrection) ||
      (reportCorrection === existingCorrection && reportPriority > existingPriority) ||
      (reportCorrection === existingCorrection && reportPriority === existingPriority && filedTime(report.index.filedAt) > filedTime(existing.index.filedAt))
    ) {
      selected.set(key, report);
    }
  }
  return [...selected.values()].sort((left, right) => left.periodStart.localeCompare(right.periodStart));
}

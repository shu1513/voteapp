// Phase 0 probe for the Phoenix city finance module (plan-phoenix-finance.md).
// NO schema, NO writes: replays the Phoenix City Clerk eFiling portal's Kendo
// grid endpoints headlessly, downloads report PDFs into a scratch cache, and
// checks the plan's Phase 0 gates. Everything the build phases need from the
// transport is pinned here first (the client moves into the pipeline module
// in Phase 1).
//
// Wire format (captured from the live portal via an XHR intercept, 2026-08-12):
//   POST /CampaignFinance/Search/_Search{Committees,Contributors,Expenditures,Loans}
//   Content-Type: application/x-www-form-urlencoded; charset=UTF-8
//   X-Requested-With: XMLHttpRequest            <-- required; without it the
//                                                   action ignores the filters
//   body: sort=&page=N&pageSize=N&group=&filter=&<FILTERS>
// No cookies and no verification token are required (verified headlessly).
// The WAF serves an HTML "maintenance" page (HTTP 200) to non-browser user
// agents, so every response is validated as JSON before use.
//
// Filter vocabulary (extracted from each page's server-rendered Kendo data
// function, 2026-08-12):
//   _SearchCommittees:   CNAME, CTYPE, CANDNAME, TNAME, COPID, CHRNAME, CFUNC
//   _SearchContributors: ONAME, CONTNAME, COPID, ALOW, AHIGH, DLOW, DHIGH,
//                        ITEMDESCRIPTION, ISNOACTIVITY
//   _SearchExpenditures: ONAME, EXPNAME, COPID, ...(same tail)
//   _SearchLoans:        ONAME, LOANNAME, COPID, ...(same tail)
//
// Gates (fixtures hand-derived from the live portal and the Ed Hermes
// CAN-25-4 Q1-2026 report PDF on 2026-08-12; a FAIL means the source or the
// composition rules changed — re-verify by hand before building):
//   1. Transport: headless grid replay filters correctly (Mazzocco committee
//      registrations Total=2; Hermes contributions Total>=710), pagination
//      pages are disjoint, and the client rejects an HTML (maintenance) body.
//   2. Registration canonicalization: committee rows are document VERSIONS —
//      Hermes has CAN-23-7 (terminated, 2023 cycle) and CAN-25-4 (active);
//      canonical selection keeps the latest approved row per COP ID and the
//      "test committee" registrations (e.g. PAC-21-15) are excluded.
//   3. Report equations, cent-exact per parsed report:
//        sum(1(a)..1(j)) = 1(k);  1(k) - 1(l) = 1(m);
//        1(m) + 2(e) + 3 + 4 + 8 + 9 + 11 + 12 (cash) = line 13 cash;
//        line 13 cash = cover (b) period;  line 16 cash = cover (c) period;
//        cover (a) + (b) - (c) = (d).
//      Hermes hard fixture: (b) period $72,621.00 / cycle $316,139.10,
//      1(k) $73,621.00, 1(l) $1,000.00, 1(m) $72,621.00, (d) $231,095.51.
//      Chain continuity across a committee's reports: (a)_n = (d)_{n-1} and
//      (b)cycle_n = (b)cycle_{n-1} + (b)period_n.
//   4. Amendment characterization: reports per (COP ID, report name, period)
//      are scanned for duplicates; duplicates resolve to the latest
//      SubmittedDate. (Early in the cycle there may be none — the scan itself
//      plus a deterministic rule is the deliverable.)
//   5. Occupation extraction: Schedule A(1)(a) and A(1)(c) rows parse with
//      occupation + employer, and the schedule sums reconcile cent-exact to
//      receipts lines 1(a) and 1(c) (Hermes: $41,695.00 / $5,485.00).
//   6. Outside census: enumerate PAC registrations carrying the
//      "Candidate-Related Independent Expenditures" function, split standing
//      (SOS-filing) vs city-filing, current-cycle vs old, and count their
//      city expenditure rows. IE-entity fillable reports and Election Funding
//      Disclosure filings are curated channels (not grid-accessible) and are
//      reported as such — outside totals must publish NULL, never zero, for
//      unmeasured channels.
//   7. Resolver: all 16 certified November-2026 candidates map to exactly one
//      active canonical committee whose registration CandidateName matches,
//      or print an explicit reason.

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const PORTAL_BASE_URL = "https://apps-secure.phoenix.gov";
const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const PDF_CACHE_DIR = "scratch/phoenix-campaign-finance/reports";
const MAX_GRID_PAGES = 200;
const GRID_PAGE_SIZE = 100;
// The Kendo grid's political-function GUID for "Candidate-Related Independent
// Expenditures" (read from the RegFilings page's POLITICALFUNCTION select).
const CANDIDATE_IE_FUNCTION_ID = "a182d408-b233-4b2b-b444-4d260375dc5f";
const TEST_COMMITTEE_PATTERN = /\btest\b/i;

/** Certified 2026-11-03 field from the City Clerk's candidate-information page
 * (read 2026-08-12), with each candidate's current-cycle COP ID from the
 * portal registration search the same day. */
const NOVEMBER_2026_CANDIDATES: readonly {
  displayName: string;
  district: number;
  copId: string;
}[] = [
  { displayName: "Matt Evans", district: 2, copId: "CAN-22-10" },
  { displayName: "Danny Mazza", district: 2, copId: "CAN-25-1" },
  { displayName: "Julie Read", district: 2, copId: "CAN-25-7" },
  { displayName: "Ashley Harder", district: 4, copId: "CAN-25-6" },
  { displayName: "Ed Hermes", district: 4, copId: "CAN-25-4" },
  { displayName: "Cassandra Hernandez", district: 4, copId: "CAN-26-4" },
  { displayName: "Patricia Jimenez", district: 4, copId: "CAN-23-5" },
  { displayName: "Zachary Lauer", district: 4, copId: "CAN-25-3" },
  { displayName: "Michael Mazzocco", district: 4, copId: "CAN-25-2" },
  { displayName: "Robb Olivieri", district: 4, copId: "CAN-25-8" },
  { displayName: "Megan Schmitz", district: 4, copId: "CAN-25-9" },
  { displayName: "Michael Del Prete", district: 6, copId: "CAN-25-5" },
  { displayName: "Kevin Robinson", district: 6, copId: "CAN-21-16" },
  { displayName: "Frank Abasciano Jr.", district: 8, copId: "CAN-26-1" },
  { displayName: "Kesha Hodge Washington", district: 8, copId: "CAN-22-6" },
  { displayName: "Jarrett Barton Maupin Jr.", district: 8, copId: "CAN-26-3" },
];

type Gate = { name: string; pass: boolean; detail: string };

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

function moneyTokenToCents(token: string): number {
  const match = /^\(?\$([\d,]+)\.(\d{2})\)?$/.exec(token.trim());
  if (match === null) throw new Error(`Not a money token: "${token}"`);
  const cents = Number(match[1]!.replaceAll(",", "")) * 100 + Number(match[2]);
  // Parenthesised amounts are negatives on this form family.
  return token.trim().startsWith("(") ? -cents : cents;
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

type GridEnvelope = { Data: Record<string, unknown>[]; Total: number };

/** POST one Kendo grid page. Rejects any body that is not the JSON envelope —
 * the WAF's maintenance page is HTML with HTTP 200. */
async function gridPage(
  path: string,
  filters: Readonly<Record<string, string>>,
  page: number,
  pageSize: number,
): Promise<GridEnvelope> {
  const body = new URLSearchParams({
    sort: "",
    page: String(page),
    pageSize: String(pageSize),
    group: "",
    filter: "",
    ...filters,
  });
  const response = await fetch(`${PORTAL_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": BROWSER_USER_AGENT,
    },
    body: body.toString(),
  });
  const text = await response.text();
  return parseGridEnvelope(text, `${path} page ${page} (HTTP ${response.status})`);
}

/** Exported for the gate-1 self-test: the maintenance page must be rejected. */
export function parseGridEnvelope(text: string, context: string): GridEnvelope {
  const trimmed = text.trimStart();
  if (!trimmed.startsWith("{")) {
    const label = /maintenance/i.test(text) ? "WAF maintenance page" : "non-JSON body";
    throw new Error(`Phoenix grid returned a ${label} for ${context}`);
  }
  const parsed = JSON.parse(trimmed) as Partial<GridEnvelope>;
  if (!Array.isArray(parsed.Data) || typeof parsed.Total !== "number") {
    throw new Error(`Phoenix grid envelope missing Data/Total for ${context}`);
  }
  return { Data: parsed.Data, Total: parsed.Total };
}

/** Page through a grid until Total rows are collected. */
async function gridAll(
  path: string,
  filters: Readonly<Record<string, string>>,
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let page = 1; page <= MAX_GRID_PAGES; page += 1) {
    const envelope = await gridPage(path, filters, page, GRID_PAGE_SIZE);
    rows.push(...envelope.Data);
    if (rows.length >= envelope.Total) return rows;
    if (envelope.Data.length === 0) {
      throw new Error(
        `Phoenix grid ${path} exhausted at ${rows.length}/${envelope.Total} rows — incomplete pagination`,
      );
    }
  }
  throw new Error(`Phoenix grid ${path} exceeded ${MAX_GRID_PAGES} pages`);
}

async function fetchReportPdf(reportPackageId: string): Promise<Uint8Array> {
  if (!/^[0-9a-f-]{36}$/i.test(reportPackageId)) {
    throw new Error(`Not a report package GUID: "${reportPackageId}"`);
  }
  const cachePath = join(PDF_CACHE_DIR, `${reportPackageId}.pdf`);
  try {
    // pdfjs rejects Node Buffers — hand it a plain Uint8Array view.
    const cached = await readFile(cachePath);
    return new Uint8Array(cached.buffer, cached.byteOffset, cached.byteLength);
  } catch {
    // cache miss — fetch below
  }
  const response = await fetch(
    `${PORTAL_BASE_URL}/CampaignFinance/Reports/PrintReport/${reportPackageId}`,
    { headers: { "User-Agent": BROWSER_USER_AGENT } },
  );
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.length < 4 || String.fromCharCode(...bytes.slice(0, 4)) !== "%PDF") {
    throw new Error(`Report ${reportPackageId} is not a PDF (HTTP ${response.status})`);
  }
  await mkdir(PDF_CACHE_DIR, { recursive: true });
  await writeFile(cachePath, bytes);
  return bytes;
}

// ---------------------------------------------------------------------------
// PDF extraction (Houston-style positioned text: group items into lines by y,
// order cells by x — naive whole-page extraction scrambles this form).
// ---------------------------------------------------------------------------

type PdfCell = { text: string; x: number };
type PdfLine = { y: number; cells: PdfCell[]; text: string };
type PdfPage = { pageNumber: number; lines: PdfLine[] };

async function extractPdfPages(data: Uint8Array): Promise<PdfPage[]> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const loadingTask = getDocument({ data });
  const pdf = await loadingTask.promise;
  try {
    const pages: PdfPage[] = [];
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      const groups: { y: number; cells: PdfCell[] }[] = [];
      for (const item of content.items) {
        if (!("str" in item)) continue;
        const text = item.str.replace(/\s+/g, " ").trim();
        if (!text) continue;
        const x = item.transform[4] ?? 0;
        const y = item.transform[5] ?? 0;
        let group = groups.find((candidate) => Math.abs(candidate.y - y) < 2);
        if (!group) {
          group = { y, cells: [] };
          groups.push(group);
        }
        group.cells.push({ text, x });
      }
      pages.push({
        pageNumber,
        lines: groups
          .sort((left, right) => right.y - left.y)
          .map((group) => {
            const cells = [...group.cells].sort((left, right) => left.x - right.x);
            return { y: group.y, cells, text: cells.map((cell) => cell.text).join(" ") };
          }),
      });
    }
    return pages;
  } finally {
    await loadingTask.destroy();
  }
}

function moneyCells(line: PdfLine): { cents: number; x: number }[] {
  return line.cells
    .filter((cell) => /^\(?\$[\d,]+\.\d{2}\)?$/.test(cell.text))
    .map((cell) => ({ cents: moneyTokenToCents(cell.text), x: cell.x }));
}

type ReportCover = {
  reportName: string;
  periodFrom: string;
  periodTo: string;
  officeSought: string | null;
  beginCents: number;
  receiptsPeriodCents: number;
  receiptsCycleCents: number | null;
  disbursementsPeriodCents: number;
  disbursementsCycleCents: number | null;
  closeCents: number;
};

type ReceiptsSummary = {
  line1: Record<string, number>; // keys a..m, cash cents
  line2eCents: number;
  otherCashCents: number; // lines 3, 4, 8, 9, 11, 12 (cash column)
  line13CashCents: number;
};

type ScheduleEntry = {
  amountCents: number;
  date: string;
  name: string;
  occupation: string | null;
  employer: string | null;
};

type ParsedReport = {
  cover: ReportCover;
  receipts: ReceiptsSummary;
  line16CashCents: number;
  a1aEntries: ScheduleEntry[];
  a1cEntries: ScheduleEntry[];
};

/** Phoenix candidate election cycles run April 1 of an odd year through
 * March 31 two years later (city cycles PDF, read 2026-08-12). */
function cycleStartYear(date: Date): number {
  const year = date.getUTCFullYear();
  const odd = year % 2 === 1 ? year : year - 1;
  return date >= new Date(Date.UTC(odd, 3, 1)) ? odd : odd - 2;
}

function coverDate(text: string): Date {
  const parsed = new Date(`${text} UTC`);
  if (Number.isNaN(parsed.getTime())) throw new Error(`Unparseable cover date: "${text}"`);
  return parsed;
}

function requireLine(page: PdfPage, pattern: RegExp, label: string): PdfLine {
  const line = page.lines.find((candidate) => pattern.test(candidate.text));
  if (line === undefined) {
    throw new Error(`Report page ${page.pageNumber} is missing ${label}`);
  }
  return line;
}

function parseCover(page: PdfPage): ReportCover {
  const periodLine = requireLine(
    page,
    /Report.*:\s+\w+ \d{2}, \d{4} to \w+ \d{2}, \d{4}/,
    "the reporting-period line",
  );
  const periodMatch =
    /^(.*?):\s+(\w+ \d{2}, \d{4}) to (\w+ \d{2}, \d{4})/.exec(periodLine.text);
  if (periodMatch === null) {
    throw new Error(`Unparseable reporting period: "${periodLine.text}"`);
  }
  const officeLine = page.lines.find((line) => /Office Sought:/.test(line.text));
  const officeMatch = officeLine
    ? /Office Sought:(?: City Office:)?\s*(.+)$/.exec(officeLine.text)
    : null;

  // The "(a)".."(d)" markers render as separate text lines from their labels,
  // so every anchor keys on the label text; amounts sit on or just below it.
  const anchorIndex = page.lines.findIndex((line) =>
    /Committee value at the beginning/.test(line.text),
  );
  if (anchorIndex < 0) throw new Error("Cover is missing the (a) anchor");
  const nearbyMoney = (startIndex: number, span: number): { cents: number; x: number }[] => {
    const cells: { cents: number; x: number }[] = [];
    for (let index = startIndex; index < Math.min(startIndex + span, page.lines.length); index += 1) {
      cells.push(...moneyCells(page.lines[index]!));
    }
    return cells;
  };
  const beginCells = nearbyMoney(anchorIndex, 3);
  if (beginCells.length !== 1) {
    throw new Error(`Cover (a) expected exactly one amount, got ${beginCells.length}`);
  }

  const receiptsLine = requireLine(page, /Total receipts/, "cover line (b)");
  const receiptsCells = moneyCells(receiptsLine);
  if (receiptsCells.length < 1 || receiptsCells.length > 2) {
    throw new Error(`Cover (b) expected 1-2 amounts, got ${receiptsCells.length}`);
  }

  const disbursementsIndex = page.lines.findIndex((line) =>
    /Total disbursements/.test(line.text),
  );
  if (disbursementsIndex < 0) throw new Error("Cover is missing the (c) anchor");
  const disbursementsCells = nearbyMoney(disbursementsIndex, 3);
  if (disbursementsCells.length < 1 || disbursementsCells.length > 2) {
    throw new Error(`Cover (c) expected 1-2 amounts, got ${disbursementsCells.length}`);
  }

  const closeIndex = page.lines.findIndex((line) =>
    /Balance at close of reporting period/.test(line.text),
  );
  if (closeIndex < 0) throw new Error("Cover is missing the (d) anchor");
  const closeCells = nearbyMoney(closeIndex, 3);
  if (closeCells.length !== 1) {
    throw new Error(`Cover (d) expected exactly one amount, got ${closeCells.length}`);
  }

  return {
    reportName: periodMatch[1]!.trim(),
    periodFrom: periodMatch[2]!,
    periodTo: periodMatch[3]!,
    officeSought: officeMatch?.[1]?.trim() ?? null,
    beginCents: beginCells[0]!.cents,
    receiptsPeriodCents: receiptsCells[0]!.cents,
    receiptsCycleCents: receiptsCells[1]?.cents ?? null,
    disbursementsPeriodCents: disbursementsCells[0]!.cents,
    disbursementsCycleCents: disbursementsCells[1]?.cents ?? null,
    closeCents: closeCells[0]!.cents,
  };
}

/** Cash column sits left of the equity column on the summary pages; on the
 * observed form the cash amounts print at x≈425 and equity at x≈500. */
const SUMMARY_CASH_MAX_X = 462;

function cashAmount(line: PdfLine, label: string): number {
  const cells = moneyCells(line).filter((cell) => cell.x < SUMMARY_CASH_MAX_X);
  if (cells.length !== 1) {
    throw new Error(`${label} expected one cash amount, got ${cells.length} ("${line.text}")`);
  }
  return cells[0]!.cents;
}

// Line-1 sub-item markers "(a)".."(m)" repeat under sections 2 and 5, so
// each key anchors on its full label text.
const LINE1_LABELS: Readonly<Record<string, RegExp>> = {
  a: /^\(a\) In-State Individuals - More than \$100/,
  b: /^\(b\) In-State Individuals - \$100 or Less/,
  c: /^\(c\) Out-of-State Individuals/,
  d: /^\(d\) Candidate Committees/,
  e: /^\(e\) Political Action Committees/,
  f: /^\(f\) Political Parties/,
  g: /^\(g\) Partnerships/,
  h: /^\(h\) Corporations & Limited Liability Companies/,
  i: /^\(i\) Labor Organizations/,
  j: /^\(j\) Candidate.s Personal Monies/,
  k: /^\(k\) Monetary Contributions Subtotal/,
  l: /^\(l\) Refunds Given Back to Contributors/,
  m: /^\(m\) Net Monetary Contributions/,
};

function parseReceiptsSummaryAnchored(page: PdfPage): ReceiptsSummary {
  const line1: Record<string, number> = {};
  for (const [key, pattern] of Object.entries(LINE1_LABELS)) {
    line1[key] = cashAmount(requireLine(page, pattern, `receipts 1(${key})`), `1(${key})`);
  }
  const line2e = requireLine(page, /^\(e\) Loans Subtotal/, "receipts line 2(e)");
  const singles = [
    requireLine(page, /^3\. Rebates and Refunds Received/, "line 3"),
    requireLine(page, /^4\. Interest Accrued/, "line 4"),
    requireLine(page, /^8\. Joint Fundraising/, "line 8"),
    requireLine(page, /^9\. Payments Received for Goods/, "line 9"),
    requireLine(page, /^11\. Transfer In Surplus/, "line 11"),
    requireLine(page, /^12\. Miscellaneous Receipts/, "line 12"),
  ];
  const line13 = requireLine(page, /^13\. Total Receipts/, "receipts line 13");
  return {
    line1,
    line2eCents: cashAmount(line2e, "receipts 2(e)"),
    otherCashCents: singles.reduce(
      (sum, line, index) => sum + cashAmount(line, `receipts other #${index}`),
      0,
    ),
    line13CashCents: cashAmount(line13, "receipts 13"),
  };
}

function parseScheduleEntries(page: PdfPage): ScheduleEntry[] {
  const entries: ScheduleEntry[] = [];
  const lines = page.lines;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index]!;
    // Each entry opens with a header line: Name ... Date Contribution
    // Received $received $cumPeriod $cumCycle (amounts x>340).
    if (!/^Name Date Contribution Received/.test(line.text)) continue;
    const amounts = moneyCells(line);
    if (amounts.length === 0) continue;
    const received = amounts.sort((a, b) => a.x - b.x)[0]!;
    let name: string | null = null;
    let date: string | null = null;
    let occupation: string | null = null;
    let employer: string | null = null;
    for (let cursor = index + 1; cursor < Math.min(index + 9, lines.length); cursor += 1) {
      const detail = lines[cursor]!;
      if (/^Name Date Contribution Received/.test(detail.text)) break;
      if (name === null) {
        const dateCell = detail.cells.find((cell) => /^\d{2}\/\d{2}\/\d{4}$/.test(cell.text));
        if (dateCell !== undefined) {
          date = dateCell.text;
          name =
            detail.cells
              .filter((cell) => cell.x < 250)
              .map((cell) => cell.text)
              .join(" ") || null;
          continue;
        }
      }
      if (/^Occupation Employer$/.test(detail.text)) {
        const values = lines[cursor + 1];
        if (values !== undefined && !/^Name /.test(values.text)) {
          occupation =
            values.cells
              .filter((cell) => cell.x < 180)
              .map((cell) => cell.text)
              .join(" ") || null;
          employer =
            values.cells
              .filter((cell) => cell.x >= 180 && cell.x < 340)
              .map((cell) => cell.text)
              .join(" ") || null;
        }
        break;
      }
    }
    if (name !== null && date !== null) {
      entries.push({ amountCents: received.cents, date, name, occupation, employer });
    }
  }
  return entries;
}

async function parseReport(bytes: Uint8Array): Promise<ParsedReport> {
  const pages = await extractPdfPages(bytes);
  const coverPage = pages.find((page) => /FINANCIAL SUMMARY \(required\)/.test(page.lines.map((l) => l.text).join("\n")));
  if (coverPage === undefined) throw new Error("Report has no FINANCIAL SUMMARY cover page");
  const receiptsPage = pages.find((page) =>
    page.lines.some((line) => /^SUMMARY OF RECEIPTS \(Schedule A\)/.test(line.text)),
  );
  if (receiptsPage === undefined) throw new Error("Report has no SUMMARY OF RECEIPTS page");
  const disbursementsPage = pages.find((page) =>
    page.lines.some((line) => /^SUMMARY OF DISBURSEMENTS \(Schedule B\)/.test(line.text)),
  );
  if (disbursementsPage === undefined) {
    throw new Error("Report has no SUMMARY OF DISBURSEMENTS page");
  }
  const line16 = requireLine(
    disbursementsPage,
    /^16\. Total Disbursements/,
    "disbursements line 16",
  );

  const a1aEntries: ScheduleEntry[] = [];
  const a1cEntries: ScheduleEntry[] = [];
  for (const page of pages) {
    const heading = page.lines.slice(0, 12).map((line) => line.text).join(" ");
    if (/MONETARY CONTRIBUTIONS RECEIVED FROM IN-STATE INDIVIDUALS - MORE THAN/.test(heading)) {
      a1aEntries.push(...parseScheduleEntries(page));
    } else if (/MONETARY CONTRIBUTIONS RECEIVED FROM OUT-OF-STATE INDIVIDUALS/.test(heading)) {
      a1cEntries.push(...parseScheduleEntries(page));
    }
  }

  return {
    cover: parseCover(coverPage),
    receipts: parseReceiptsSummaryAnchored(receiptsPage),
    line16CashCents: cashAmount(line16, "disbursements 16"),
    a1aEntries,
    a1cEntries,
  };
}

// ---------------------------------------------------------------------------
// Registration handling
// ---------------------------------------------------------------------------

type RegistrationRow = {
  copId: string;
  committeeName: string;
  committeeType: string;
  candidateName: string | null;
  electionCycle: string;
  terminated: boolean;
  approved: boolean;
  approvedTimestamp: number;
  isStandingCommittee: boolean;
  functions: string[];
};

function toRegistrationRow(raw: Record<string, unknown>): RegistrationRow {
  const timestamp = /\/Date\((\d+)\)\//.exec(String(raw.AppovedTimestamp ?? ""));
  const functions = Array.isArray(raw.FUNCTIONSCAN)
    ? (raw.FUNCTIONSCAN as { TypeCode?: string; isSelected?: boolean }[])
        .filter((entry) => entry.isSelected === true && typeof entry.TypeCode === "string")
        .map((entry) => entry.TypeCode as string)
    : [];
  return {
    copId: String(raw.COPID ?? ""),
    committeeName: String(raw.CommitteeName ?? "").trim(),
    committeeType: String(raw.CommitteeType ?? ""),
    candidateName: raw.CandidateName ? String(raw.CandidateName).trim() || null : null,
    electionCycle: String(raw.ElectionCycle ?? ""),
    terminated: raw.Terminated === true,
    approved: raw.Approved === true,
    approvedTimestamp: timestamp ? Number(timestamp[1]) : 0,
    isStandingCommittee: raw.IsStandingCommittee === true,
    functions,
  };
}

/** Registration rows are document versions; the canonical row per COP ID is
 * the latest approved one (terminated wins over an older active version). */
function canonicalRegistration(rows: readonly RegistrationRow[]): RegistrationRow | null {
  const approved = rows.filter((row) => row.approved);
  if (approved.length === 0) return null;
  return approved.reduce((best, row) =>
    row.approvedTimestamp > best.approvedTimestamp ? row : best,
  );
}

function normalizeNameTokens(name: string): string[] {
  return name
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token.length > 0 && !["jr", "sr", "ii", "iii", "iv"].includes(token));
}

/** Surname must match exactly; given names match when equal or one is a
 * prefix of the other (Matt/Matthew). Fails closed on anything else. */
function candidateNameMatches(registrationName: string, displayName: string): boolean {
  const a = normalizeNameTokens(registrationName);
  const b = normalizeNameTokens(displayName);
  if (a.length === 0 || b.length === 0) return false;
  if (a[a.length - 1] !== b[b.length - 1]) return false;
  const givenA = a[0]!;
  const givenB = b[0]!;
  return givenA === givenB || givenA.startsWith(givenB) || givenB.startsWith(givenA);
}

// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  for (const arg of process.argv.slice(2)) {
    throw new Error(`Unknown Phoenix finance probe flag: ${arg}`);
  }
  const gates: Gate[] = [];

  // --- Gate 1: transport. ---
  const mazzocco = await gridPage(
    "/CampaignFinance/Search/_SearchCommittees",
    { CANDNAME: "Mazzocco" },
    1,
    10,
  );
  const hermesPage1 = await gridPage(
    "/CampaignFinance/Search/_SearchContributors",
    { COPID: "CAN-25-4" },
    1,
    10,
  );
  const hermesPage2 = await gridPage(
    "/CampaignFinance/Search/_SearchContributors",
    { COPID: "CAN-25-4" },
    2,
    10,
  );
  const rowKey = (row: Record<string, unknown>): string =>
    JSON.stringify([row.ContributorName, row.ContributionDate, row.Amount, row.SubmittedDate]);
  const page1Keys = new Set(hermesPage1.Data.map(rowKey));
  const disjoint = hermesPage2.Data.every((row) => !page1Keys.has(rowKey(row)));
  let maintenanceRejected = false;
  try {
    parseGridEnvelope(
      "<!DOCTYPE html><html><body><h2>Sorry! The requested service is currently undergoing maintenance.</h2></body></html>",
      "self-test",
    );
  } catch (error) {
    maintenanceRejected =
      error instanceof Error && /maintenance page/.test(error.message);
  }
  gates.push({
    name: "transport: filters + pagination + maintenance rejection",
    pass:
      mazzocco.Total === 2 &&
      hermesPage1.Total >= 710 &&
      hermesPage1.Data.length === 10 &&
      disjoint &&
      maintenanceRejected,
    detail: `Mazzocco registrations=${mazzocco.Total} (want 2); Hermes contributions=${hermesPage1.Total} (want >=710); pages disjoint=${disjoint}; maintenance rejected=${maintenanceRejected}`,
  });

  // --- Gate 2 + 7: registrations and resolver for all 16 candidates. ---
  const registrationsByCandidate = new Map<string, RegistrationRow[]>();
  for (const candidate of NOVEMBER_2026_CANDIDATES) {
    const rows = (
      await gridAll("/CampaignFinance/Search/_SearchCommittees", { COPID: candidate.copId })
    ).map(toRegistrationRow);
    registrationsByCandidate.set(candidate.displayName, rows);
  }
  // Hermes committee-version fixture: his CANDNAME search returns both the
  // terminated 2023 committee and the active 2025 one.
  const hermesVersions = (
    await gridAll("/CampaignFinance/Search/_SearchCommittees", { CANDNAME: "Hermes" })
  ).map(toRegistrationRow);
  const hermes237 = hermesVersions.filter((row) => row.copId === "CAN-23-7");
  const hermes254 = hermesVersions.filter((row) => row.copId === "CAN-25-4");
  const testCommittees = (
    await gridAll("/CampaignFinance/Search/_SearchCommittees", { COPID: "PAC-21-15" })
  ).map(toRegistrationRow);
  gates.push({
    name: "registration canonicalization (versions, termination, test exclusion)",
    pass:
      hermes237.length > 0 &&
      hermes237.every((row) => row.terminated) &&
      hermes254.length > 0 &&
      hermes254.every((row) => !row.terminated) &&
      testCommittees.length > 0 &&
      testCommittees.every((row) => TEST_COMMITTEE_PATTERN.test(row.committeeName)),
    detail: `Hermes CAN-23-7 rows=${hermes237.length} (all terminated=${hermes237.every((r) => r.terminated)}); CAN-25-4 rows=${hermes254.length} (all active=${hermes254.every((r) => !r.terminated)}); PAC-21-15 rows=${testCommittees.length} all match /test/`,
  });

  let resolved = 0;
  console.log("\nresolver:");
  for (const candidate of NOVEMBER_2026_CANDIDATES) {
    const rows = registrationsByCandidate.get(candidate.displayName) ?? [];
    const canonical = canonicalRegistration(rows);
    if (canonical === null) {
      console.log(`  unresolved D${candidate.district} ${candidate.displayName}: no approved registration for ${candidate.copId}`);
      continue;
    }
    if (canonical.candidateName === null) {
      console.log(`  unresolved D${candidate.district} ${candidate.displayName}: registration ${candidate.copId} carries no CandidateName`);
      continue;
    }
    if (!candidateNameMatches(canonical.candidateName, candidate.displayName)) {
      console.log(`  unresolved D${candidate.district} ${candidate.displayName}: registration name "${canonical.candidateName}" does not match`);
      continue;
    }
    if (canonical.terminated) {
      console.log(`  unresolved D${candidate.district} ${candidate.displayName}: canonical registration ${candidate.copId} is terminated`);
      continue;
    }
    resolved += 1;
    console.log(
      `  matched   D${candidate.district} ${candidate.displayName} -> ${candidate.copId} "${canonical.committeeName}" (${canonical.electionCycle})`,
    );
  }
  gates.push({
    name: "all 16 certified candidates resolve to a canonical committee",
    pass: resolved === NOVEMBER_2026_CANDIDATES.length,
    detail: `${resolved}/16 matched`,
  });

  // --- Report discovery for equation committees. ---
  // Committee sample: Hermes (has refunds), plus the two largest other filers
  // by contribution rows, plus any cohort committee with loan rows.
  type ReportRef = { reportPackageId: string; reportName: string; submittedDateMs: number };
  const reportsByCommittee = new Map<string, Map<string, ReportRef>>();
  const contributionCounts = new Map<string, number>();
  const collectRefs = (reports: Map<string, ReportRef>, rows: Record<string, unknown>[]): void => {
    for (const row of rows) {
      const id = String(row.ReportPackageId ?? "");
      if (!/^[0-9a-f-]{36}$/i.test(id)) continue;
      const submitted = /\/Date\((\d+)\)\//.exec(String(row.SubmittedDate ?? ""));
      reports.set(id, {
        reportPackageId: id,
        reportName: String(row.ReportName ?? ""),
        submittedDateMs: submitted ? Number(submitted[1]) : 0,
      });
    }
  };
  for (const candidate of NOVEMBER_2026_CANDIDATES) {
    // Contribution rows alone miss expenditure-only and no-activity reports,
    // so package discovery reads both transaction grids (loan rows are folded
    // in below from the portal-wide loans fetch).
    const contributionRows = await gridAll("/CampaignFinance/Search/_SearchContributors", {
      COPID: candidate.copId,
    });
    contributionCounts.set(candidate.copId, contributionRows.length);
    const reports = new Map<string, ReportRef>();
    collectRefs(reports, contributionRows);
    collectRefs(
      reports,
      await gridAll("/CampaignFinance/Search/_SearchExpenditures", { COPID: candidate.copId }),
    );
    reportsByCommittee.set(candidate.copId, reports);
  }
  const loanRows = await gridAll("/CampaignFinance/Search/_SearchLoans", {});
  const cohortIds = new Set(NOVEMBER_2026_CANDIDATES.map((candidate) => candidate.copId));
  const cohortLoanCopIds = [
    ...new Set(
      loanRows
        .map((row) => String(row.COPID ?? ""))
        .filter((copId) => cohortIds.has(copId)),
    ),
  ];
  for (const row of loanRows) {
    const copId = String(row.COPID ?? "");
    const reports = reportsByCommittee.get(copId);
    if (reports !== undefined) collectRefs(reports, [row]);
  }
  console.log(
    `\nloan rows portal-wide: ${loanRows.length}; cohort committees with loans: ${cohortLoanCopIds.join(", ") || "(none)"}`,
  );

  const equationCommittees = new Set<string>(["CAN-25-4"]);
  for (const copId of cohortLoanCopIds) equationCommittees.add(copId);
  for (const [copId] of [...contributionCounts.entries()]
    .filter(([id]) => id !== "CAN-25-4")
    .sort((a, b) => b[1] - a[1])) {
    if (equationCommittees.size >= 4) break;
    equationCommittees.add(copId);
  }

  // --- Gate 4: amendment canonicalization across the cohort's reports.
  // Duplicate (committee, report name) packages are superseded versions; the
  // latest SubmittedDate wins and the losers are DROPPED from every
  // downstream parse. The parsed covers then prove "one canonical report per
  // period" directly: reporting periods must not overlap (asserted below,
  // feeding this gate through periodOverlapFailures).
  let duplicatePeriods = 0;
  let supersededDropped = 0;
  for (const [copId, reports] of reportsByCommittee) {
    const byName = new Map<string, ReportRef[]>();
    for (const report of reports.values()) {
      const bucket = byName.get(report.reportName) ?? [];
      bucket.push(report);
      byName.set(report.reportName, bucket);
    }
    for (const [reportName, bucket] of byName) {
      if (bucket.length > 1) {
        duplicatePeriods += 1;
        const winner = bucket.reduce((best, entry) =>
          entry.submittedDateMs > best.submittedDateMs ? entry : best,
        );
        for (const loser of bucket) {
          if (loser.reportPackageId !== winner.reportPackageId) {
            reports.delete(loser.reportPackageId);
            supersededDropped += 1;
          }
        }
        console.log(
          `amendment ${copId} "${reportName}": ${bucket.length} packages, latest-submitted wins ${winner.reportPackageId}`,
        );
      }
    }
  }
  const periodOverlapFailures: string[] = [];

  // --- Gates 3 + 5: parse reports, check equations + occupation reconciliation. ---
  let equationReports = 0;
  const equationFailures: string[] = [];
  let hermesFixturePass = false;
  const occupationDetail: string[] = [];
  let occupationPass = true;
  const equationCommitteesParsed = new Set<string>();
  const scheduleCommittees = new Set<string>();
  const coverCorroboration: string[] = [];

  for (const copId of equationCommittees) {
    const reports = [...(reportsByCommittee.get(copId)?.values() ?? [])].sort(
      (a, b) => a.submittedDateMs - b.submittedDateMs,
    );
    if (reports.length === 0) {
      console.log(`\n${copId}: no e-filed reports discovered via contribution rows`);
      continue;
    }
    console.log(`\n${copId}: parsing ${reports.length} report(s)`);
    const parsedReports: ParsedReport[] = [];
    for (const report of reports) {
      const bytes = await fetchReportPdf(report.reportPackageId);
      const parsed = await parseReport(bytes);
      parsedReports.push(parsed);
      const { cover, receipts } = parsed;
      const line1Sum = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].reduce(
        (sum, key) => sum + (receipts.line1[key] ?? 0),
        0,
      );
      const checks: [string, boolean][] = [
        ["sum(1a..1j)=1k", line1Sum === receipts.line1.k],
        ["1k-1l=1m", receipts.line1.k! - receipts.line1.l! === receipts.line1.m],
        [
          "1m+2e+other=13",
          receipts.line1.m! + receipts.line2eCents + receipts.otherCashCents ===
            receipts.line13CashCents,
        ],
        ["13=cover(b)", receipts.line13CashCents === cover.receiptsPeriodCents],
        ["16=cover(c)", parsed.line16CashCents === cover.disbursementsPeriodCents],
        [
          "(a)+(b)-(c)=(d)",
          cover.beginCents + cover.receiptsPeriodCents - cover.disbursementsPeriodCents ===
            cover.closeCents,
        ],
      ];
      equationReports += 1;
      equationCommitteesParsed.add(copId);
      for (const [label, ok] of checks) {
        if (!ok) equationFailures.push(`${copId} ${report.reportName}: ${label}`);
      }
      const candidateForCommittee = NOVEMBER_2026_CANDIDATES.find(
        (entry) => entry.copId === copId,
      );
      if (candidateForCommittee !== undefined && cover.officeSought !== null) {
        const districtMatch = /Council Member District (\d+)/.exec(cover.officeSought);
        const ok =
          districtMatch !== null &&
          Number(districtMatch[1]) === candidateForCommittee.district;
        coverCorroboration.push(
          `${copId} "${cover.officeSought}" vs D${candidateForCommittee.district} ${ok ? "ok" : "MISMATCH"}`,
        );
      }
      console.log(
        `  ${cover.reportName} ${cover.periodFrom}..${cover.periodTo}: (a)=${usd(cover.beginCents)} (b)=${usd(cover.receiptsPeriodCents)}${cover.receiptsCycleCents !== null ? `/${usd(cover.receiptsCycleCents)}` : ""} (c)=${usd(cover.disbursementsPeriodCents)} (d)=${usd(cover.closeCents)} checks=${checks.every(([, ok]) => ok) ? "ok" : "FAIL"}`,
      );

      if (copId === "CAN-25-4" && /Q1/.test(cover.reportName) && /2026/.test(cover.periodFrom)) {
        hermesFixturePass =
          cover.receiptsPeriodCents === 7_262_100 &&
          cover.receiptsCycleCents === 31_613_910 &&
          receipts.line1.k === 7_362_100 &&
          receipts.line1.l === 100_000 &&
          receipts.line1.m === 7_262_100 &&
          cover.closeCents === 23_109_551 &&
          cover.officeSought === "Council Member District 4";
      }

      // Occupation reconciliation on reports that carry itemized schedules.
      if (parsed.a1aEntries.length > 0 || parsed.a1cEntries.length > 0) {
        scheduleCommittees.add(copId);
        const a1aSum = parsed.a1aEntries.reduce((sum, entry) => sum + entry.amountCents, 0);
        const a1cSum = parsed.a1cEntries.reduce((sum, entry) => sum + entry.amountCents, 0);
        const a1aOk = a1aSum === receipts.line1.a;
        const a1cOk = a1cSum === receipts.line1.c;
        const withOccupation = [...parsed.a1aEntries, ...parsed.a1cEntries].filter(
          (entry) => entry.occupation !== null && entry.employer !== null,
        ).length;
        if (!a1aOk || !a1cOk) occupationPass = false;
        occupationDetail.push(
          `${copId} ${cover.reportName}: A1a ${parsed.a1aEntries.length} rows ${usd(a1aSum)} vs 1(a) ${usd(receipts.line1.a!)} ${a1aOk ? "ok" : "FAIL"}; A1c ${parsed.a1cEntries.length} rows ${usd(a1cSum)} vs 1(c) ${usd(receipts.line1.c!)} ${a1cOk ? "ok" : "FAIL"}; occ+emp on ${withOccupation}`,
        );
      }
    }
    // Chain continuity within the committee, ordered by reporting period
    // (grids expose one package per period — the amended one — so periods
    // are unique). Two source behaviors are expected, typed, and non-fatal:
    //   - the cycle-to-date column RESETS at the Phoenix cycle boundary for
    //     committees that keep one COP ID across cycles, and
    //   - an amendment to an earlier report strands the cycle column of any
    //     report filed in between (verified live: CAN-22-6's Q1-2026 cycle
    //     value matches the PRE-amendment annual exactly). Phase 3 therefore
    //     sums period values and treats the cycle column as a cross-check.
    // A broken (a)=prior(d) cash chain is a filer error; it gate-fails only
    // inside the current cycle (the module's aggregation window).
    parsedReports.sort(
      (a, b) => coverDate(a.cover.periodFrom).getTime() - coverDate(b.cover.periodFrom).getTime(),
    );
    for (let index = 1; index < parsedReports.length; index += 1) {
      const previous = parsedReports[index - 1]!.cover;
      const current = parsedReports[index]!.cover;
      if (coverDate(current.periodFrom) <= coverDate(previous.periodTo)) {
        periodOverlapFailures.push(
          `${copId}: ${current.reportName} (${current.periodFrom}) overlaps ${previous.reportName} (..${previous.periodTo})`,
        );
      }
      if (current.beginCents !== previous.closeCents) {
        const detail = `${copId} violation cash_chain_break: (a) of ${current.reportName} ${usd(current.beginCents)} != prior (d) ${usd(previous.closeCents)}`;
        console.log(`  ${detail}`);
        if (cycleStartYear(coverDate(current.periodFrom)) >= 2025) {
          equationFailures.push(detail);
        }
      }
      const sameCycle =
        cycleStartYear(coverDate(current.periodFrom)) ===
        cycleStartYear(coverDate(previous.periodFrom));
      if (
        sameCycle &&
        previous.receiptsCycleCents !== null &&
        current.receiptsCycleCents !== null &&
        current.receiptsCycleCents !== previous.receiptsCycleCents + current.receiptsPeriodCents
      ) {
        console.log(
          `  ${copId} violation cycle_column_discrepancy: (b) cycle of ${current.reportName} is ${usd(current.receiptsCycleCents)}, prior cycle + period = ${usd(previous.receiptsCycleCents + current.receiptsPeriodCents)} (expected when an earlier report was amended)`,
        );
      }
      if (
        sameCycle &&
        previous.disbursementsCycleCents !== null &&
        current.disbursementsCycleCents !== null &&
        current.disbursementsCycleCents !==
          previous.disbursementsCycleCents + current.disbursementsPeriodCents
      ) {
        console.log(
          `  ${copId} violation cycle_column_discrepancy: (c) cycle of ${current.reportName} is ${usd(current.disbursementsCycleCents)}, prior cycle + period = ${usd(previous.disbursementsCycleCents + current.disbursementsPeriodCents)} (expected when an earlier report was amended)`,
        );
      }
    }
  }
  gates.push({
    name: "amendment canonicalization: superseded packages dropped, periods disjoint",
    pass: periodOverlapFailures.length === 0,
    detail:
      periodOverlapFailures.length === 0
        ? `${duplicatePeriods} duplicate period group(s), ${supersededDropped} superseded package(s) dropped, parsed periods disjoint`
        : periodOverlapFailures.join("; "),
  });
  gates.push({
    name: "report equations cent-exact on every parsed report (>=3 committees)",
    pass: equationCommitteesParsed.size >= 3 && equationFailures.length === 0,
    detail:
      equationFailures.length === 0
        ? `${equationReports} report(s) parsed across ${equationCommitteesParsed.size} committee(s), all equations hold`
        : equationFailures.join("; "),
  });
  gates.push({
    name: "parsed covers corroborate the candidate's office + district",
    pass:
      coverCorroboration.length > 0 &&
      coverCorroboration.every((entry) => entry.endsWith("ok")),
    detail: [...new Set(coverCorroboration)].join("; ") || "no covers corroborated",
  });
  gates.push({
    name: "Hermes Q1-2026 hard fixture (receipts, refunds, close, office)",
    pass: hermesFixturePass,
    detail: hermesFixturePass ? "all hand-derived values reproduced" : "fixture mismatch — re-derive by hand",
  });
  gates.push({
    name: "occupation/employer extraction reconciles to lines 1(a)/1(c) (>=2 committees)",
    pass: occupationPass && scheduleCommittees.size >= 2,
    detail: occupationDetail.join(" | ") || "no itemized schedules parsed",
  });

  // --- Gate 6: outside-spending census. ---
  const ieRegistrations = (
    await gridAll("/CampaignFinance/Search/_SearchCommittees", {
      CFUNC: CANDIDATE_IE_FUNCTION_ID,
    })
  ).map(toRegistrationRow);
  const ieByCopId = new Map<string, RegistrationRow[]>();
  for (const row of ieRegistrations) {
    const bucket = ieByCopId.get(row.copId) ?? [];
    bucket.push(row);
    ieByCopId.set(row.copId, bucket);
  }
  let ieActive = 0;
  let ieStanding = 0;
  let ieTestExcluded = 0;
  const ieCityFilers: {
    copId: string;
    name: string;
    expenditureRows: number;
    currentCycleRows: number;
    b6Package: string | null;
  }[] = [];
  for (const [copId, rows] of ieByCopId) {
    const canonical = canonicalRegistration(rows);
    if (canonical === null) continue;
    if (TEST_COMMITTEE_PATTERN.test(canonical.committeeName)) {
      ieTestExcluded += 1;
      continue;
    }
    if (canonical.terminated) continue;
    ieActive += 1;
    if (canonical.isStandingCommittee) {
      ieStanding += 1;
      continue; // standing PACs file finance reports only with the AZ SOS
    }
    const expenditureRows = await gridAll("/CampaignFinance/Search/_SearchExpenditures", {
      COPID: copId,
    });
    const currentCycle = await gridPage(
      "/CampaignFinance/Search/_SearchExpenditures",
      { COPID: copId, DLOW: "04/01/2025" },
      1,
      1,
    );
    const b6Row = expenditureRows.find((row) =>
      /B\(6\)/.test(String(row.ReportScheduleName ?? "")),
    );
    ieCityFilers.push({
      copId,
      name: canonical.committeeName,
      expenditureRows: expenditureRows.length,
      currentCycleRows: currentCycle.Total,
      b6Package: b6Row ? String(b6Row.ReportPackageId) : null,
    });
  }

  // Pin the IE itemization schedule (B(6)) on a real filing: the page must
  // carry the supported/opposed candidate blocks and an Office Sought field.
  // Live finding (PAC-22-14, 2024 Q3): the candidate NAME cell can be BLANK
  // while the % and office fields are filled — Phase 3 target matching must
  // fail closed on empty names, never infer.
  let ieSchedulePinned = false;
  let ieScheduleDetail = "no city-filing IE PAC exposes a Schedule B(6) filing";
  const b6Filer = ieCityFilers.find((filer) => filer.b6Package !== null);
  if (b6Filer !== undefined) {
    const pages = await extractPdfPages(await fetchReportPdf(b6Filer.b6Package!));
    const b6Page = pages.find((page) =>
      page.lines.some((line) => /INDEPENDENT EXPENDITURES MADE: SCHEDULE B\(6\)/.test(line.text)),
    );
    if (b6Page !== undefined) {
      const text = b6Page.lines.map((line) => line.text).join("\n");
      const hasSupported = /Candidate\(s\) Supported \(including % Supported\)/.test(text);
      const hasOpposed = /Candidate\(s\) Opposed \(including % opposed\)/i.test(text);
      const officeLine = b6Page.lines.find((line) => /Election Month\/Year Office Sought/.test(line.text));
      const officeIndex = officeLine === undefined ? -1 : b6Page.lines.indexOf(officeLine);
      const officeValues = officeIndex >= 0 ? b6Page.lines[officeIndex + 1]?.text ?? "" : "";
      ieSchedulePinned = hasSupported && hasOpposed && officeValues.length > 0;
      ieScheduleDetail = `B(6) pinned on ${b6Filer.copId} package ${b6Filer.b6Package}: supported/opposed blocks=${hasSupported && hasOpposed}, election+office values "${officeValues}"`;
      const supportedIndex = b6Page.lines.findIndex((line) =>
        /Candidate\(s\) Supported/.test(line.text),
      );
      if (supportedIndex >= 0) {
        console.log(
          `  B(6) raw candidate cells: "${b6Page.lines[supportedIndex + 1]?.text ?? "(blank)"}" — blank names occur live; fail closed`,
        );
      }
    }
  }
  console.log("\noutside census (candidate-IE authorized PACs):");
  console.log(
    `  registrations=${ieByCopId.size} distinct committees; active=${ieActive}; standing (SOS-filing)=${ieStanding}; test-excluded=${ieTestExcluded}`,
  );
  for (const filer of ieCityFilers.sort((a, b) => b.expenditureRows - a.expenditureRows)) {
    console.log(
      `  city-filing ${filer.copId} "${filer.name}": ${filer.expenditureRows} expenditure rows (${filer.currentCycleRows} in the 2025-2027 cycle)${filer.b6Package ? ` B(6) in ${filer.b6Package}` : ""}`,
    );
  }
  console.log(`  ${ieScheduleDetail}`);
  console.log(
    "  unmeasured channels (publish NULL, never zero, until measured): standing-PAC Spotlight exposure UNVERIFIED (required before the Phase 3 outside leg); IE-entity fillable reports; Election Funding Disclosure (dark money) filings",
  );
  gates.push({
    name: "outside census: channel split + IE schedule format pinned on a real filing",
    pass:
      ieByCopId.size > 0 &&
      ieActive >= 1 &&
      ieStanding >= 1 &&
      ieTestExcluded >= 1 &&
      ieCityFilers.length >= 1 &&
      ieSchedulePinned,
    detail: `${ieByCopId.size} committees (${ieActive} active, ${ieStanding} standing, ${ieTestExcluded} test-excluded), ${ieCityFilers.length} city-filing; ${ieScheduleDetail}`,
  });

  // --- Summary. ---
  console.log("\n=== Phase 0 gates ===");
  let failures = 0;
  for (const gate of gates) {
    const status = gate.pass ? "PASS" : "FAIL";
    if (!gate.pass) failures += 1;
    console.log(`${status}  ${gate.name} — ${gate.detail}`);
  }
  if (failures > 0) {
    process.exitCode = 1;
    console.log(`\n${failures} gate(s) failed`);
  } else {
    console.log("\nall gates passed");
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Phoenix candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}

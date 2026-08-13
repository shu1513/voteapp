// Positioned-text parsing of Phoenix "COMMITTEE CAMPAIGN FINANCE REPORT"
// PDFs (plan Phase 3), promoted from the Phase 0 probe where every pattern
// below was verified cent-exact against 21 live reports (2026-08-12). The
// Houston precedent applies: pdfjs `getTextContent()` items are grouped into
// lines by y and ordered by x — naive whole-page extraction scrambles this
// form's label/value order.
//
// Parsed surfaces:
//   - Cover FINANCIAL SUMMARY: (a) begin + (b) receipts period/cycle +
//     (c) disbursements period/cycle + (d) close, reporting period, report
//     name, and the machine-readable "Office Sought" (resolver corroboration).
//   - SUMMARY OF RECEIPTS (Schedule A): lines 1(a)..1(m), 2(e), the other
//     cash receipts (3, 4, 8, 9, 11, 12), and line 13 — the cash column only
//     (equity/in-kind prints in a second column to the right).
//   - SUMMARY OF DISBURSEMENTS (Schedule B): line 16 cash, and line 6
//     (independent expenditures made) for the outside-leg cross-check.
//   - Schedule A(1)(a) / A(1)(c) itemized rows: amount, date, name,
//     occupation, employer (the occupation/employer source — grids carry
//     neither).
//   - Schedule B(6) independent-expenditure entries (PAC reports): amounts,
//     candidate-supported/opposed blocks, election month/year, office sought.
//     LIVE DIRT (PAC-22-14 package d7118529): the candidate NAME cell can be
//     BLANK while the % and office fields are filled — the outside aggregator
//     fails closed on empty names, never infers.
//
// Every function here is pure over the positioned-text page model so tests
// run on synthetic pages; only extractPhoenixPdfPages touches pdfjs. Raw
// PDFs are never committed (PII policy) — fixtures are hand-derived values.

export type PhoenixPdfCell = { text: string; x: number };
export type PhoenixPdfLine = { y: number; cells: PhoenixPdfCell[]; text: string };
export type PhoenixPdfPage = { pageNumber: number; lines: PhoenixPdfLine[] };

/** pdfjs rejects Node Buffers — hand this a plain Uint8Array view. */
export async function extractPhoenixPdfPages(
  data: Uint8Array,
): Promise<PhoenixPdfPage[]> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const loadingTask = getDocument({ data });
  const pdf = await loadingTask.promise;
  try {
    const pages: PhoenixPdfPage[] = [];
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      const groups: { y: number; cells: PhoenixPdfCell[] }[] = [];
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
            return {
              y: group.y,
              cells,
              text: cells.map((cell) => cell.text).join(" "),
            };
          }),
      });
    }
    return pages;
  } finally {
    await loadingTask.destroy();
  }
}

/** "$1,234.56" / "($1,234.56)" → signed integer cents (parenthesised amounts
 * are negatives on this form family). */
export function phoenixMoneyTokenToCents(token: string): number {
  const match = /^\(?\$([\d,]+)\.(\d{2})\)?$/.exec(token.trim());
  if (match === null) throw new Error(`Not a money token: "${token}"`);
  const cents = Number(match[1]!.replaceAll(",", "")) * 100 + Number(match[2]);
  return token.trim().startsWith("(") ? -cents : cents;
}

function moneyCells(line: PhoenixPdfLine): { cents: number; x: number }[] {
  return line.cells
    .filter((cell) => /^\(?\$[\d,]+\.\d{2}\)?$/.test(cell.text))
    .map((cell) => ({ cents: phoenixMoneyTokenToCents(cell.text), x: cell.x }));
}

/** Cover "Month DD, YYYY" → ISO date. Throws on anything unparseable. */
export function phoenixCoverDateToIso(text: string): string {
  const parsed = new Date(`${text} UTC`);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Unparseable cover date: "${text}"`);
  }
  return parsed.toISOString().slice(0, 10);
}

export type PhoenixReportCover = {
  reportName: string;
  /** ISO dates. */
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

export type PhoenixReceiptsSummary = {
  /** Keys a..m — Schedule A line 1 sub-items, cash cents. */
  line1: Record<string, number>;
  line2eCents: number;
  /** Lines 3, 4, 8, 9, 11, 12 (cash column). */
  otherCashCents: number;
  line13CashCents: number;
};

export type PhoenixScheduleEntry = {
  amountCents: number;
  /** MM/DD/YYYY as printed. */
  date: string;
  name: string;
  occupation: string | null;
  employer: string | null;
};

export type PhoenixParsedReport = {
  cover: PhoenixReportCover;
  /** Filed as no-activity: the cover page carries the "no financial
   * activity" checkbox and IS the whole filing ("only this cover page need
   * be filed" — live on CAN-22-10's post-election reports). The receipts
   * and disbursements values below are then all zero, taken from the cover
   * rather than absent summary pages. */
  noActivity: boolean;
  receipts: PhoenixReceiptsSummary;
  line16CashCents: number;
  /** Schedule B line 6 (independent expenditures made), cash; null when the
   * summary page lacks the line (format drift → treated as unparsed). */
  line6CashCents: number | null;
  a1aEntries: PhoenixScheduleEntry[];
  a1cEntries: PhoenixScheduleEntry[];
  b6Entries: PhoenixB6Entry[];
};

function requireLine(
  page: PhoenixPdfPage,
  pattern: RegExp,
  label: string,
): PhoenixPdfLine {
  const line = page.lines.find((candidate) => pattern.test(candidate.text));
  if (line === undefined) {
    throw new Error(`Report page ${page.pageNumber} is missing ${label}`);
  }
  return line;
}

export function parsePhoenixReportCover(page: PhoenixPdfPage): PhoenixReportCover {
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
  const nearbyMoney = (
    startIndex: number,
    span: number,
  ): { cents: number; x: number }[] => {
    const cells: { cents: number; x: number }[] = [];
    for (
      let index = startIndex;
      index < Math.min(startIndex + span, page.lines.length);
      index += 1
    ) {
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
    throw new Error(
      `Cover (c) expected 1-2 amounts, got ${disbursementsCells.length}`,
    );
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
    periodFrom: phoenixCoverDateToIso(periodMatch[2]!),
    periodTo: phoenixCoverDateToIso(periodMatch[3]!),
    officeSought: officeMatch?.[1]?.trim() || null,
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

function cashAmount(line: PhoenixPdfLine, label: string): number {
  const cells = moneyCells(line).filter((cell) => cell.x < SUMMARY_CASH_MAX_X);
  if (cells.length !== 1) {
    throw new Error(
      `${label} expected one cash amount, got ${cells.length} ("${line.text}")`,
    );
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

export function parsePhoenixReceiptsSummary(
  page: PhoenixPdfPage,
): PhoenixReceiptsSummary {
  const line1: Record<string, number> = {};
  for (const [key, pattern] of Object.entries(LINE1_LABELS)) {
    line1[key] = cashAmount(
      requireLine(page, pattern, `receipts 1(${key})`),
      `1(${key})`,
    );
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

export function parsePhoenixScheduleEntries(
  page: PhoenixPdfPage,
): PhoenixScheduleEntry[] {
  const entries: PhoenixScheduleEntry[] = [];
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
        const dateCell = detail.cells.find((cell) =>
          /^\d{2}\/\d{2}\/\d{4}$/.test(cell.text),
        );
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

// ---------------------------------------------------------------------------
// Schedule B(6) — independent expenditures made (PAC reports).
// Layout pinned on the live PAC-22-14 filing (package d7118529, 2026-08-12):
//   Recipient Name | Mode of Advertising            <- entry anchor line
//   $period(x≈369) $cumPeriod(x≈441) $cumCycle(x≈510)
//   <recipient/address lines>
//   Candidate(s) Supported (including % Supported) | Candidate(s) Opposed...
//   <value cells: supported block x<194, opposed block x≥194>
//   Election Month/Year Office Sought
//   <values: election text 190≤x<255, office text x≥255>
// ---------------------------------------------------------------------------

export type PhoenixB6Entry = {
  /** Expenditure amount this reporting period (leftmost money column). */
  amountCents: number;
  /** Non-numeric cells of the "Candidate(s) Supported" block — the disclosed
   * candidate name(s); [] when the filer left the cell blank (observed live). */
  supportedNames: string[];
  /** Numeric cells of the supported block (the disclosed % values). */
  supportedPercents: number[];
  opposedNames: string[];
  opposedPercents: number[];
  /** "Election Month/Year" value text ("2024", "11/2024"); null when blank. */
  electionText: string | null;
  /** "Office Sought" value text ("City Council" — no district on the pinned
   * live filing); null when blank. */
  officeText: string | null;
};

const B6_ENTRY_ANCHOR = /^Recipient Name/;
const B6_ENTRY_END = /^Enter total only if/;
// Money-column boundaries (see layout sketch above).
const B6_PERIOD_AMOUNT_MAX_X = 430;
// Supported block spans from the left margin to the opposed header's x.
const B6_OPPOSED_MIN_X = 194;
const B6_BLOCK_MAX_X = 350;
const B6_ELECTION_MIN_X = 190;
const B6_OFFICE_MIN_X = 255;

function isPercentCell(text: string): boolean {
  return /^\d{1,3}(?:\.\d+)?%?$/.test(text);
}

/** Parses every Schedule B(6) entry on one page. Throws when an entry's
 * period amount cannot be located — a silent skip would under-count a
 * committee's disclosed independent expenditures. */
export function parsePhoenixB6Entries(page: PhoenixPdfPage): PhoenixB6Entry[] {
  const lines = page.lines;
  const anchors: number[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (B6_ENTRY_ANCHOR.test(lines[index]!.text)) anchors.push(index);
  }
  const entries: PhoenixB6Entry[] = [];
  for (let anchorIndex = 0; anchorIndex < anchors.length; anchorIndex += 1) {
    const start = anchors[anchorIndex]!;
    let end = lines.length;
    for (let index = start + 1; index < lines.length; index += 1) {
      if (B6_ENTRY_ANCHOR.test(lines[index]!.text) || B6_ENTRY_END.test(lines[index]!.text)) {
        end = index;
        break;
      }
    }
    const span = lines.slice(start, end);

    let amountCents: number | null = null;
    for (const line of span) {
      const cells = moneyCells(line).filter(
        (cell) => cell.x < B6_PERIOD_AMOUNT_MAX_X,
      );
      if (cells.length > 0) {
        amountCents = cells.sort((a, b) => a.x - b.x)[0]!.cents;
        break;
      }
    }
    if (amountCents === null) {
      throw new Error(
        `Schedule B(6) entry ${anchorIndex + 1} on page ${page.pageNumber} has no period amount`,
      );
    }

    const supportedNames: string[] = [];
    const supportedPercents: number[] = [];
    const opposedNames: string[] = [];
    const opposedPercents: number[] = [];
    const headerIndex = span.findIndex((line) =>
      /Candidate\(s\) Supported/.test(line.text),
    );
    if (headerIndex >= 0) {
      for (let index = headerIndex + 1; index < span.length; index += 1) {
        const line = span[index]!;
        if (
          /Date of First Publication|Election Month\/Year/.test(line.text)
        ) {
          break;
        }
        for (const cell of line.cells) {
          if (cell.x >= B6_BLOCK_MAX_X) continue;
          const block =
            cell.x < B6_OPPOSED_MIN_X
              ? { names: supportedNames, percents: supportedPercents }
              : { names: opposedNames, percents: opposedPercents };
          if (isPercentCell(cell.text)) {
            block.percents.push(Number(cell.text.replace("%", "")));
          } else {
            block.names.push(cell.text);
          }
        }
      }
    }

    let electionText: string | null = null;
    let officeText: string | null = null;
    const electionHeaderIndex = span.findIndex((line) =>
      /Election Month\/Year/.test(line.text),
    );
    if (electionHeaderIndex >= 0) {
      for (let index = electionHeaderIndex + 1; index < span.length; index += 1) {
        const line = span[index]!;
        const electionCells = line.cells.filter(
          (cell) => cell.x >= B6_ELECTION_MIN_X && cell.x < B6_OFFICE_MIN_X,
        );
        const officeCells = line.cells.filter((cell) => cell.x >= B6_OFFICE_MIN_X);
        if (electionCells.length === 0 && officeCells.length === 0) continue;
        electionText = electionCells.map((cell) => cell.text).join(" ") || null;
        officeText = officeCells.map((cell) => cell.text).join(" ") || null;
        break;
      }
    }

    entries.push({
      amountCents,
      supportedNames,
      supportedPercents,
      opposedNames,
      opposedPercents,
      electionText,
      officeText,
    });
  }
  return entries;
}

/** True when the page carries the B(6) schedule heading. */
export function isPhoenixB6Page(page: PhoenixPdfPage): boolean {
  return page.lines.some((line) =>
    /INDEPENDENT EXPENDITURES MADE: SCHEDULE B\(6\)/.test(line.text),
  );
}

/**
 * Parses a full report from its positioned-text pages. Candidate reports
 * carry every surface; PAC reports parse the same way (their covers have no
 * Office Sought and their A(1) schedules are usually empty).
 */
export function parsePhoenixReportPages(
  pages: readonly PhoenixPdfPage[],
): PhoenixParsedReport {
  const coverPage = pages.find((page) =>
    page.lines.some((line) => /FINANCIAL SUMMARY \(required\)/.test(line.text)),
  );
  if (coverPage === undefined) {
    throw new Error("Report has no FINANCIAL SUMMARY cover page");
  }
  const receiptsPage = pages.find((page) =>
    page.lines.some((line) => /^SUMMARY OF RECEIPTS \(Schedule A\)/.test(line.text)),
  );
  // A cover-only filing is the form's own no-activity path ("only this
  // cover page need be filed"), verified live on CAN-22-10. Its cover
  // reports $0.00 receipts and disbursements for the period, so the summary
  // values are read from the cover and every schedule is empty. A report
  // that is missing summary pages WITHOUT being cover-only is malformed and
  // still throws.
  if (receiptsPage === undefined) {
    const cover = parsePhoenixReportCover(coverPage);
    if (
      pages.length !== 1 ||
      cover.receiptsPeriodCents !== 0 ||
      cover.disbursementsPeriodCents !== 0
    ) {
      throw new Error("Report has no SUMMARY OF RECEIPTS page");
    }
    return {
      cover,
      noActivity: true,
      receipts: {
        line1: { a: 0, b: 0, c: 0, d: 0, e: 0, f: 0, g: 0, h: 0, i: 0, j: 0, k: 0, l: 0, m: 0 },
        line2eCents: 0,
        otherCashCents: 0,
        line13CashCents: 0,
      },
      line16CashCents: 0,
      line6CashCents: null,
      a1aEntries: [],
      a1cEntries: [],
      b6Entries: [],
    };
  }
  const disbursementsPage = pages.find((page) =>
    page.lines.some((line) =>
      /^SUMMARY OF DISBURSEMENTS \(Schedule B\)/.test(line.text),
    ),
  );
  if (disbursementsPage === undefined) {
    throw new Error("Report has no SUMMARY OF DISBURSEMENTS page");
  }
  const line16 = requireLine(
    disbursementsPage,
    /^16\. Total Disbursements/,
    "disbursements line 16",
  );
  // Line 6 ("Independent Expenditures Made") powers the outside-leg
  // cross-check; unlike line 16 it is read leniently — a missing line yields
  // null instead of failing the whole (direct-leg) parse.
  const line6 = disbursementsPage.lines.find((line) =>
    /^6\. Independent Expenditures Made/.test(line.text),
  );

  const a1aEntries: PhoenixScheduleEntry[] = [];
  const a1cEntries: PhoenixScheduleEntry[] = [];
  const b6Entries: PhoenixB6Entry[] = [];
  for (const page of pages) {
    const heading = page.lines.slice(0, 12).map((line) => line.text).join(" ");
    if (
      /MONETARY CONTRIBUTIONS RECEIVED FROM IN-STATE INDIVIDUALS - MORE THAN/.test(
        heading,
      )
    ) {
      a1aEntries.push(...parsePhoenixScheduleEntries(page));
    } else if (
      /MONETARY CONTRIBUTIONS RECEIVED FROM OUT-OF-STATE INDIVIDUALS/.test(heading)
    ) {
      a1cEntries.push(...parsePhoenixScheduleEntries(page));
    }
    if (isPhoenixB6Page(page)) {
      b6Entries.push(...parsePhoenixB6Entries(page));
    }
  }

  return {
    cover: parsePhoenixReportCover(coverPage),
    noActivity: false,
    receipts: parsePhoenixReceiptsSummary(receiptsPage),
    line16CashCents: cashAmount(line16, "disbursements 16"),
    line6CashCents: line6 ? cashAmount(line6, "disbursements 6") : null,
    a1aEntries,
    a1cEntries,
    b6Entries,
  };
}

/** Bytes → parsed report (the only pdfjs-touching composition). */
export async function parsePhoenixReport(
  bytes: Uint8Array,
): Promise<PhoenixParsedReport> {
  return parsePhoenixReportPages(await extractPhoenixPdfPages(bytes));
}

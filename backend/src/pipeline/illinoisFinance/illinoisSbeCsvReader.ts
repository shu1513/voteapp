export const ILLINOIS_SBE_EXPORT_ROW_CAP = 25_000;

const ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL =
  "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx";
const ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL =
  "https://www.elections.il.gov/CampaignDisclosure/ExpenditureSearchByAllExpenditures.aspx";

export type IllinoisSbeSupportOppose = "support" | "oppose";

export type IllinoisSbeCsvRow = Record<string, string>;

export type IllinoisSbeContributionRecord = {
  contributorName: string | null;
  contributorAddress: string | null;
  occupation: string | null;
  employer: string | null;
  amount: number;
  receivedDate: string | null;
  reportReceivedDate: string | null;
  contributionType: string | null;
  recipientCommitteeName: string | null;
  description: string | null;
  vendorName: string | null;
  vendorAddress: string | null;
  sourceUrl: string;
};

export type IllinoisSbeExpenditureRecord = {
  payeeName: string | null;
  payeeAddress: string | null;
  amount: number;
  expendedDate: string | null;
  reportReceivedDate: string | null;
  expenditureType: string | null;
  expendingCommitteeName: string | null;
  purpose: string | null;
  candidateName: string | null;
  officeDistrict: string | null;
  supportOppose: IllinoisSbeSupportOppose | null;
  sourceUrl: string;
};

export type IllinoisSbeExportCapStatus = {
  rowCount: number;
  cap: number;
  capped: boolean;
  warningTextPresent: boolean;
  reason: "row_count_reached_cap" | "warning_text_present" | null;
};

export type IllinoisSbePartitionDateWindow = {
  fromDate: string;
  toDate: string;
};

export type IllinoisSbePartitionAmountWindow = {
  minAmount: number;
  maxAmount: number;
};

export type IllinoisSbeExportPartitionWindow = {
  fromDate?: string;
  toDate?: string;
  minAmount?: number;
  maxAmount?: number;
};

export type IllinoisSbeExportPartitionPlan = {
  status: IllinoisSbeExportCapStatus;
  strategy: "date" | "amount" | null;
  partitions: [IllinoisSbeExportPartitionWindow, IllinoisSbeExportPartitionWindow] | null;
};

function normalizeHeader(value: string): string {
  return value
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === "\"" && next === "\"") {
        field += "\"";
        index += 1;
      } else if (char === "\"") {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === "\"") {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if (char === "\r") {
      continue;
    }
    field += char;
  }

  if (inQuotes) {
    throw new Error("Illinois SBE CSV has an unterminated quoted field");
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((fields) => fields.some((fieldValue) => fieldValue.trim().length > 0));
}

export function parseIllinoisSbeCsvRows(csv: string): IllinoisSbeCsvRow[] {
  const rows = parseCsvRows(csv);
  const headerRow = rows[0];
  if (!headerRow) {
    return [];
  }
  const headers = headerRow.map(normalizeHeader);
  return rows.slice(1).map((fields) => {
    const row: IllinoisSbeCsvRow = {};
    for (let index = 0; index < headers.length; index += 1) {
      const header = headers[index];
      if (header) {
        row[header] = fields[index]?.trim() ?? "";
      }
    }
    return row;
  });
}

function getString(row: IllinoisSbeCsvRow, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = row[normalizeHeader(key)] ?? row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim().replace(/\u00a0/g, " ");
    }
  }
  return null;
}

function parseAmount(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const isParenthesized = value.includes("(") && value.includes(")");
  const parsed = Number(value.replace(/[$,()]/g, "").trim());
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return isParenthesized ? -parsed : parsed;
}

function splitMultilineCell(value: string | null): string[] {
  return (value ?? "")
    .split(/\r?\n/)
    .map((line) => line.trim().replace(/\u00a0/g, " "))
    .filter(Boolean);
}

function parseNameAddressOccupationEmployer(value: string | null): {
  name: string | null;
  address: string | null;
  occupation: string | null;
  employer: string | null;
} {
  const lines = splitMultilineCell(value);
  let occupation: string | null = null;
  let employer: string | null = null;
  const identityLines: string[] = [];

  for (const line of lines) {
    const occupationMatch = /^Occupation:\s*(.+)$/i.exec(line);
    if (occupationMatch?.[1]) {
      occupation = occupationMatch[1].trim();
      continue;
    }
    const employerMatch = /^Employer:\s*(.+)$/i.exec(line);
    if (employerMatch?.[1]) {
      employer = employerMatch[1].trim();
      continue;
    }
    identityLines.push(line);
  }

  return {
    name: identityLines[0] ?? null,
    address: identityLines.slice(1).join("\n") || null,
    occupation,
    employer,
  };
}

function parseTypeAndCommittee(value: string | null): { type: string | null; committeeName: string | null } {
  const lines = splitMultilineCell(value);
  return {
    type: lines[0] ?? null,
    committeeName: lines.slice(1).join(" ") || null,
  };
}

function parseSupportOppose(value: string | null): IllinoisSbeSupportOppose | null {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "supporting" || normalized === "support") {
    return "support";
  }
  if (normalized === "opposing" || normalized === "oppose") {
    return "oppose";
  }
  return null;
}

export function illinoisSbeContributionRecordFromRow(
  row: IllinoisSbeCsvRow,
  sourceUrl = ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL
): IllinoisSbeContributionRecord | null {
  const amount = parseAmount(getString(row, "Amount"));
  if (amount === null) {
    return null;
  }
  const contributor = parseNameAddressOccupationEmployer(getString(row, "Contributed By"));
  const recipient = parseTypeAndCommittee(getString(row, "Received By"));
  return {
    contributorName: contributor.name,
    contributorAddress: contributor.address,
    occupation: contributor.occupation,
    employer: contributor.employer,
    amount,
    receivedDate: getString(row, "Amount Received Date", "Contribution Received Date", "Received Date"),
    reportReceivedDate: getString(row, "Report Received Date"),
    contributionType: recipient.type,
    recipientCommitteeName: recipient.committeeName,
    description: getString(row, "Description"),
    vendorName: getString(row, "Vendor Name"),
    vendorAddress: getString(row, "Vendor Address"),
    sourceUrl,
  };
}

export function illinoisSbeExpenditureRecordFromRow(
  row: IllinoisSbeCsvRow,
  sourceUrl = ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL
): IllinoisSbeExpenditureRecord | null {
  const amount = parseAmount(getString(row, "Amount"));
  if (amount === null) {
    return null;
  }
  const payee = parseNameAddressOccupationEmployer(getString(row, "Received By"));
  const spender = parseTypeAndCommittee(getString(row, "Expended By"));
  return {
    payeeName: payee.name,
    payeeAddress: payee.address,
    amount,
    expendedDate: getString(row, "Expended By Date", "Date Expended"),
    reportReceivedDate: getString(row, "Report Received Date"),
    expenditureType: spender.type,
    expendingCommitteeName: spender.committeeName,
    purpose: getString(row, "Purpose / Beneficiary", "Purpose", "Beneficiary"),
    candidateName: getString(row, "Candidate Name"),
    officeDistrict: getString(row, "Office - District", "Office Being Sought"),
    supportOppose: parseSupportOppose(getString(row, "Supporting / Opposing")),
    sourceUrl,
  };
}

export function parseIllinoisSbeContributionRecordsCsv(
  csv: string,
  sourceUrl = ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL
): IllinoisSbeContributionRecord[] {
  return parseIllinoisSbeCsvRows(csv)
    .map((row) => illinoisSbeContributionRecordFromRow(row, sourceUrl))
    .filter((row): row is IllinoisSbeContributionRecord => row !== null);
}

export function parseIllinoisSbeExpenditureRecordsCsv(
  csv: string,
  sourceUrl = ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL
): IllinoisSbeExpenditureRecord[] {
  return parseIllinoisSbeCsvRows(csv)
    .map((row) => illinoisSbeExpenditureRecordFromRow(row, sourceUrl))
    .filter((row): row is IllinoisSbeExpenditureRecord => row !== null);
}

export function hasIllinoisSbeExportCapWarning(text: string): boolean {
  return /maximum\s+number\s+of\s+records\s+available\s+for\s+download\s+is\s+25,?000/i.test(text);
}

export function getIllinoisSbeExportCapStatus(input: {
  csvRowCount: number;
  resultText?: string | null;
  cap?: number;
}): IllinoisSbeExportCapStatus {
  const cap = input.cap ?? ILLINOIS_SBE_EXPORT_ROW_CAP;
  const warningTextPresent = input.resultText ? hasIllinoisSbeExportCapWarning(input.resultText) : false;
  const rowCountReachedCap = input.csvRowCount >= cap;
  return {
    rowCount: input.csvRowCount,
    cap,
    capped: rowCountReachedCap || warningTextPresent,
    warningTextPresent,
    reason: rowCountReachedCap ? "row_count_reached_cap" : warningTextPresent ? "warning_text_present" : null,
  };
}

function parseIllinoisDate(value: string): Date | null {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value.trim());
  if (!match) {
    return null;
  }
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) {
    return null;
  }
  return date;
}

function formatIllinoisDate(date: Date): string {
  return `${date.getUTCMonth() + 1}/${date.getUTCDate()}/${date.getUTCFullYear()}`;
}

function addUtcDays(date: Date, days: number): Date {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

export function splitIllinoisSbeDateWindow(
  window: IllinoisSbePartitionDateWindow
): [IllinoisSbePartitionDateWindow, IllinoisSbePartitionDateWindow] | null {
  const from = parseIllinoisDate(window.fromDate);
  const to = parseIllinoisDate(window.toDate);
  if (!from || !to || from.getTime() >= to.getTime()) {
    return null;
  }

  const dayMs = 24 * 60 * 60 * 1000;
  const days = Math.floor((to.getTime() - from.getTime()) / dayMs);
  if (days < 1) {
    return null;
  }
  const midpoint = addUtcDays(from, Math.floor(days / 2));
  const secondStart = addUtcDays(midpoint, 1);
  if (secondStart.getTime() > to.getTime()) {
    return null;
  }
  return [
    { fromDate: formatIllinoisDate(from), toDate: formatIllinoisDate(midpoint) },
    { fromDate: formatIllinoisDate(secondStart), toDate: formatIllinoisDate(to) },
  ];
}

export function splitIllinoisSbeAmountWindow(
  window: IllinoisSbePartitionAmountWindow
): [IllinoisSbePartitionAmountWindow, IllinoisSbePartitionAmountWindow] | null {
  if (
    !Number.isFinite(window.minAmount) ||
    !Number.isFinite(window.maxAmount) ||
    window.minAmount < 0 ||
    window.maxAmount <= window.minAmount
  ) {
    return null;
  }
  const minCents = Math.round(window.minAmount * 100);
  const maxCents = Math.round(window.maxAmount * 100);
  if (maxCents - minCents < 1) {
    return null;
  }
  const midpointCents = Math.floor((minCents + maxCents) / 2);
  if (midpointCents < minCents || midpointCents >= maxCents) {
    return null;
  }
  return [
    { minAmount: minCents / 100, maxAmount: midpointCents / 100 },
    { minAmount: (midpointCents + 1) / 100, maxAmount: maxCents / 100 },
  ];
}

export function planIllinoisSbeExportPartitions(input: {
  csvRowCount: number;
  resultText?: string | null;
  cap?: number;
  fromDate?: string | null;
  toDate?: string | null;
  minAmount?: number | null;
  maxAmount?: number | null;
}): IllinoisSbeExportPartitionPlan {
  const status = getIllinoisSbeExportCapStatus({
    csvRowCount: input.csvRowCount,
    resultText: input.resultText,
    cap: input.cap,
  });
  if (!status.capped) {
    return { status, strategy: null, partitions: null };
  }

  if (input.fromDate && input.toDate) {
    const datePartitions = splitIllinoisSbeDateWindow({
      fromDate: input.fromDate,
      toDate: input.toDate,
    });
    if (datePartitions) {
      return {
        status,
        strategy: "date",
        partitions: datePartitions.map((partition) => ({
          ...partition,
          ...(input.minAmount !== null && input.minAmount !== undefined ? { minAmount: input.minAmount } : {}),
          ...(input.maxAmount !== null && input.maxAmount !== undefined ? { maxAmount: input.maxAmount } : {}),
        })) as [IllinoisSbeExportPartitionWindow, IllinoisSbeExportPartitionWindow],
      };
    }
  }

  if (input.minAmount !== null && input.minAmount !== undefined && input.maxAmount !== null && input.maxAmount !== undefined) {
    const amountPartitions = splitIllinoisSbeAmountWindow({
      minAmount: input.minAmount,
      maxAmount: input.maxAmount,
    });
    if (amountPartitions) {
      return {
        status,
        strategy: "amount",
        partitions: amountPartitions.map((partition) => ({
          ...(input.fromDate ? { fromDate: input.fromDate } : {}),
          ...(input.toDate ? { toDate: input.toDate } : {}),
          ...partition,
        })) as [IllinoisSbeExportPartitionWindow, IllinoisSbeExportPartitionWindow],
      };
    }
  }

  return { status, strategy: null, partitions: null };
}

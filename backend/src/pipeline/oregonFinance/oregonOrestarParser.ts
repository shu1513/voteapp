export const OREGON_ORESTAR_BASE_URL = "https://secure.sos.state.or.us";
export const OREGON_ORESTAR_TRANSACTION_SEARCH_URL =
  "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do";
export const OREGON_ORESTAR_EXPORT_PATH = "XcelCNESearch";

export type OregonOrestarSupportOppose = "support" | "oppose";

export type OregonOrestarSearchForm = {
  actionUrl: string;
  csrfToken: string;
  sessionId: string | null;
};

export type OregonOrestarTransactionSearchResultRow = {
  transactionId: string;
  transactionDate: string | null;
  status: string | null;
  filerCommitteeName: string | null;
  filerCommitteeId: string | null;
  contributorPayeeName: string | null;
  contributorPayeeOutOfState: boolean;
  subType: string | null;
  amount: number | null;
  isInKindExpenditure: boolean;
  detailUrl: string | null;
  committeeUrl: string | null;
};

export type OregonOrestarTransactionSearchResults = {
  criteriaText: string | null;
  resultCount: number | null;
  displayedResultLimit: number | null;
  visibleRowCount: number;
  hasNextPage: boolean;
  nextPageUrl: string | null;
  exportUrl: string | null;
  rows: OregonOrestarTransactionSearchResultRow[];
};

export type OregonOrestarOutsideAssociationType =
  | "independent_expenditure"
  | "in_kind_expenditure";

export type OregonOrestarOutsideAssociation = {
  associationType: OregonOrestarOutsideAssociationType;
  supportOppose: OregonOrestarSupportOppose;
  targetCommitteeName: string;
  targetCommitteeId: string | null;
  amount: number;
  rawText: string;
};

export type OregonOrestarTransactionDetail = {
  transactionId: string | null;
  transactionDate: string | null;
  transactionType: string | null;
  transactionSubType: string | null;
  filedDate: string | null;
  amount: number | null;
  aggregate: number | null;
  processStatus: string | null;
  purpose: string | null;
  filerCommitteeName: string | null;
  filerCommitteeId: string | null;
  addressBookType: string | null;
  contributorPayeeName: string | null;
  address: string | null;
  occupation: string | null;
  employerName: string | null;
  outsideAssociations: OregonOrestarOutsideAssociation[];
  sourceUrl: string | null;
};

type HtmlCell = {
  text: string;
  html: string;
};

type HtmlRow = {
  cells: HtmlCell[];
  html: string;
};

function decodeNumericHtmlEntity(match: string, code: string, radix: number): string {
  const parsed = Number.parseInt(code, radix);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 0x10ffff) {
    return match;
  }
  try {
    return String.fromCodePoint(parsed);
  } catch {
    return match;
  }
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&#(\d+);/g, (match, code: string) => decodeNumericHtmlEntity(match, code, 10))
    .replace(/&#x([0-9a-f]+);/gi, (match, code: string) => decodeNumericHtmlEntity(match, code, 16))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function collapseWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function stripHtml(value: string): string {
  return collapseWhitespace(
    decodeHtmlEntities(
      value
        .replace(/<br\s*\/?>/gi, "\n")
        .replace(/<\/(?:p|div|li|tr|h[1-6])>/gi, "\n")
        .replace(/<[^>]+>/g, " ")
    )
  );
}

function stripHtmlPreservingLines(value: string): string {
  return decodeHtmlEntities(
    value
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/(?:p|div|li|tr|h[1-6])>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
  )
    .split(/\n+/)
    .map(collapseWhitespace)
    .filter(Boolean)
    .join("\n");
}

function absoluteOrestarUrl(value: string, sourceUrl?: string | null): string | null {
  let parsed: URL;
  try {
    parsed = new URL(decodeHtmlEntities(value).trim(), sourceUrl ?? OREGON_ORESTAR_TRANSACTION_SEARCH_URL);
  } catch {
    return null;
  }
  return parsed.origin === OREGON_ORESTAR_BASE_URL ? parsed.toString() : null;
}

function firstHref(html: string, pattern?: RegExp): string | null {
  const regex = /<a\b[^>]*\bhref\s*=\s*["']([^"']+)["'][^>]*>/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(html))) {
    const href = match[1]?.trim();
    if (!href) {
      continue;
    }
    if (!pattern || pattern.test(href)) {
      return decodeHtmlEntities(href);
    }
  }
  return null;
}

function firstHrefByText(html: string, pattern: RegExp): string | null {
  const regex = /<a\b[^>]*\bhref\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(html))) {
    const href = match[1]?.trim();
    const text = stripHtml(match[2] ?? "");
    if (href && pattern.test(text)) {
      return decodeHtmlEntities(href);
    }
  }
  return null;
}

function inputValueByName(html: string, name: string): string | null {
  const regex = /<input\b[^>]*>/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(html))) {
    const input = match[0];
    const inputName = attributeValue(input, "name");
    if (inputName !== name) {
      continue;
    }
    return attributeValue(input, "value") ?? "";
  }
  return null;
}

function attributeValue(tagHtml: string, name: string): string | null {
  const regex = new RegExp(`\\b${name}\\s*=\\s*["']([^"']*)["']`, "i");
  const match = regex.exec(tagHtml);
  return match?.[1] !== undefined ? decodeHtmlEntities(match[1]) : null;
}

function parseHtmlRows(html: string): HtmlRow[] {
  const rows: HtmlRow[] = [];
  const rowRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch: RegExpExecArray | null;
  while ((rowMatch = rowRegex.exec(html))) {
    const rowHtml = rowMatch[0];
    const cellRegex = /<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi;
    const cells: HtmlCell[] = [];
    let cellMatch: RegExpExecArray | null;
    while ((cellMatch = cellRegex.exec(rowHtml))) {
      const cellHtml = cellMatch[1] ?? "";
      cells.push({
        html: cellHtml,
        text: stripHtmlPreservingLines(cellHtml),
      });
    }
    if (cells.length > 0) {
      rows.push({ cells, html: rowHtml });
    }
  }
  return rows;
}

function parseMoney(raw: string | null | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const negative = /^\(.*\)$/.test(trimmed);
  const normalized = trimmed.replace(/[($,)\s]/g, "");
  if (!/^-?\d+(?:\.\d+)?$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.round((negative ? -Math.abs(parsed) : parsed) * 100) / 100;
}

function committeeIdFromHref(href: string | null): string | null {
  if (!href) {
    return null;
  }
  const match = /[?&]cneCommitteeId=([^&#]+)/i.exec(href);
  if (!match?.[1]) {
    return null;
  }
  try {
    return decodeURIComponent(match[1]).trim() || null;
  } catch {
    return match[1].trim() || null;
  }
}

function sessionIdFromUrl(url: string): string | null {
  const match = /;JSESSIONID_ORESTAR=([^/?#]+)/i.exec(url);
  return match?.[1] ? match[1] : null;
}

function cleanContributorPayeeName(value: string | null): {
  name: string | null;
  outOfState: boolean;
  inKind: boolean;
} {
  const normalized = value?.trim().replace(/\s+/g, " ") ?? "";
  if (!normalized) {
    return { name: null, outOfState: false, inKind: false };
  }
  return {
    name: normalized.replace(/\s*\*{1,2}\s*$/g, "").trim() || null,
    outOfState: /\*\*\s*$/.test(normalized),
    inKind: /\*\s*$/.test(normalized) && !/\*\*\s*$/.test(normalized),
  };
}

function normalizeFieldLabel(value: string): string {
  return collapseWhitespace(value).replace(/\s*:$/, "").toUpperCase();
}

function fieldsFromRows(rows: readonly HtmlRow[]): Map<string, string> {
  const fields = new Map<string, string>();
  for (const row of rows) {
    const cells = row.cells.map((cell) => cell.text);
    for (let index = 0; index + 2 < cells.length; index += 1) {
      if (cells[index + 1] !== ":") {
        continue;
      }
      const key = normalizeFieldLabel(cells[index] ?? "");
      const value = cells[index + 2]?.trim() ?? "";
      if (key && !fields.has(key)) {
        fields.set(key, value);
      }
    }
  }
  return fields;
}

function firstField(fields: ReadonlyMap<string, string>, ...labels: string[]): string | null {
  for (const label of labels) {
    const value = fields.get(normalizeFieldLabel(label));
    if (value?.trim()) {
      return value.trim();
    }
  }
  return null;
}

function parseCommitteeTitle(rows: readonly HtmlRow[]): { name: string | null; id: string | null } {
  for (const row of rows) {
    const texts = row.cells.map((cell) => cell.text.trim()).filter(Boolean);
    const detailIndex = texts.findIndex((text) => /^Transaction Detail$/i.test(text));
    const candidate = detailIndex >= 0 ? texts[detailIndex + 1] : null;
    const match = candidate?.match(/^(.*?)\s*\(([^()]+)\)\s*$/);
    if (match?.[1] && match[2]) {
      return { name: match[1].trim(), id: match[2].trim() };
    }
    if (candidate) {
      return { name: candidate.trim(), id: null };
    }
  }
  return { name: null, id: null };
}

function parseCommitteeTitleText(value: string | null | undefined): { name: string | null; id: string | null } | null {
  const text = value?.trim();
  if (!text) {
    return null;
  }
  const match = text.match(/^(.*?)\s*\(([^()]+)\)\s*$/);
  if (match?.[1] && match[2]) {
    return { name: match[1].trim(), id: match[2].trim() };
  }
  return { name: text, id: null };
}

function parseCommitteeTitleFromHtml(html: string): { name: string | null; id: string | null } | null {
  const headerMatch = /<h1\b[^>]*>\s*Transaction Detail[\s\S]*?<\/h1>[\s\S]*?<div\b[^>]*\bid\s*=\s*["']header2["'][^>]*>([\s\S]*?)<\/div>/i.exec(
    html
  );
  return parseCommitteeTitleText(headerMatch?.[1] ? stripHtml(headerMatch[1]) : null);
}

export function isOregonOrestarBlockedPage(html: string): boolean {
  const text = stripHtml(html);
  return /Please Contact Us/i.test(text) && /cyber-security service/i.test(text) && /Support ID/i.test(text);
}

export function parseOregonOrestarSearchForm(
  html: string,
  sourceUrl = OREGON_ORESTAR_TRANSACTION_SEARCH_URL
): OregonOrestarSearchForm {
  const formMatch = /<form\b[^>]*name\s*=\s*["']cneSearchForm["'][^>]*>/i.exec(html);
  if (!formMatch) {
    throw new Error("ORESTAR transaction search form not found");
  }
  const action = attributeValue(formMatch[0], "action");
  if (!action) {
    throw new Error("ORESTAR transaction search form action not found");
  }
  const csrfToken = inputValueByName(html, "OWASP_CSRFTOKEN")?.trim();
  if (!csrfToken) {
    throw new Error("ORESTAR transaction search CSRF token not found");
  }
  const actionUrl = absoluteOrestarUrl(action, sourceUrl);
  if (!actionUrl) {
    throw new Error("ORESTAR transaction search form action URL is not allowed");
  }
  return {
    actionUrl,
    csrfToken,
    sessionId: sessionIdFromUrl(actionUrl),
  };
}

export function parseOregonOrestarTransactionSearchResults(
  html: string,
  sourceUrl = OREGON_ORESTAR_TRANSACTION_SEARCH_URL
): OregonOrestarTransactionSearchResults {
  const rows = parseHtmlRows(html);
  const pageText = stripHtml(html);
  const resultMatch = /Results\s*:\s*([0-9,]+)\s+records?\s+found/i.exec(pageText);
  const limitMatch = /maximum\s+([0-9,]+)\s+records?\s+are\s+displayed/i.exec(pageText);
  const exportHref = firstHref(html, /^XcelCNESearch\b/i);
  const nextHref = firstHrefByText(html, /^Next$/i);

  const parsedRows: OregonOrestarTransactionSearchResultRow[] = [];
  for (const row of rows) {
    if (row.cells.length < 7) {
      continue;
    }
    const [idCell, dateCell, statusCell, filerCell, contributorCell, subTypeCell, amountCell] = row.cells;
    const transactionId = idCell?.text.match(/\b(\d{4,})\b/)?.[1];
    if (!transactionId || /^Tran ID$/i.test(idCell.text)) {
      continue;
    }

    const detailHref = firstHref(idCell.html, /gotoPublicTransactionDetail\.do/i);
    const committeeHref = firstHref(filerCell?.html ?? "", /sooDetail\.do/i);
    const contributorPayee = cleanContributorPayeeName(contributorCell?.text ?? null);
    parsedRows.push({
      transactionId,
      transactionDate: dateCell?.text.trim() || null,
      status: statusCell?.text.trim() || null,
      filerCommitteeName: filerCell?.text.trim() || null,
      filerCommitteeId: committeeIdFromHref(committeeHref),
      contributorPayeeName: contributorPayee.name,
      contributorPayeeOutOfState: contributorPayee.outOfState,
      subType: subTypeCell?.text.trim() || null,
      amount: parseMoney(amountCell?.text),
      isInKindExpenditure: contributorPayee.inKind,
      detailUrl: detailHref ? absoluteOrestarUrl(detailHref, sourceUrl) : null,
      committeeUrl: committeeHref ? absoluteOrestarUrl(committeeHref, sourceUrl) : null,
    });
  }

  return {
    criteriaText: pageText.match(/Search Criteria\s*:\s*(.*?)\s*(?:Results\s*:|$)/i)?.[1]?.trim() || null,
    resultCount: resultMatch?.[1] ? Number.parseInt(resultMatch[1].replace(/,/g, ""), 10) : null,
    displayedResultLimit: limitMatch?.[1] ? Number.parseInt(limitMatch[1].replace(/,/g, ""), 10) : null,
    visibleRowCount: parsedRows.length,
    hasNextPage: /<input\b[^>]*\bvalue\s*=\s*["']Next["'][^>]*>/i.test(html) && !/<input\b[^>]*\bvalue\s*=\s*["']Next["'][^>]*disabled/i.test(html),
    nextPageUrl: nextHref ? absoluteOrestarUrl(nextHref, sourceUrl) : null,
    exportUrl: exportHref ? absoluteOrestarUrl(exportHref, sourceUrl) : null,
    rows: parsedRows,
  };
}

export function parseOregonOrestarOutsideAssociations(raw: string | null | undefined): OregonOrestarOutsideAssociation[] {
  const text = raw?.trim();
  if (!text) {
    return [];
  }
  const associations: OregonOrestarOutsideAssociation[] = [];
  const regex =
    /((?:Independent Expenditure\s+in\s+(?:Support|Opposition|Oppose|Against)|In-Kind Expenditure)\s*-\s*.*?\s*-\s*\$[0-9,]+(?:\.\d{2})?)/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(text))) {
    const rawAssociation = collapseWhitespace(match[1] ?? "");
    const parsed = /^(Independent Expenditure)\s+in\s+(Support|Opposition|Oppose|Against)\s*-\s*(.*?)\s*-\s*(\$[0-9,]+(?:\.\d{2})?)$/i.exec(rawAssociation) ??
      /^(In-Kind Expenditure)\s*-\s*(.*?)\s*-\s*(\$[0-9,]+(?:\.\d{2})?)$/i.exec(rawAssociation);
    if (!parsed) {
      continue;
    }

    const isIndependent = /^Independent/i.test(parsed[1] ?? "");
    const stance = isIndependent ? parsed[2] ?? "" : "Support";
    const committeeRaw = isIndependent ? parsed[3] ?? "" : parsed[2] ?? "";
    const amountRaw = isIndependent ? parsed[4] ?? "" : parsed[3] ?? "";
    const amount = parseMoney(amountRaw);
    const committeeMatch = committeeRaw.trim().match(/^(.*?)\s*\(([^()]+)\)\s*$/);
    const committeeName = (committeeMatch?.[1] ?? committeeRaw).trim();
    if (!committeeName || amount === null || amount <= 0) {
      continue;
    }
    associations.push({
      associationType: isIndependent ? "independent_expenditure" : "in_kind_expenditure",
      supportOppose: /Opposition|Oppose|Against/i.test(stance) ? "oppose" : "support",
      targetCommitteeName: committeeName,
      targetCommitteeId: committeeMatch?.[2]?.trim() || null,
      amount,
      rawText: rawAssociation,
    });
  }
  return associations;
}

export function parseOregonOrestarTransactionDetail(
  html: string,
  sourceUrl: string | null = null
): OregonOrestarTransactionDetail {
  const rows = parseHtmlRows(html);
  const fields = fieldsFromRows(rows);
  const title = parseCommitteeTitleFromHtml(html) ?? parseCommitteeTitle(rows);
  const associationsText = firstField(fields, "In-Kind/Independent Expenditures");

  return {
    transactionId: firstField(fields, "Transaction ID"),
    transactionDate: firstField(fields, "Transaction Date"),
    transactionType: firstField(fields, "Transaction Type"),
    transactionSubType: firstField(fields, "Transaction Sub Type"),
    filedDate: firstField(fields, "Filed Date"),
    amount: parseMoney(firstField(fields, "Amount")),
    aggregate: parseMoney(firstField(fields, "Aggregate")),
    processStatus: firstField(fields, "Process Status"),
    purpose: firstField(fields, "Purpose"),
    filerCommitteeName: title.name,
    filerCommitteeId: title.id,
    addressBookType: firstField(fields, "Address Book Type"),
    contributorPayeeName: firstField(fields, "Name"),
    address: firstField(fields, "Address"),
    occupation: firstField(fields, "Occupation"),
    employerName: firstField(fields, "Employer Name"),
    outsideAssociations: parseOregonOrestarOutsideAssociations(associationsText),
    sourceUrl,
  };
}

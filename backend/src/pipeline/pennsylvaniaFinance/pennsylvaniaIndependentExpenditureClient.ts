export const PENNSYLVANIA_INDEPENDENT_EXPENDITURE_DEFAULT_URL =
  "https://www.campaignfinanceonline.pa.gov/pages/IndependentExpenditures.aspx";
export const PENNSYLVANIA_INDEPENDENT_EXPENDITURE_DEFAULT_TIMEOUT_MS = 60_000;

export type PennsylvaniaIndependentExpenditureClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type PennsylvaniaIndependentExpenditureRow = {
  CandidateQuestion?: string;
  Organization?: string;
  Amount?: string | number | null;
  IsSupported?: string | number | boolean | null;
  IsOpposed?: string | number | boolean | null;
  ElectionID?: string | number | null;
  [key: string]: unknown;
};

export type PennsylvaniaIndependentExpenditureElectionOption = {
  id: string;
  label: string;
  selected: boolean;
};

export type PennsylvaniaIndependentExpenditurePage = {
  rows: PennsylvaniaIndependentExpenditureRow[];
  sourceUrl: string;
  electionId: string | null;
  electionOptions: PennsylvaniaIndependentExpenditureElectionOption[];
};

export type FetchPennsylvaniaIndependentExpendituresInput = {
  url?: string;
  electionId?: string | null;
  electionFieldName?: string;
};

function htmlDecode(value: string): string {
  return value
    .replace(/&quot;/g, "\"")
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function normalizeHttpsUrl(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid Pennsylvania independent expenditure URL: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid Pennsylvania independent expenditure URL protocol: ${parsed.protocol}`);
  }
  return parsed.toString();
}

function parseHtmlAttributes(tag: string): Record<string, string> {
  const attributes: Record<string, string> = {};
  const pattern = /([A-Za-z_:][A-Za-z0-9_:.-]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/g;
  for (const match of tag.matchAll(pattern)) {
    const name = match[1]?.toLowerCase();
    const value = match[2] ?? match[3] ?? match[4] ?? "";
    if (name) {
      attributes[name] = htmlDecode(value);
    }
  }
  return attributes;
}

export function parsePennsylvaniaIndependentExpenditureHiddenFields(html: string): Map<string, string> {
  const fields = new Map<string, string>();
  for (const match of html.matchAll(/<input\b[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const type = attrs.type?.toLowerCase() ?? "";
    const name = attrs.name ?? attrs.id;
    if (name && type === "hidden") {
      fields.set(name, attrs.value ?? "");
    }
  }
  return fields;
}

function selectTags(html: string): string[] {
  return [...html.matchAll(/<select\b[\s\S]*?<\/select>/gi)].map((match) => match[0]);
}

function selectName(tag: string): string | null {
  return parseHtmlAttributes(tag.match(/<select\b[^>]*>/i)?.[0] ?? "").name ?? null;
}

function selectedOptionValue(tag: string): string | null {
  const selected = tag.match(/<option\b(?=[^>]*\bselected\b)[^>]*>/i);
  if (!selected) {
    return null;
  }
  return parseHtmlAttributes(selected[0]).value ?? null;
}

export function findPennsylvaniaIndependentExpenditureElectionFieldName(html: string): string | null {
  for (const tag of selectTags(html)) {
    const name = selectName(tag);
    if (!name) {
      continue;
    }
    const normalizedName = name.toLowerCase();
    if (normalizedName.includes("election")) {
      return name;
    }
  }
  return null;
}

export function parsePennsylvaniaIndependentExpenditureElectionOptions(
  html: string,
  electionFieldName?: string
): PennsylvaniaIndependentExpenditureElectionOption[] {
  const options: PennsylvaniaIndependentExpenditureElectionOption[] = [];
  const selectedFieldName = electionFieldName ?? findPennsylvaniaIndependentExpenditureElectionFieldName(html);
  if (!selectedFieldName) {
    return options;
  }

  const selectTag = selectTags(html).find((tag) => selectName(tag) === selectedFieldName);
  if (!selectTag) {
    return options;
  }

  for (const match of selectTag.matchAll(/<option\b[^>]*>([\s\S]*?)<\/option>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const id = (attrs.value ?? "").trim();
    const label = htmlDecode((match[1] ?? "").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
    if (id) {
      options.push({
        id,
        label,
        selected: /\bselected\b/i.test(match[0]),
      });
    }
  }
  return options;
}

function scanQuotedString(source: string, startIndex: number): { quote: string; rawValue: string; endIndex: number } {
  const quote = source[startIndex];
  if (quote !== "\"" && quote !== "'") {
    throw new Error("Pennsylvania independent expenditure dataJson string was not quoted");
  }
  let escaped = false;
  let rawValue = "";
  for (let index = startIndex + 1; index < source.length; index += 1) {
    const char = source[index] ?? "";
    if (escaped) {
      rawValue += `\\${char}`;
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (char === quote) {
      return { quote, rawValue, endIndex: index + 1 };
    }
    rawValue += char;
  }
  throw new Error("Unterminated Pennsylvania independent expenditure dataJson string");
}

function decodeJavaScriptStringLiteral(input: { quote: string; rawValue: string }): string {
  let decoded = "";
  for (let index = 0; index < input.rawValue.length; index += 1) {
    const char = input.rawValue[index] ?? "";
    if (char !== "\\") {
      decoded += char;
      continue;
    }
    const next = input.rawValue[index + 1] ?? "";
    index += 1;
    switch (next) {
      case "\"":
      case "'":
      case "\\":
      case "/":
        decoded += next;
        break;
      case "b":
        decoded += "\b";
        break;
      case "f":
        decoded += "\f";
        break;
      case "n":
        decoded += "\n";
        break;
      case "r":
        decoded += "\r";
        break;
      case "t":
        decoded += "\t";
        break;
      case "u": {
        const hex = input.rawValue.slice(index + 1, index + 5);
        if (/^[0-9a-fA-F]{4}$/.test(hex)) {
          decoded += String.fromCharCode(Number.parseInt(hex, 16));
          index += 4;
        } else {
          decoded += "u";
        }
        break;
      }
      default:
        decoded += next;
        break;
    }
  }
  return decoded;
}

function scanJsonArray(source: string, startIndex: number): string {
  let depth = 0;
  let inString = false;
  let quote = "";
  let escaped = false;
  for (let index = startIndex; index < source.length; index += 1) {
    const char = source[index] ?? "";
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === quote) {
        inString = false;
      }
      continue;
    }
    if (char === "\"" || char === "'") {
      inString = true;
      quote = char;
      continue;
    }
    if (char === "[") {
      depth += 1;
    } else if (char === "]") {
      depth -= 1;
      if (depth === 0) {
        return source.slice(startIndex, index + 1);
      }
    }
  }
  throw new Error("Unterminated Pennsylvania independent expenditure dataJson array");
}

function asRows(value: unknown): PennsylvaniaIndependentExpenditureRow[] {
  if (!Array.isArray(value)) {
    throw new Error("Pennsylvania independent expenditure dataJson was not an array");
  }
  return value.filter((row): row is PennsylvaniaIndependentExpenditureRow => row !== null && typeof row === "object");
}

export function parsePennsylvaniaIndependentExpenditureDataJson(html: string): PennsylvaniaIndependentExpenditureRow[] {
  const markerIndex = html.search(/\bdataJson\b/);
  if (markerIndex < 0) {
    throw new Error("Pennsylvania independent expenditure page did not include dataJson");
  }
  const assignmentIndex = html.indexOf("=", markerIndex);
  if (assignmentIndex < 0) {
    throw new Error("Pennsylvania independent expenditure dataJson assignment was not found");
  }
  const rest = html.slice(assignmentIndex + 1).trimStart();

  if (/^JSON\.parse\s*\(/i.test(rest)) {
    const openingParen = rest.indexOf("(");
    const stringStart = rest.slice(openingParen + 1).search(/["']/);
    if (stringStart < 0) {
      throw new Error("Pennsylvania independent expenditure JSON.parse dataJson was missing a string");
    }
    const literal = scanQuotedString(rest, openingParen + 1 + stringStart);
    return asRows(JSON.parse(htmlDecode(decodeJavaScriptStringLiteral(literal))));
  }

  const arrayStart = rest.indexOf("[");
  if (arrayStart < 0) {
    throw new Error("Pennsylvania independent expenditure dataJson array was not found");
  }
  return asRows(JSON.parse(htmlDecode(scanJsonArray(rest, arrayStart))));
}

export function buildPennsylvaniaIndependentExpenditureElectionPostbackBody(input: {
  html: string;
  electionId: string;
  electionFieldName?: string;
}): URLSearchParams {
  const electionId = input.electionId.trim();
  if (!electionId) {
    throw new Error("Pennsylvania independent expenditure electionId is required");
  }
  const electionFieldName =
    input.electionFieldName ?? findPennsylvaniaIndependentExpenditureElectionFieldName(input.html);
  if (!electionFieldName) {
    throw new Error("Pennsylvania independent expenditure election select field was not found");
  }

  const params = new URLSearchParams();
  for (const [name, value] of parsePennsylvaniaIndependentExpenditureHiddenFields(input.html)) {
    params.set(name, value);
  }
  params.set("__EVENTTARGET", params.get("__EVENTTARGET") || electionFieldName);
  params.set("__EVENTARGUMENT", params.get("__EVENTARGUMENT") || "");
  params.set(electionFieldName, electionId);
  return params;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchPennsylvaniaIndependentExpenditureHtml(
  url: string,
  init: RequestInit,
  options: PennsylvaniaIndependentExpenditureClientOptions
): Promise<string> {
  const timeoutMs = options.timeoutMs ?? PENNSYLVANIA_INDEPENDENT_EXPENDITURE_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await (options.fetchImpl ?? fetch)(url, { ...init, signal: controller.signal });
    if (!response.ok) {
      throw new Error(`Pennsylvania independent expenditure request failed: ${response.status} ${response.statusText}`);
    }
    return await response.text();
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Pennsylvania independent expenditure request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export async function fetchPennsylvaniaIndependentExpenditures(
  input: FetchPennsylvaniaIndependentExpendituresInput = {},
  options: PennsylvaniaIndependentExpenditureClientOptions = {}
): Promise<PennsylvaniaIndependentExpenditurePage> {
  const sourceUrl = normalizeHttpsUrl(input.url ?? PENNSYLVANIA_INDEPENDENT_EXPENDITURE_DEFAULT_URL);
  const landingHtml = await fetchPennsylvaniaIndependentExpenditureHtml(
    sourceUrl,
    { headers: { accept: "text/html,*/*;q=0.8" } },
    options
  );
  const electionId = input.electionId?.trim() || null;
  const html = electionId
    ? await fetchPennsylvaniaIndependentExpenditureHtml(
        sourceUrl,
        {
          method: "POST",
          headers: {
            accept: "text/html,*/*;q=0.8",
            "content-type": "application/x-www-form-urlencoded",
          },
          body: buildPennsylvaniaIndependentExpenditureElectionPostbackBody({
            html: landingHtml,
            electionId,
            electionFieldName: input.electionFieldName,
          }).toString(),
        },
        options
      )
    : landingHtml;

  return {
    rows: parsePennsylvaniaIndependentExpenditureDataJson(html),
    sourceUrl,
    electionId,
    electionOptions: parsePennsylvaniaIndependentExpenditureElectionOptions(html, input.electionFieldName),
  };
}

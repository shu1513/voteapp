export const SAN_FRANCISCO_OPEN_DATA_BASE_URL =
  "https://data.sfgov.org/resource";
export const SAN_FRANCISCO_SUMMARY_TOTALS_DATASET_ID = "9ggq-m8hp";
export const SAN_FRANCISCO_TRANSACTIONS_DATASET_ID = "pitq-e56w";

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_PAGE_LIMIT = 1_000;
const DEFAULT_MAX_PAGES = 100;

export type SanFranciscoOpenDataClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  appToken?: string;
  pageLimit?: number;
  maxPages?: number;
  retryCount?: number;
};

export function defaultSanFranciscoOpenDataClientOptions(): SanFranciscoOpenDataClientOptions {
  const appToken = process.env.SAN_FRANCISCO_OPEN_DATA_APP_TOKEN?.trim();
  return appToken ? { appToken } : {};
}

// DataSF money values are decimal strings ("4841006.0", "92.4"). Convert to
// integer cents without ever passing through binary floating point so that
// to-the-cent reconciliation is exact.
export function moneyStringToCents(value: unknown): number | null {
  const text =
    typeof value === "string"
      ? value.trim()
      : typeof value === "number"
        ? String(value)
        : "";
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(text);
  if (!match) return null;
  const sign = match[1] === "-" ? -1 : 1;
  const whole = Number(match[2]);
  const fractionText = (match[3] ?? "").padEnd(2, "0");
  // A third decimal digit would mean sub-cent data; round half up on the
  // magnitude, which matches how the source rounds published figures.
  const cents = Number(fractionText.slice(0, 2));
  const roundUp = Number(fractionText[2] ?? "0") >= 5 ? 1 : 0;
  if (!Number.isSafeInteger(whole)) return null;
  return sign * (whole * 100 + cents + roundUp);
}

function soqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function stringValue(row: Record<string, unknown>, key: string): string {
  const value = row[key];
  return typeof value === "string" ? value.trim() : "";
}

async function fetchRows(
  url: string,
  options: SanFranciscoOpenDataClientOptions,
): Promise<Record<string, unknown>[]> {
  const retries = options.retryCount ?? 2;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const headers = new Headers({ accept: "application/json" });
    if (options.appToken?.trim())
      headers.set("X-App-Token", options.appToken.trim());
    try {
      const response = await (options.fetchImpl ?? fetch)(url, {
        headers,
        signal: controller.signal,
      });
      if (!response.ok) {
        if (
          (response.status === 429 || response.status >= 500) &&
          attempt < retries
        )
          continue;
        throw new Error(
          `San Francisco Open Data request failed: ${response.status} ${response.statusText}`,
        );
      }
      const payload: unknown = await response.json();
      if (!Array.isArray(payload))
        throw new Error("San Francisco Open Data response is not an array");
      return payload.filter(
        (row): row is Record<string, unknown> =>
          typeof row === "object" && row !== null && !Array.isArray(row),
      );
    } catch (error) {
      if (
        attempt < retries &&
        (error instanceof TypeError ||
          (error instanceof Error && error.name === "AbortError"))
      )
        continue;
      if (error instanceof Error && error.name === "AbortError")
        throw new Error(
          `San Francisco Open Data request timed out after ${timeoutMs}ms`,
        );
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error("San Francisco Open Data request exhausted retries");
}

async function fetchAllPages(
  datasetId: string,
  params: Record<string, string>,
  options: SanFranciscoOpenDataClientOptions,
): Promise<Record<string, unknown>[]> {
  const pageLimit = options.pageLimit ?? DEFAULT_PAGE_LIMIT;
  const maxPages = options.maxPages ?? DEFAULT_MAX_PAGES;
  if (!Number.isInteger(pageLimit) || pageLimit <= 0)
    throw new Error(`Invalid San Francisco Open Data pageLimit: ${pageLimit}`);
  if (!Number.isInteger(maxPages) || maxPages <= 0)
    throw new Error(`Invalid San Francisco Open Data maxPages: ${maxPages}`);
  const results: Record<string, unknown>[] = [];
  for (let page = 0; page < maxPages; page += 1) {
    const url = new URL(`${SAN_FRANCISCO_OPEN_DATA_BASE_URL}/${datasetId}.json`);
    for (const [key, value] of Object.entries(params))
      url.searchParams.set(key, value);
    url.searchParams.set("$limit", String(pageLimit));
    url.searchParams.set("$offset", String(page * pageLimit));
    const rows = await fetchRows(url.toString(), options);
    results.push(...rows);
    if (rows.length < pageLimit) return results;
  }
  throw new Error(
    `San Francisco Open Data query exceeded ${maxPages} pages: ${datasetId}`,
  );
}

export type SanFranciscoSummaryRow = {
  filingNid: string;
  filingIdNumber: string;
  filingType: string;
  formType: string;
  periodStart: string | null;
  periodEnd: string | null;
  /** Form 460 line 5 column A — total contributions for the period, cents. */
  contributionsCents: number | null;
  /** Form 460 line 11 column A — total expenditures for the period, cents. */
  expendituresCents: number | null;
};

/**
 * Form 460 summary rows for one committee, ordered by period start. The
 * published dataset carries only the current version of each filing
 * (amendments replace originals upstream); that guarantee is asserted here
 * so silent duplicates can never inflate a reconciliation.
 */
export async function getSanFranciscoCommitteeSummaryRows(
  input: { fppcId: string },
  options: SanFranciscoOpenDataClientOptions = {},
): Promise<SanFranciscoSummaryRow[]> {
  const fppcId = input.fppcId.trim();
  if (!/^\d{4,12}$/.test(fppcId))
    throw new Error(`Invalid San Francisco FPPC id: ${input.fppcId}`);
  const rows = await fetchAllPages(
    SAN_FRANCISCO_SUMMARY_TOTALS_DATASET_ID,
    {
      $where: `fppc_id=${soqlString(fppcId)}`,
      $order: "start_date,filing_id_number,:id",
    },
    options,
  );
  const seenFilingNids = new Set<string>();
  const results: SanFranciscoSummaryRow[] = [];
  for (const row of rows) {
    const filingNid = stringValue(row, "filing_nid");
    if (!filingNid)
      throw new Error(
        `San Francisco summary row for committee ${fppcId} is missing filing_nid`,
      );
    if (seenFilingNids.has(filingNid))
      throw new Error(
        `San Francisco summary rows for committee ${fppcId} contain duplicate filing_nid ${filingNid}; the current-version-only source guarantee is broken`,
      );
    seenFilingNids.add(filingNid);
    results.push({
      filingNid,
      filingIdNumber: stringValue(row, "filing_id_number"),
      filingType: stringValue(row, "filing_type"),
      formType: stringValue(row, "form_type"),
      periodStart: stringValue(row, "start_date") || null,
      periodEnd: stringValue(row, "end_date") || null,
      contributionsCents: moneyStringToCents(row["line_5_col_a"]),
      expendituresCents: moneyStringToCents(row["line_11_col_a"]),
    });
  }
  return results;
}

export type SanFranciscoTargetedSpendingRow = {
  spenderFppcId: string | null;
  spenderName: string;
  formType: string;
  supportOpposeCode: string | null;
  amountCents: number;
  transactionCount: number;
};

/**
 * Server-side aggregation of transactions that name a target candidate,
 * grouped by spender committee, form type, and disclosed direction. Used by
 * the Phase 0 probe to quantify how much of the dashboard's outside money is
 * visible through candidate-tagged rows alone (F496 vs Schedule D overlap,
 * primarily-formed committee spending that carries no candidate tag, …).
 */
export async function getSanFranciscoCandidateTargetedSpending(
  input: {
    candidateLastName: string;
    candidateFirstName?: string;
    electionDate?: string;
  },
  options: SanFranciscoOpenDataClientOptions = {},
): Promise<SanFranciscoTargetedSpendingRow[]> {
  const lastName = input.candidateLastName.trim().toUpperCase();
  if (!lastName)
    throw new Error("San Francisco targeted-spending query needs a last name");
  const conditions = [`upper(candidate_last_name)=${soqlString(lastName)}`];
  const firstName = input.candidateFirstName?.trim().toUpperCase();
  if (firstName)
    conditions.push(`upper(candidate_first_name)=${soqlString(firstName)}`);
  if (input.electionDate) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(input.electionDate))
      throw new Error(
        `Invalid San Francisco election date: ${input.electionDate}`,
      );
    conditions.push(
      `election_date=${soqlString(`${input.electionDate}T00:00:00.000`)}`,
    );
  }
  const rows = await fetchAllPages(
    SAN_FRANCISCO_TRANSACTIONS_DATASET_ID,
    {
      $select:
        "fppc_id,filer_name,form_type,support_oppose_code,sum(calculated_amount) AS amount,count(*) AS transaction_count",
      $where: conditions.join(" AND "),
      $group: "fppc_id,filer_name,form_type,support_oppose_code",
      $order: "amount DESC",
    },
    options,
  );
  const results: SanFranciscoTargetedSpendingRow[] = [];
  for (const row of rows) {
    const amountCents = moneyStringToCents(row["amount"]);
    if (amountCents === null) continue;
    const fppcId = stringValue(row, "fppc_id");
    results.push({
      spenderFppcId: /^\d{4,12}$/.test(fppcId) ? fppcId : null,
      spenderName: stringValue(row, "filer_name"),
      formType: stringValue(row, "form_type"),
      supportOpposeCode: stringValue(row, "support_oppose_code") || null,
      amountCents,
      transactionCount: Number(row["transaction_count"] ?? 0) || 0,
    });
  }
  return results;
}

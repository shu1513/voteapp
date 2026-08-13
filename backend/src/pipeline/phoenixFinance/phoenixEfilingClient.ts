// Transport + registration handling for the Phoenix City Clerk eFiling
// portal, promoted out of the Phase 0 probe (probePhoenixCandidateFinance.ts)
// where every behavior below was pinned against the live portal 2026-08-12:
//
//   POST /CampaignFinance/Search/_Search{Committees,Contributors,...}
//   Content-Type: application/x-www-form-urlencoded; charset=UTF-8
//   X-Requested-With: XMLHttpRequest   <-- required; without it the action
//                                          ignores the filters
//   body: sort=&page=N&pageSize=N&group=&filter=&<FILTERS>
//
// No cookies and no verification token are required. The WAF serves an HTML
// "maintenance" page (HTTP 200) to non-browser user agents, so every response
// is validated as the JSON envelope before use — an HTML body is a fetch
// failure, never empty data.
//
// Registration rows are document VERSIONS, not committees (verified: one
// committee re-registers for a new cycle by AMENDING under the same COP ID —
// Robinson CAN-21-16 carries a 2021-cycle and a 2025-cycle version). The
// canonical row per COP ID is the latest approved version. Live field facts
// the resolver depends on (re-verified 2026-08-12 on CAN-25-4 / CAN-23-5 /
// CAN-21-16 / CAN-22-10):
//   - CommitteeType is a display string ("Candidate Committee", "Political
//     Action Committee", ...) — the candidate-committee gate.
//   - ElectionCycle is a heterogeneous display string ("2025 Election Cycle",
//     "2026-2027 CAN > Districts 2, 4, 6 and 8") — stored verbatim as the
//     link's portal_cycle_name, NEVER parsed for dates.
//   - OfficeSoughtElectionCycle is the ELECTION YEAR sought ("2026") — the
//     machine-readable cycle evidence (a 2023-cycle registration legitimately
//     targets 2026: Jimenez CAN-23-5).
//   - CandidateOfficeSought / CandidateRunningDistrict are null on live rows;
//     district evidence must come from elsewhere (report covers).

export const PHOENIX_PORTAL_BASE_URL = "https://apps-secure.phoenix.gov";

const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const MAX_GRID_PAGES = 200;
const GRID_PAGE_SIZE = 100;
export const PHOENIX_PORTAL_DEFAULT_TIMEOUT_MS = 30_000;
// Plan fetch hygiene: size caps on every portal response (a grid page is
// ~100 rows of JSON; the cap only exists to bound a misbehaving response).
const MAX_RESPONSE_BYTES = 32 * 1024 * 1024;

// Production data contains explicit test registrations ("2021 New City of
// Phoenix Test Committee", PAC-21-15) — excluded everywhere.
export const PHOENIX_TEST_COMMITTEE_PATTERN = /\btest\b/i;

type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<{ status: number; text: () => Promise<string> }>;

export type PhoenixGridEnvelope = {
  Data: Record<string, unknown>[];
  Total: number;
};

/** Rejects any body that is not the Kendo JSON envelope — the WAF's
 * maintenance page is HTML with HTTP 200. */
export function parsePhoenixGridEnvelope(
  text: string,
  context: string,
): PhoenixGridEnvelope {
  const trimmed = text.trimStart();
  if (!trimmed.startsWith("{")) {
    const label = /maintenance/i.test(text)
      ? "WAF maintenance page"
      : "non-JSON body";
    throw new Error(`Phoenix grid returned a ${label} for ${context}`);
  }
  const parsed = JSON.parse(trimmed) as Partial<PhoenixGridEnvelope>;
  if (!Array.isArray(parsed.Data) || typeof parsed.Total !== "number") {
    throw new Error(`Phoenix grid envelope missing Data/Total for ${context}`);
  }
  return { Data: parsed.Data, Total: parsed.Total };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

/** POST one Kendo grid page. Non-2xx statuses are rejected before the body
 * is ever parsed, and the abort timeout stays armed through the BODY read,
 * not just the headers — a stalled body would otherwise hang the caller
 * with no clock running (the Denver SearchLight client pattern). */
export async function phoenixGridPage(input: {
  path: string;
  filters: Readonly<Record<string, string>>;
  page: number;
  pageSize?: number;
  timeoutMs?: number;
  fetchImpl?: FetchLike;
}): Promise<PhoenixGridEnvelope> {
  const pageSize = input.pageSize ?? GRID_PAGE_SIZE;
  const timeoutMs = input.timeoutMs ?? PHOENIX_PORTAL_DEFAULT_TIMEOUT_MS;
  const context = `${input.path} page ${input.page}`;
  const body = new URLSearchParams({
    sort: "",
    page: String(input.page),
    pageSize: String(pageSize),
    group: "",
    filter: "",
    ...input.filters,
  });
  const fetchImpl = input.fetchImpl ?? (fetch as FetchLike);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    let response: Awaited<ReturnType<FetchLike>>;
    try {
      response = await fetchImpl(`${PHOENIX_PORTAL_BASE_URL}${input.path}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          "X-Requested-With": "XMLHttpRequest",
          "User-Agent": BROWSER_USER_AGENT,
        },
        body: body.toString(),
        signal: controller.signal,
      });
    } catch (error) {
      if (isAbortError(error)) {
        throw new Error(
          `Phoenix grid request timed out after ${timeoutMs}ms for ${context}`,
        );
      }
      throw error;
    }
    if (response.status < 200 || response.status >= 300) {
      throw new Error(
        `Phoenix grid request failed for ${context}: HTTP ${response.status}`,
      );
    }
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      if (isAbortError(error)) {
        throw new Error(
          `Phoenix grid body read timed out after ${timeoutMs}ms for ${context}`,
        );
      }
      throw error;
    }
    if (text.length > MAX_RESPONSE_BYTES) {
      throw new Error(
        `Phoenix grid response for ${context} is ${text.length} characters, over the ${MAX_RESPONSE_BYTES} cap`,
      );
    }
    return parsePhoenixGridEnvelope(
      text,
      `${context} (HTTP ${response.status})`,
    );
  } finally {
    clearTimeout(timeout);
  }
}

/** Pages through a grid until Total rows are collected; throws on premature
 * exhaustion (an empty page before Total is reached would silently truncate
 * the committee universe). */
export async function phoenixGridAll(input: {
  path: string;
  filters: Readonly<Record<string, string>>;
  timeoutMs?: number;
  fetchImpl?: FetchLike;
}): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let page = 1; page <= MAX_GRID_PAGES; page += 1) {
    const envelope = await phoenixGridPage({ ...input, page });
    rows.push(...envelope.Data);
    if (rows.length >= envelope.Total) return rows;
    if (envelope.Data.length === 0) {
      throw new Error(
        `Phoenix grid ${input.path} exhausted at ${rows.length}/${envelope.Total} rows — incomplete pagination`,
      );
    }
  }
  throw new Error(`Phoenix grid ${input.path} exceeded ${MAX_GRID_PAGES} pages`);
}

/** One registration document version as returned by _SearchCommittees. */
export type PhoenixRegistrationRow = {
  copId: string;
  committeeName: string;
  /** Display string; candidate committees are exactly "Candidate Committee". */
  committeeType: string;
  candidateName: string | null;
  /** Display string ("2025 Election Cycle") — portal_cycle_name verbatim. */
  electionCycle: string;
  /** Election YEAR sought ("2026"); the machine-readable cycle evidence. */
  officeSoughtElectionCycle: string | null;
  terminated: boolean;
  approved: boolean;
  approvedTimestamp: number;
  isStandingCommittee: boolean;
};

export function toPhoenixRegistrationRow(
  raw: Record<string, unknown>,
): PhoenixRegistrationRow {
  // "AppovedTimestamp" is the portal's own field spelling.
  const timestamp = /\/Date\((\d+)\)\//.exec(String(raw.AppovedTimestamp ?? ""));
  const officeSought = String(raw.OfficeSoughtElectionCycle ?? "").trim();
  return {
    copId: String(raw.COPID ?? "").trim().toUpperCase(),
    committeeName: String(raw.CommitteeName ?? "").trim(),
    committeeType: String(raw.CommitteeType ?? "").trim(),
    candidateName: raw.CandidateName
      ? String(raw.CandidateName).trim() || null
      : null,
    electionCycle: String(raw.ElectionCycle ?? "").trim(),
    officeSoughtElectionCycle: officeSought || null,
    terminated: raw.Terminated === true,
    approved: raw.Approved === true,
    approvedTimestamp: timestamp ? Number(timestamp[1]) : 0,
    isStandingCommittee: raw.IsStandingCommittee === true,
  };
}

/** The canonical row per COP ID is the latest approved document version
 * (a terminated or re-registered version supersedes older ones); null when
 * no version is approved. */
export function canonicalPhoenixRegistration(
  rows: readonly PhoenixRegistrationRow[],
): PhoenixRegistrationRow | null {
  const approved = rows.filter((row) => row.approved);
  if (approved.length === 0) return null;
  return approved.reduce((best, row) =>
    row.approvedTimestamp > best.approvedTimestamp ? row : best,
  );
}

/**
 * Fetches the portal-wide registration index (~860 document versions,
 * 2026-08-12) and collapses it to one canonical registration per COP ID.
 * ALL committee types are returned — the resolver gates on type itself so a
 * stored id pointing at a PAC fails with a precise reason instead of
 * vanishing from the universe.
 */
export async function fetchPhoenixCanonicalRegistrations(input?: {
  timeoutMs?: number;
  fetchImpl?: FetchLike;
}): Promise<PhoenixRegistrationRow[]> {
  const rows = (
    await phoenixGridAll({
      path: "/CampaignFinance/Search/_SearchCommittees",
      filters: {},
      timeoutMs: input?.timeoutMs,
      fetchImpl: input?.fetchImpl,
    })
  ).map(toPhoenixRegistrationRow);
  const byCopId = new Map<string, PhoenixRegistrationRow[]>();
  for (const row of rows) {
    if (!row.copId) continue;
    const bucket = byCopId.get(row.copId) ?? [];
    bucket.push(row);
    byCopId.set(row.copId, bucket);
  }
  const canonical: PhoenixRegistrationRow[] = [];
  for (const key of [...byCopId.keys()].sort()) {
    const row = canonicalPhoenixRegistration(byCopId.get(key)!);
    if (row !== null) canonical.push(row);
  }
  return canonical;
}

/** The Kendo grid's political-function GUID for "Candidate-Related
 * Independent Expenditures" (read from the RegFilings page's
 * POLITICALFUNCTION select, pinned by the Phase 0 probe). Filtering
 * _SearchCommittees by CFUNC with this id returns every IE-authorized
 * registration. */
export const PHOENIX_CANDIDATE_IE_FUNCTION_ID =
  "a182d408-b233-4b2b-b444-4d260375dc5f";

type PdfFetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<{ status: number; arrayBuffer: () => Promise<ArrayBuffer> }>;

export type PhoenixPdfCache = {
  read: (reportPackageId: string) => Promise<Uint8Array | null>;
  write: (reportPackageId: string, bytes: Uint8Array) => Promise<void>;
};

/** Filesystem PDF cache under cacheDir (the probe's layout:
 * `<cacheDir>/<packageId>.pdf`). Read errors mean cache miss. */
export function phoenixFilesystemPdfCache(cacheDir: string): PhoenixPdfCache {
  return {
    read: async (reportPackageId) => {
      try {
        const { readFile } = await import("node:fs/promises");
        const { join } = await import("node:path");
        const cached = await readFile(join(cacheDir, `${reportPackageId}.pdf`));
        // pdfjs rejects Node Buffers — hand back a plain Uint8Array view.
        return new Uint8Array(cached.buffer, cached.byteOffset, cached.byteLength);
      } catch {
        return null;
      }
    },
    write: async (reportPackageId, bytes) => {
      const { mkdir, writeFile } = await import("node:fs/promises");
      const { join } = await import("node:path");
      await mkdir(cacheDir, { recursive: true });
      await writeFile(join(cacheDir, `${reportPackageId}.pdf`), bytes);
    },
  };
}

/**
 * Fetches one report PDF (`/CampaignFinance/Reports/PrintReport/<guid>`),
 * validating the %PDF signature — the WAF maintenance page comes back HTML
 * with HTTP 200 here too. Reports are immutable per package id (amendments
 * get NEW ids), so a cache hit never revalidates.
 */
export async function fetchPhoenixReportPdf(input: {
  reportPackageId: string;
  cache?: PhoenixPdfCache;
  timeoutMs?: number;
  fetchImpl?: PdfFetchLike;
}): Promise<Uint8Array> {
  const id = input.reportPackageId;
  if (!/^[0-9a-f-]{36}$/i.test(id)) {
    throw new Error(`Not a report package GUID: "${id}"`);
  }
  const cached = await input.cache?.read(id);
  if (cached) return cached;
  const timeoutMs = input.timeoutMs ?? PHOENIX_PORTAL_DEFAULT_TIMEOUT_MS;
  const fetchImpl = input.fetchImpl ?? (fetch as PdfFetchLike);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    let response: Awaited<ReturnType<PdfFetchLike>>;
    try {
      response = await fetchImpl(
        `${PHOENIX_PORTAL_BASE_URL}/CampaignFinance/Reports/PrintReport/${id}`,
        {
          headers: { "User-Agent": BROWSER_USER_AGENT },
          signal: controller.signal,
        },
      );
    } catch (error) {
      if (isAbortError(error)) {
        throw new Error(
          `Phoenix report PDF fetch timed out after ${timeoutMs}ms for ${id}`,
        );
      }
      throw error;
    }
    if (response.status < 200 || response.status >= 300) {
      throw new Error(`Phoenix report PDF fetch failed for ${id}: HTTP ${response.status}`);
    }
    let bytes: Uint8Array;
    try {
      bytes = new Uint8Array(await response.arrayBuffer());
    } catch (error) {
      if (isAbortError(error)) {
        throw new Error(
          `Phoenix report PDF body read timed out after ${timeoutMs}ms for ${id}`,
        );
      }
      throw error;
    }
    if (bytes.length > MAX_RESPONSE_BYTES) {
      throw new Error(
        `Phoenix report PDF ${id} is ${bytes.length} bytes, over the ${MAX_RESPONSE_BYTES} cap`,
      );
    }
    if (bytes.length < 4 || String.fromCharCode(...bytes.slice(0, 4)) !== "%PDF") {
      throw new Error(`Phoenix report ${id} is not a PDF`);
    }
    await input.cache?.write(id, bytes);
    return bytes;
  } finally {
    clearTimeout(timeout);
  }
}

export type PhoenixReportRef = {
  reportPackageId: string;
  reportName: string;
  submittedDateMs: number;
};

const REPORT_PACKAGE_GUID = /^[0-9a-f-]{36}$/i;

function collectPhoenixReportRefs(
  refs: Map<string, PhoenixReportRef>,
  rows: readonly Record<string, unknown>[],
): void {
  for (const row of rows) {
    const id = String(row.ReportPackageId ?? "");
    if (!REPORT_PACKAGE_GUID.test(id)) continue;
    const submitted = /\/Date\((\d+)\)\//.exec(String(row.SubmittedDate ?? ""));
    refs.set(id, {
      reportPackageId: id,
      reportName: String(row.ReportName ?? ""),
      submittedDateMs: submitted ? Number(submitted[1]) : 0,
    });
  }
}

/** Amendment canonicalization (Phase 0 gate 4): duplicate (report name)
 * packages are superseded versions; the latest SubmittedDate wins and losers
 * are dropped. Grids expose one package per period AFTER amendment, so the
 * parsed periods must come out disjoint — the aggregator asserts that. */
export function canonicalPhoenixReportRefs(
  refs: readonly PhoenixReportRef[],
): { refs: PhoenixReportRef[]; supersededDropped: number } {
  const byName = new Map<string, PhoenixReportRef[]>();
  for (const ref of refs) {
    const bucket = byName.get(ref.reportName) ?? [];
    bucket.push(ref);
    byName.set(ref.reportName, bucket);
  }
  const canonical: PhoenixReportRef[] = [];
  let supersededDropped = 0;
  for (const bucket of byName.values()) {
    const winner = bucket.reduce((best, ref) =>
      ref.submittedDateMs > best.submittedDateMs ? ref : best,
    );
    canonical.push(winner);
    supersededDropped += bucket.length - 1;
  }
  canonical.sort((a, b) => a.submittedDateMs - b.submittedDateMs);
  return { refs: canonical, supersededDropped };
}

/**
 * Discovers a committee's filed report packages via the three transaction
 * grids. Contribution rows alone miss expenditure-only and loan-only
 * reports, so all three are read. No-activity filings DO surface (verified
 * live: CAN-22-10's cover-only post-election reports come back through the
 * contributors grid, which carries an ISNOACTIVITY marker), so a report
 * invisible to all three grids is an anomaly — the aggregator's
 * coverage_hole check is what catches one that actually moved money.
 */
export async function discoverPhoenixCanonicalReportRefs(input: {
  copId: string;
  timeoutMs?: number;
  fetchImpl?: FetchLike;
}): Promise<{ refs: PhoenixReportRef[]; supersededDropped: number }> {
  const refs = new Map<string, PhoenixReportRef>();
  for (const path of [
    "/CampaignFinance/Search/_SearchContributors",
    "/CampaignFinance/Search/_SearchExpenditures",
    "/CampaignFinance/Search/_SearchLoans",
  ]) {
    collectPhoenixReportRefs(
      refs,
      await phoenixGridAll({
        path,
        filters: { COPID: input.copId },
        timeoutMs: input.timeoutMs,
        fetchImpl: input.fetchImpl,
      }),
    );
  }
  return canonicalPhoenixReportRefs([...refs.values()]);
}

/** Phoenix candidate election cycles run April 1 of an odd year through
 * March 31 two years later (city cycles PDF, read 2026-08-12). The bounds are
 * derived from this documented rule and a date INSIDE the cycle — never from
 * COP-ID digits and never by parsing the heterogeneous ElectionCycle display
 * strings. */
export function phoenixCandidateCycleForDate(isoDate: string): {
  startYear: number;
  /** ISO date, April 1 of the odd start year. */
  cycleStart: string;
  /** ISO date, March 31 two years later. */
  cycleEnd: string;
} {
  const parsed = new Date(`${isoDate}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(isoDate) || Number.isNaN(parsed.getTime())) {
    throw new Error(`Not an ISO date: "${isoDate}"`);
  }
  const year = parsed.getUTCFullYear();
  const odd = year % 2 === 1 ? year : year - 1;
  const startYear =
    parsed.getTime() >= Date.UTC(odd, 3, 1) ? odd : odd - 2;
  return {
    startYear,
    cycleStart: `${startYear}-04-01`,
    cycleEnd: `${startYear + 2}-03-31`,
  };
}

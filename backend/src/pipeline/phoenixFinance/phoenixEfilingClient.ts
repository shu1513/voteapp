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

/** POST one Kendo grid page. */
export async function phoenixGridPage(input: {
  path: string;
  filters: Readonly<Record<string, string>>;
  page: number;
  pageSize?: number;
  fetchImpl?: FetchLike;
}): Promise<PhoenixGridEnvelope> {
  const pageSize = input.pageSize ?? GRID_PAGE_SIZE;
  const body = new URLSearchParams({
    sort: "",
    page: String(input.page),
    pageSize: String(pageSize),
    group: "",
    filter: "",
    ...input.filters,
  });
  const fetchImpl = input.fetchImpl ?? (fetch as FetchLike);
  const response = await fetchImpl(`${PHOENIX_PORTAL_BASE_URL}${input.path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": BROWSER_USER_AGENT,
    },
    body: body.toString(),
  });
  const text = await response.text();
  return parsePhoenixGridEnvelope(
    text,
    `${input.path} page ${input.page} (HTTP ${response.status})`,
  );
}

/** Pages through a grid until Total rows are collected; throws on premature
 * exhaustion (an empty page before Total is reached would silently truncate
 * the committee universe). */
export async function phoenixGridAll(input: {
  path: string;
  filters: Readonly<Record<string, string>>;
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
  fetchImpl?: FetchLike;
}): Promise<PhoenixRegistrationRow[]> {
  const rows = (
    await phoenixGridAll({
      path: "/CampaignFinance/Search/_SearchCommittees",
      filters: {},
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

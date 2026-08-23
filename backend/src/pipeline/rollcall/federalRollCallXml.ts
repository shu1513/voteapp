import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Fetch + parse for the two federal roll-call feeds (both plain XML, no
// key, no block — verified in docs/plans/roll-call-vote-import.md):
//   House:  https://clerk.house.gov/evs/<year>/roll<NNN>.xml
//   Senate: https://www.senate.gov/legislative/LIS/roll_call_votes/
//             vote<congress><session>/vote_<congress>_<session>_<NNNNN>.xml
// Only the roll-call metadata is parsed here; member rows live in
// federalRollCallMembers.ts and are read from the evidence copy of the XML
// at resolve / fan-out time, so the only member-level fact kept on the
// legislative_votes row is a count for the run report.

export type FederalRollCallUrls = {
  // Human page (legislative_votes.display_url).
  displayUrl: string;
  // Machine-readable file (legislative_votes.machine_url; the record's
  // eventual source_url).
  machineUrl: string;
};

export type ParsedFederalRollCall = {
  chamber: LegislativeVoteChamber;
  congress: number;
  session: number;
  rollNumber: number;
  // ISO date (YYYY-MM-DD) in the chamber's own calendar.
  voteDate: string;
  // Measure as the feed prints it; null when the vote has none.
  measureId: string | null;
  question: string;
  result: string;
  yeas: number;
  nays: number;
  // House vote-desc / Senate vote_title; display only.
  title: string | null;
  memberVoteCount: number;
};

/**
 * The House Clerk files votes by calendar year. The first session of the
 * Nth Congress sits in year 1787 + 2N (119th → 2025); the second session
 * one year later. A session's last days can spill into the following
 * January, so callers still verify the XML's own congress/session.
 */
export function houseRollCallYear(congress: number, session: number): number {
  return 1787 + 2 * congress + (session - 1);
}

export function houseRollCallUrls(congress: number, session: number, rollNumber: number): FederalRollCallUrls {
  const year = houseRollCallYear(congress, session);
  return {
    displayUrl: `https://clerk.house.gov/Votes/${year}${rollNumber}`,
    machineUrl: `https://clerk.house.gov/evs/${year}/roll${String(rollNumber).padStart(3, "0")}.xml`,
  };
}

export function senateRollCallUrls(congress: number, session: number, rollNumber: number): FederalRollCallUrls {
  const base = `https://www.senate.gov/legislative/LIS/roll_call_votes/vote${congress}${session}/vote_${congress}_${session}_${String(
    rollNumber
  ).padStart(5, "0")}`;
  return { displayUrl: `${base}.htm`, machineUrl: `${base}.xml` };
}

export function federalRollCallUrls(
  chamber: LegislativeVoteChamber,
  congress: number,
  session: number,
  rollNumber: number
): FederalRollCallUrls {
  return chamber === "house"
    ? houseRollCallUrls(congress, session, rollNumber)
    : senateRollCallUrls(congress, session, rollNumber);
}

// --- XML helpers ---------------------------------------------------------
//
// The feeds are flat, well-formed, and attribute-light, so a first-match tag
// reader is enough; the repo has no XML dependency and the finance clients
// take the same approach.

const ENTITY_TABLE: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
};

export function decodeXmlEntities(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, body: string) => {
    if (body.startsWith("#x") || body.startsWith("#X")) {
      return String.fromCodePoint(Number.parseInt(body.slice(2), 16));
    }
    if (body.startsWith("#")) {
      return String.fromCodePoint(Number.parseInt(body.slice(1), 10));
    }
    return ENTITY_TABLE[body.toLowerCase()] ?? whole;
  });
}

/**
 * Text of the first `<tag>` in `xml`, entity-decoded and whitespace-collapsed;
 * null when the tag is absent or empty (`<tag/>`, `<tag></tag>`).
 */
export function firstTagText(xml: string, tag: string): string | null {
  const match = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`).exec(xml);
  if (!match) {
    return null;
  }
  const text = decodeXmlEntities(match[1]!).replace(/\s+/g, " ").trim();
  return text.length > 0 ? text : null;
}

function firstTagBlock(xml: string, tag: string): string | null {
  const match = new RegExp(`<${tag}(?:\\s[^>]*)?>[\\s\\S]*?</${tag}>`).exec(xml);
  return match ? match[0] : null;
}

function countTag(xml: string, tag: string): number {
  return xml.split(`<${tag}>`).length - 1;
}

function requireText(xml: string, tag: string, feed: string): string {
  const text = firstTagText(xml, tag);
  if (text === null) {
    throw new Error(`${feed} XML is missing <${tag}>`);
  }
  return text;
}

function requireInteger(xml: string, tag: string, feed: string): number {
  const text = requireText(xml, tag, feed);
  if (!/^\d+$/.test(text)) {
    throw new Error(`${feed} XML <${tag}> is not an integer: ${text}`);
  }
  return Number(text);
}

const MONTHS: Record<string, string> = {
  jan: "01",
  feb: "02",
  mar: "03",
  apr: "04",
  may: "05",
  jun: "06",
  jul: "07",
  aug: "08",
  sep: "09",
  oct: "10",
  nov: "11",
  dec: "12",
};

function isoDate(year: string, monthName: string, day: string, raw: string, feed: string): string {
  const month = MONTHS[monthName.slice(0, 3).toLowerCase()];
  if (!month) {
    throw new Error(`${feed} XML has an unreadable date: ${raw}`);
  }
  return `${year}-${month}-${day.padStart(2, "0")}`;
}

// --- House ---------------------------------------------------------------

/** `3-Jan-2025` → `2025-01-03`. */
export function parseHouseActionDate(raw: string): string {
  const match = /^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/.exec(raw.trim());
  if (!match) {
    throw new Error(`House XML has an unreadable date: ${raw}`);
  }
  return isoDate(match[3]!, match[2]!, match[1]!, raw, "House");
}

export function parseHouseRollCallXml(xml: string): ParsedFederalRollCall {
  if (!xml.includes("<rollcall-vote>")) {
    throw new Error("House XML has no <rollcall-vote> root");
  }
  // The metadata sits first, so a body cut short anywhere in the member
  // list would otherwise parse as a complete roll call with fewer members.
  if (!/<\/rollcall-vote>\s*$/.test(xml)) {
    throw new Error("House XML does not end with </rollcall-vote>; the file is incomplete");
  }
  const metadata = firstTagBlock(xml, "vote-metadata");
  if (!metadata) {
    throw new Error("House XML is missing <vote-metadata>");
  }
  const sessionText = requireText(metadata, "session", "House");
  const sessionMatch = /^(\d)(?:st|nd|rd|th)$/.exec(sessionText);
  if (!sessionMatch) {
    throw new Error(`House XML <session> is not an ordinal: ${sessionText}`);
  }
  // <yea-total> first appears inside the per-party totals; the chamber-wide
  // numbers sit in <totals-by-vote>. The Speaker election has neither — it
  // tallies by candidate — and is reported as unparseable rather than
  // invented as 0-0.
  const totals = firstTagBlock(metadata, "totals-by-vote");
  if (!totals) {
    throw new Error("House XML has no <totals-by-vote> (not a yea/nay vote, e.g. the Speaker election)");
  }
  return {
    chamber: "house",
    congress: requireInteger(metadata, "congress", "House"),
    session: Number(sessionMatch[1]),
    rollNumber: requireInteger(metadata, "rollcall-num", "House"),
    voteDate: parseHouseActionDate(requireText(metadata, "action-date", "House")),
    // Absent for the Speaker election; `QUORUM` for quorum calls (parsed to
    // null downstream by parseFederalMeasure).
    measureId: firstTagText(metadata, "legis-num"),
    question: requireText(metadata, "vote-question", "House"),
    result: requireText(metadata, "vote-result", "House"),
    yeas: requireInteger(totals, "yea-total", "House"),
    nays: requireInteger(totals, "nay-total", "House"),
    title: firstTagText(metadata, "vote-desc"),
    memberVoteCount: countTag(xml, "recorded-vote"),
  };
}

// --- Senate --------------------------------------------------------------

/** `May 14, 2025,  02:47 PM` → `2025-05-14`. */
export function parseSenateVoteDate(raw: string): string {
  const match = /^([A-Za-z]+) (\d{1,2}), (\d{4})\b/.exec(raw.trim());
  if (!match) {
    throw new Error(`Senate XML has an unreadable date: ${raw}`);
  }
  return isoDate(match[3]!, match[1]!, match[2]!, raw, "Senate");
}

export function parseSenateRollCallXml(xml: string): ParsedFederalRollCall {
  if (!xml.includes("<roll_call_vote>")) {
    throw new Error("Senate XML has no <roll_call_vote> root");
  }
  if (!/<\/roll_call_vote>\s*$/.test(xml)) {
    throw new Error("Senate XML does not end with </roll_call_vote>; the file is incomplete");
  }
  // Everything before <members> is the vote header; <count> holds the
  // chamber-wide tally.
  const header = xml.split("<members>")[0]!;
  const count = firstTagBlock(header, "count");
  if (!count) {
    throw new Error("Senate XML is missing <count>");
  }
  return {
    chamber: "senate",
    congress: requireInteger(header, "congress", "Senate"),
    session: requireInteger(header, "session", "Senate"),
    rollNumber: requireInteger(header, "vote_number", "Senate"),
    voteDate: parseSenateVoteDate(requireText(header, "vote_date", "Senate")),
    measureId: firstTagText(header, "document_name"),
    question: requireText(header, "question", "Senate"),
    result: requireText(header, "vote_result", "Senate"),
    yeas: requireInteger(count, "yeas", "Senate"),
    nays: requireInteger(count, "nays", "Senate"),
    title: firstTagText(header, "vote_title"),
    memberVoteCount: countTag(xml, "member"),
  };
}

export function parseFederalRollCallXml(chamber: LegislativeVoteChamber, xml: string): ParsedFederalRollCall {
  return chamber === "house" ? parseHouseRollCallXml(xml) : parseSenateRollCallXml(xml);
}

// --- Fetch ---------------------------------------------------------------

export type FetchFn = (input: string, init?: RequestInit) => Promise<Response>;

export type FederalRollCallFetchResult = { status: "ok"; body: string } | { status: "missing" };

export type FederalRollCallFetchOptions = {
  fetchFn?: FetchFn;
  timeoutMs?: number;
};

const DEFAULT_TIMEOUT_MS = 30_000;
// The Senate answers an unknown vote number with a 301 to this fixed page
// (verified live 2026-08-23); any other redirect is unexpected and is
// surfaced as an error rather than read as "no such vote".
export const SENATE_VOTE_NOT_AVAILABLE_URL = "https://www.senate.gov/legislative/roll-call-vote-not-available.htm";
const USER_AGENT = "voteapp-rollcall-import (+https://electionssimplified.com)";

/**
 * One GET. A 404 (House) or the Senate's not-available redirect means the
 * roll call does not exist; any other non-200, including an unexpected
 * redirect, is an error the caller may retry.
 */
export async function fetchFederalRollCallXml(
  url: string,
  options: FederalRollCallFetchOptions = {}
): Promise<FederalRollCallFetchResult> {
  const fetchFn = options.fetchFn ?? globalThis.fetch?.bind(globalThis);
  if (!fetchFn) {
    throw new Error("global fetch is unavailable for roll-call XML fetch");
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  try {
    const response = await fetchFn(url, {
      method: "GET",
      redirect: "manual",
      headers: { accept: "text/xml,application/xml;q=0.9,*/*;q=0.8", "user-agent": USER_AGENT },
      signal: controller.signal,
    });
    if (response.status === 404) {
      return { status: "missing" };
    }
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("location") ?? "";
      if (location === SENATE_VOTE_NOT_AVAILABLE_URL) {
        return { status: "missing" };
      }
      throw new Error(`HTTP ${response.status} for ${url} redirects to ${location || "(no location)"}`);
    }
    if (response.status !== 200) {
      throw new Error(`HTTP ${response.status} for ${url}`);
    }
    return { status: "ok", body: await response.text() };
  } finally {
    clearTimeout(timeout);
  }
}

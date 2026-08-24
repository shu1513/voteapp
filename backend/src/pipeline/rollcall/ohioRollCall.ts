import { createHash } from "node:crypto";

import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// The Ohio LIS API (search-prod.lis.state.oh.us), the state pilot's source
// (docs/plans/roll-call-vote-import.md §5 phase 3). One undocumented but
// official JSON feed per bill lists every action, and floor-vote actions
// carry the per-member `yeas[]`/`nays[]` as lpids (`sen_wilson_steve_1`).
// Probed live 2026-08-23: no auth, trailing slash canonical (the API 301s
// the slashless spelling to it), host is `state.oh.us` so the record
// source policy lists it as government.
//
// Ohio prints NO roll-call numbers. The surrogate roll_number stored on
// legislative_votes is the vote action's own `occurred` timestamp in epoch
// seconds — deterministic from the source, unique per chamber (two floor
// votes of one chamber never share a second), int4-safe until 2038, and
// decodable by eye. The fetcher still guards against a collision.

export const OHIO_JURISDICTION = "OH";

// Bill types with floor passage votes we keep: bills and joint resolutions,
// the same cut the federal filter makes. Simple and concurrent resolutions
// (hr/sr/hcr/scr) are excluded by default per plan §1.
export const OHIO_KEPT_MEASURE_TYPES = ["hb", "sb", "hjr", "sjr"] as const;
const OHIO_MEASURE_TYPES = ["hb", "sb", "hjr", "sjr", "hcr", "scr", "hr", "sr"] as const;
export type OhioMeasureType = (typeof OHIO_MEASURE_TYPES)[number];

export type OhioMeasure = { type: OhioMeasureType; number: string };

/** `hb96` (the API's `number` field) → its type and digits; null for anything else. */
export function parseOhioBillNumber(raw: string): OhioMeasure | null {
  const match = /^(hcr|hjr|hb|hr|scr|sjr|sb|sr)(\d+)$/.exec(raw.trim().toLowerCase());
  if (!match) {
    return null;
  }
  return { type: match[1] as OhioMeasureType, number: match[2]! };
}

/**
 * The measure_id spelling stored on legislative_votes and named by
 * judgments: `HB 96`, `SJR 3`. Ohio measures do not parse as federal ones,
 * so the judge's measure check falls back to an exact string compare —
 * both sides must use exactly this spelling.
 */
export function ohioMeasureId(measure: OhioMeasure): string {
  return `${measure.type.toUpperCase()} ${measure.number}`;
}

export function ohioApiBase(generalAssembly: number): string {
  return `https://search-prod.lis.state.oh.us/api/v2/general_assembly_${generalAssembly}`;
}

/** Machine-readable per-bill actions feed; the records' eventual source_url. */
export function ohioActionsUrl(generalAssembly: number, billNumber: string): string {
  return `${ohioApiBase(generalAssembly)}/legislation/${billNumber}/actions/`;
}

export function ohioLegislationListUrl(generalAssembly: number): string {
  return `${ohioApiBase(generalAssembly)}/legislation/`;
}

export function ohioLegislatorsUrl(generalAssembly: number): string {
  return `${ohioApiBase(generalAssembly)}/legislators/`;
}

/** Human bill page (display_url / bill_url). Serves 200; its TLS chain omits an intermediate, so it is never a source_url. */
export function ohioDisplayUrl(generalAssembly: number, billNumber: string): string {
  return `https://www.legislature.ohio.gov/legislation/${generalAssembly}/${billNumber}`;
}

// One element of the actions feed, loosely typed: only the fields the
// pipeline reads are named, and each is checked where it is used.
export type OhioAction = {
  action_code?: unknown;
  action?: unknown;
  chamber?: unknown;
  occurred?: unknown;
  session_day?: unknown;
  date?: unknown;
  cmte_name?: unknown;
  committee?: unknown;
  yeas?: unknown;
  nays?: unknown;
  result?: unknown;
};

export function ohioActionChamber(action: OhioAction): LegislativeVoteChamber {
  if (action.chamber === "House") {
    return "house";
  }
  if (action.chamber === "Senate") {
    return "senate";
  }
  throw new Error(`Ohio action chamber is not House or Senate: ${JSON.stringify(action.chamber)}`);
}

/** `day045_s_20250611` → `2025-06-11`. The journal's session day is the official vote date. */
export function parseOhioSessionDay(raw: string): string {
  const match = /^day\d+_[hs]_(\d{4})(\d{2})(\d{2})$/.exec(raw.trim());
  if (!match) {
    throw new Error(`Ohio session_day is not day<N>_<h|s>_<YYYYMMDD>: ${raw}`);
  }
  return `${match[1]}-${match[2]}-${match[3]}`;
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

export function ohioActionVoteDate(action: OhioAction): string {
  if (typeof action.session_day === "string" && action.session_day.trim().length > 0) {
    return parseOhioSessionDay(action.session_day);
  }
  if (typeof action.date === "string" && ISO_DATE.test(action.date)) {
    return action.date;
  }
  throw new Error(`Ohio action has neither a session_day nor an ISO date: ${JSON.stringify(action).slice(0, 200)}`);
}

// occurred carries an explicit offset (`2025-06-11T13:05:12-04:00`), so the
// epoch math needs no timezone table.
const OCCURRED_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2}$/;

/** The surrogate roll number: the action's `occurred` timestamp in epoch seconds. */
export function ohioRollNumber(action: OhioAction): number {
  const occurred = action.occurred;
  if (typeof occurred !== "string" || !OCCURRED_PATTERN.test(occurred.trim())) {
    throw new Error(`Ohio action occurred is not an offset timestamp: ${JSON.stringify(occurred)}`);
  }
  const ms = Date.parse(occurred.trim());
  const seconds = Math.floor(ms / 1000);
  // legislative_votes.roll_number is a positive int4.
  if (!Number.isSafeInteger(seconds) || seconds <= 0 || seconds > 2_147_000_000) {
    throw new Error(`Ohio action occurred is outside the storable range: ${occurred}`);
  }
  return seconds;
}

function lpidList(value: unknown, field: string): string[] {
  if (value === undefined || value === null) {
    return [];
  }
  if (!Array.isArray(value) || value.some((element) => typeof element !== "string" || element.trim().length === 0)) {
    throw new Error(`Ohio action ${field} is not an array of lpids`);
  }
  return (value as string[]).map((lpid) => lpid.trim());
}

/** True when the action records member positions at all. */
export function ohioActionHasVotes(action: OhioAction): boolean {
  return (Array.isArray(action.yeas) && action.yeas.length > 0) || (Array.isArray(action.nays) && action.nays.length > 0);
}

/**
 * The two member lists, checked: every lpid a non-empty string, and no
 * lpid on both sides (a member cannot vote yea and nay at once).
 */
export function ohioActionMemberVotes(action: OhioAction): { yeas: string[]; nays: string[] } {
  const yeas = lpidList(action.yeas, "yeas");
  const nays = lpidList(action.nays, "nays");
  const yeaSet = new Set(yeas);
  if (yeaSet.size !== yeas.length || new Set(nays).size !== nays.length) {
    throw new Error("Ohio action lists an lpid twice on one side");
  }
  const both = nays.find((lpid) => yeaSet.has(lpid));
  if (both) {
    throw new Error(`Ohio action lists ${both} as both yea and nay`);
  }
  return { yeas, nays };
}

export type OhioQuestionClass = "passage" | "concurrence" | "conference_report" | "veto_override";

export type OhioActionClassification = {
  isFloorVote: boolean | null;
  questionClass: OhioQuestionClass | null;
  reason: string;
};

// Ohio's action codes are structured, so the classifier keys on them, not
// on the free-text `action`. Vocabulary measured on a 30-bill GA-136
// sample, 2026-08-23:
//   pass_300    "Passed" / "Passed - Amended"                → passage
//   msg_507     "Concurred in Senate amendments"             → concurrence
//   concur_606  "Concurred in House amendments"              → concurrence
//   confer_712/713  "Conference report agreed to"            → conference report
//   govern_858  "Item passed notwithstanding objections of the Governor"
//                                                            → veto override
//   msg_506 / concur_608  "Refused to concur ..."            → floor but procedural, excluded
//   crpt_301    "Reported ..." (+ cmte_name)                 → committee, excluded
// NOTE: a conference-report FLOOR vote can carry a cmte_name (the
// conference committee itself), so cmte_name alone must never decide
// committee-ness; the action code does. Any vote-bearing code outside this
// vocabulary classifies as unknown (is_floor_vote = null = never queued)
// and is surfaced in the fetch report for a human.
const FLOOR_VOTE_CODES: Readonly<Record<string, OhioQuestionClass>> = {
  pass_300: "passage",
  msg_507: "concurrence",
  concur_606: "concurrence",
  confer_712: "conference_report",
  confer_713: "conference_report",
  govern_858: "veto_override",
};
const EXCLUDED_FLOOR_CODES = new Set(["msg_506", "concur_608"]);
const COMMITTEE_CODE_PREFIXES = ["crpt_", "refer_"];

export function classifyOhioVoteAction(input: { actionCode: string; measure: OhioMeasure }): OhioActionClassification {
  const code = input.actionCode;
  if (COMMITTEE_CODE_PREFIXES.some((prefix) => code.startsWith(prefix))) {
    return { isFloorVote: false, questionClass: null, reason: `committee:${code}` };
  }
  if (EXCLUDED_FLOOR_CODES.has(code)) {
    return { isFloorVote: false, questionClass: null, reason: `excluded_question:${code}` };
  }
  const questionClass = FLOOR_VOTE_CODES[code];
  if (questionClass === undefined) {
    return { isFloorVote: null, questionClass: null, reason: `unknown_action:${code}` };
  }
  if (!(OHIO_KEPT_MEASURE_TYPES as readonly string[]).includes(input.measure.type)) {
    return { isFloorVote: false, questionClass, reason: `excluded_measure:${input.measure.type}` };
  }
  return { isFloorVote: true, questionClass, reason: `kept:${questionClass}` };
}

/**
 * Preflight for one bill's vote actions: the `chamber:date` keys that hold
 * MORE than one kept floor vote. The per-bill actions URL is the record's
 * source_url and cannot tell two same-day floor votes apart, so the
 * fetcher stores NEITHER member of a colliding pair (rejecting only the
 * second would leave the first — equally indistinguishable — in the
 * queue). Actions this preflight cannot read (bad chamber, bad date) are
 * skipped here; the per-action loop reports them itself.
 */
export function ohioKeptFloorDayCollisions(voteActions: readonly OhioAction[], measure: OhioMeasure): Set<string> {
  const keptPerDay = new Map<string, number>();
  for (const action of voteActions) {
    const actionCode = typeof action.action_code === "string" ? action.action_code : "";
    if (classifyOhioVoteAction({ actionCode, measure }).isFloorVote !== true) {
      continue;
    }
    try {
      const key = `${ohioActionChamber(action)}:${ohioActionVoteDate(action)}`;
      keptPerDay.set(key, (keptPerDay.get(key) ?? 0) + 1);
    } catch {
      continue;
    }
  }
  return new Set([...keptPerDay.entries()].filter(([, count]) => count > 1).map(([key]) => key));
}

const COMMITTEE_PREFLIGHT_PREFIXES = ["crpt_", "refer_"];

/**
 * Preflight for one bill's vote actions: the `chamber:roll` surrogate keys
 * shared by MORE than one storable (non-committee) action. Ohio's surrogate
 * roll number is the occurred second, so two distinct same-bill actions
 * stamped the same second would silently fold into ONE legislative_votes
 * row and one evidence file — the upsert would read the second as a
 * republication of the first. The fetcher stores NEITHER. Actions this
 * preflight cannot read are skipped here; the per-action loop reports them.
 */
export function ohioDuplicateRollKeys(voteActions: readonly OhioAction[]): Set<string> {
  const owners = new Map<string, number>();
  for (const action of voteActions) {
    const actionCode = typeof action.action_code === "string" ? action.action_code : "";
    if (COMMITTEE_PREFLIGHT_PREFIXES.some((prefix) => actionCode.startsWith(prefix))) {
      continue;
    }
    try {
      const key = `${ohioActionChamber(action)}:${ohioRollNumber(action)}`;
      owners.set(key, (owners.get(key) ?? 0) + 1);
    } catch {
      continue;
    }
  }
  return new Set([...owners.entries()].filter(([, count]) => count > 1).map(([key]) => key));
}

/**
 * The stored hash pins the ACTION ELEMENT as fetched, not the whole feed
 * response: a bill's actions array keeps growing after our fetch (the
 * governor signs, a journal correction lands), and re-hashing the whole
 * response would flag every already-approved vote as an approved_conflict
 * each time a sibling action appears. JSON.parse/stringify round-trips are
 * byte-stable in Node for one input, so fetcher and importer agree.
 */
export function ohioActionSha256(action: unknown): string {
  return createHash("sha256").update(JSON.stringify(action)).digest("hex");
}

// One evidence file per stored vote, the state analog of the federal
// per-roll XML: `oh-<chamber>-<ga>-roll<epoch>.json`.
export const OHIO_EVIDENCE_FILE_PATTERN = /^oh-(house|senate)-(\d+)-roll(\d+)\.json$/;

export function ohioEvidenceFileName(chamber: LegislativeVoteChamber, generalAssembly: number, rollNumber: number): string {
  return `oh-${chamber}-${generalAssembly}-roll${rollNumber}.json`;
}

export type OhioVoteEvidence = {
  jurisdiction: typeof OHIO_JURISDICTION;
  generalAssembly: number;
  chamber: LegislativeVoteChamber;
  rollNumber: number;
  bill: string;
  measureId: string;
  machineUrl: string;
  fetchedAt: string;
  // The action element verbatim as the feed returned it; source_sha256 is
  // ohioActionSha256 over exactly this value.
  action: unknown;
};

/** Reads and checks one evidence file's JSON against its file name. */
export function parseOhioVoteEvidence(
  raw: unknown,
  expected: { chamber: LegislativeVoteChamber; generalAssembly: number; rollNumber: number }
): OhioVoteEvidence {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error("Ohio evidence file is not an object");
  }
  const evidence = raw as Record<string, unknown>;
  if (evidence.jurisdiction !== OHIO_JURISDICTION) {
    throw new Error(`Ohio evidence jurisdiction is ${JSON.stringify(evidence.jurisdiction)}`);
  }
  for (const [field, value] of [
    ["generalAssembly", expected.generalAssembly],
    ["chamber", expected.chamber],
    ["rollNumber", expected.rollNumber],
  ] as const) {
    if (evidence[field] !== value) {
      throw new Error(`Ohio evidence ${field} is ${JSON.stringify(evidence[field])}, but the file name says ${value}`);
    }
  }
  for (const field of ["bill", "measureId", "machineUrl", "fetchedAt"] as const) {
    if (typeof evidence[field] !== "string" || (evidence[field] as string).trim().length === 0) {
      throw new Error(`Ohio evidence ${field} is missing`);
    }
  }
  if (typeof evidence.action !== "object" || evidence.action === null) {
    throw new Error("Ohio evidence action is missing");
  }
  return evidence as OhioVoteEvidence;
}

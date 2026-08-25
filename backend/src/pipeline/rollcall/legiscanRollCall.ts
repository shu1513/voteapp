import { createHash } from "node:crypto";

import type { LegislativeVoteChamber } from "./legislativeVotes.js";
import type { LegiscanQuestionClass, LegiscanStateConfig } from "./legiscanStateConfigs.js";

// LegiScan bulk datasets, the phase-4 state-rollout source
// (docs/plans/roll-call-vote-import.md §5 phase 4). One weekly ZIP per
// session holds every getBill / getRollCall / getPerson payload as an
// individual JSON file; the pipeline reads an EXTRACTED dataset directory —
// there is no live-API code, because bulk is the plan of record and the
// scripts must run whole-session surveys without burning the 30k-query
// budget. Schema pinned against the LegiScan API User Manual v1.91
// (revision 2025-03-17), Data Dictionary sections `bill`, `roll_call`,
// `person`; every field this module reads is re-checked at parse time, so a
// schema drift fails a roll call loudly instead of storing bad data.
//
// Unlike Ohio, LegiScan HAS a real roll identifier: `roll_call_id`, unique
// across the whole national corpus (~1.5M today, int4-safe), and a public
// per-roll page (`legiscan.com/<ST>/rollcall/<bill>/id/<roll_call_id>`)
// listing every member's position — the record's source_url (the domain is
// on the founding source-policy allowlist). So no surrogate roll numbers
// and no same-day-collision machinery.

// LegiScan bill_type values we keep: bills, joint resolutions, and the two
// constitutional-amendment instruments — the same cut Ohio and the federal
// filter make (simple/concurrent resolutions, memorials, commendations,
// petitions etc. are excluded by default per plan §1).
export const LEGISCAN_KEPT_BILL_TYPES: readonly string[] = ["B", "JR", "JRCA", "CA"];

// Tally thresholds of the floor-vs-committee check. LegiScan has no
// committee flag, so committee-ness is inferred: `total` counts every
// listed member (yea + nay + nv + absent), which on a floor vote is close
// to the chamber size and on a committee vote is the committee's dozen.
// A kept-desc vote below the floor line, or an unknown-desc vote between
// the two lines, classifies null (= surfaced, never queued, never silently
// dropped) — the gray zone exists so a state that lists only voting
// members cannot slip a floor vote into the committee bucket unseen.
export const LEGISCAN_FLOOR_MIN_RATIO = 0.6;
export const LEGISCAN_COMMITTEE_MAX_RATIO = 0.5;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ---------------------------------------------------------------------------
// Dataset files

// Which payload a dataset JSON file holds, decided by its envelope key —
// not by its directory name, so an archive-layout change cannot misroute a
// file. Anything else (hash manifests, future payload kinds) is "other".
export type LegiscanDatasetPayload =
  | { kind: "bill"; bill: Record<string, unknown> }
  | { kind: "vote"; rollCall: Record<string, unknown> }
  | { kind: "person"; person: Record<string, unknown> }
  | { kind: "other" };

export function classifyLegiscanDatasetFile(raw: unknown): LegiscanDatasetPayload {
  if (!isRecord(raw)) {
    return { kind: "other" };
  }
  if (isRecord(raw.bill)) {
    return { kind: "bill", bill: raw.bill };
  }
  if (isRecord(raw.roll_call)) {
    return { kind: "vote", rollCall: raw.roll_call };
  }
  if (isRecord(raw.person)) {
    return { kind: "person", person: raw.person };
  }
  return { kind: "other" };
}

// ---------------------------------------------------------------------------
// Measures

/**
 * The measure_id spelling stored on legislative_votes and named by
 * judgments: LegiScan's compact `bill_number` (`HB1`, `SB0544`) becomes
 * `HB 1` / `SB 544` — the Ohio pilot's spelling. State measures do not
 * parse as federal ones, so the judge's measure check falls back to an
 * exact string compare; both sides must use exactly this spelling.
 */
export function formatLegiscanMeasureId(billNumber: string): string {
  const compact = billNumber.replace(/\s+/g, "");
  const match = /^([A-Za-z]+)0*(\d+)$/.exec(compact);
  if (!match) {
    throw new Error(`LegiScan bill_number is not <letters><digits>: ${billNumber}`);
  }
  return `${match[1]!.toUpperCase()} ${match[2]}`;
}

/** The public per-roll page; the fallback when the bill feed carries no vote url. */
export function legiscanRollCallPageUrl(state: string, billNumber: string, rollCallId: number): string {
  return `https://legiscan.com/${state.toUpperCase()}/rollcall/${billNumber.replace(/\s+/g, "")}/id/${rollCallId}`;
}

// ---------------------------------------------------------------------------
// Bills

// What the fetcher needs from one dataset bill file.
export type LegiscanBillSummary = {
  billId: number;
  // Verbatim feed spelling (`SB0544`).
  billNumber: string;
  billType: string;
  measureId: string;
  sessionId: number;
  state: string;
  title: string;
  // Official state bill page (bill_url when present) and the LegiScan bill
  // page (the fallback).
  stateLink: string | null;
  legiscanUrl: string | null;
  // Per-roll LegiScan page URLs from the bill's votes[] summary.
  voteUrlsByRollCallId: ReadonlyMap<number, string>;
};

function readPositiveInt(record: Record<string, unknown>, key: string, where: string): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${where}: ${key} is not a positive integer`);
  }
  return value;
}

function readNonNegativeInt(record: Record<string, unknown>, key: string, where: string): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${where}: ${key} is not a non-negative integer`);
  }
  return value;
}

function readString(record: Record<string, unknown>, key: string, where: string): string {
  const value = record[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${where}: ${key} is missing or not a string`);
  }
  return value.trim();
}

function readOptionalUrl(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

export function parseLegiscanBill(raw: Record<string, unknown>): LegiscanBillSummary {
  const billId = readPositiveInt(raw, "bill_id", "LegiScan bill");
  const where = `LegiScan bill ${billId}`;
  const billNumber = readString(raw, "bill_number", where);
  if (!isRecord(raw.session)) {
    throw new Error(`${where}: session is not an object`);
  }
  const votes = raw.votes === undefined || raw.votes === null ? [] : raw.votes;
  if (!Array.isArray(votes)) {
    throw new Error(`${where}: votes is not an array`);
  }
  const voteUrlsByRollCallId = new Map<number, string>();
  for (const vote of votes) {
    if (!isRecord(vote)) {
      throw new Error(`${where}: votes[] element is not an object`);
    }
    const rollCallId = readPositiveInt(vote, "roll_call_id", `${where} votes[]`);
    const url = readOptionalUrl(vote, "url");
    if (url !== null) {
      voteUrlsByRollCallId.set(rollCallId, url);
    }
  }
  return {
    billId,
    billNumber,
    billType: readString(raw, "bill_type", where),
    measureId: formatLegiscanMeasureId(billNumber),
    sessionId: readPositiveInt(raw.session, "session_id", `${where} session`),
    state: readString(raw, "state", where).toUpperCase(),
    title: readString(raw, "title", where),
    stateLink: readOptionalUrl(raw, "state_link"),
    legiscanUrl: readOptionalUrl(raw, "url"),
    voteUrlsByRollCallId,
  };
}

// ---------------------------------------------------------------------------
// Roll calls

// LegiScan vote_id values (manual, Static Values): 1 Yea, 2 Nay,
// 3 Not Voting / abstain, 4 Absent / excused. 3 and 4 mean the member took
// no position — no record, same as the federal Present / Not Voting rows.
export const LEGISCAN_VOTE_YEA = 1;
export const LEGISCAN_VOTE_NAY = 2;
export const LEGISCAN_VOTE_NV = 3;
export const LEGISCAN_VOTE_ABSENT = 4;

export type LegiscanRollCall = {
  rollCallId: number;
  billId: number;
  // ISO date, YYYY-MM-DD.
  date: string;
  desc: string;
  yea: number;
  nay: number;
  nv: number;
  absent: number;
  total: number;
  passed: boolean;
  chamber: LegislativeVoteChamber;
  votes: readonly { peopleId: number; voteId: number }[];
};

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Reads and checks one dataset roll_call element: every field the pipeline
 * uses present and well-formed, no member listed twice, every vote_id in
 * the documented vocabulary, and the per-member list consistent with the
 * summary tallies — a feed defect fails the roll call, it never skips
 * members silently.
 */
export function parseLegiscanRollCall(raw: Record<string, unknown>): LegiscanRollCall {
  const rollCallId = readPositiveInt(raw, "roll_call_id", "LegiScan roll_call");
  const where = `LegiScan roll call ${rollCallId}`;
  // legislative_votes.roll_number is a positive int4.
  if (rollCallId > 2_147_000_000) {
    throw new Error(`${where}: roll_call_id is outside the storable range`);
  }
  const date = readString(raw, "date", where);
  if (!ISO_DATE.test(date)) {
    throw new Error(`${where}: date is not YYYY-MM-DD: ${date}`);
  }
  const chamberRaw = readString(raw, "chamber", where);
  if (chamberRaw !== "H" && chamberRaw !== "S") {
    throw new Error(`${where}: chamber is not H or S: ${chamberRaw}`);
  }
  if (raw.passed !== 0 && raw.passed !== 1 && raw.passed !== true && raw.passed !== false) {
    throw new Error(`${where}: passed is not a 0/1 flag`);
  }
  if (!Array.isArray(raw.votes)) {
    throw new Error(`${where}: votes is not an array`);
  }
  const votes: { peopleId: number; voteId: number }[] = [];
  const seen = new Set<number>();
  const counts: Record<number, number> = {
    [LEGISCAN_VOTE_YEA]: 0,
    [LEGISCAN_VOTE_NAY]: 0,
    [LEGISCAN_VOTE_NV]: 0,
    [LEGISCAN_VOTE_ABSENT]: 0,
  };
  for (const [index, element] of raw.votes.entries()) {
    if (!isRecord(element)) {
      throw new Error(`${where}: votes[${index}] is not an object`);
    }
    const peopleId = readPositiveInt(element, "people_id", `${where} votes[${index}]`);
    const voteId = readPositiveInt(element, "vote_id", `${where} votes[${index}]`);
    if (voteId !== LEGISCAN_VOTE_YEA && voteId !== LEGISCAN_VOTE_NAY && voteId !== LEGISCAN_VOTE_NV && voteId !== LEGISCAN_VOTE_ABSENT) {
      throw new Error(`${where}: votes[${index}] has unknown vote_id ${voteId}`);
    }
    if (seen.has(peopleId)) {
      throw new Error(`${where}: lists people_id ${peopleId} twice`);
    }
    seen.add(peopleId);
    counts[voteId] = (counts[voteId] ?? 0) + 1;
    votes.push({ peopleId, voteId });
  }
  const rollCall: LegiscanRollCall = {
    rollCallId,
    billId: readPositiveInt(raw, "bill_id", where),
    date,
    desc: readString(raw, "desc", where),
    yea: readNonNegativeInt(raw, "yea", where),
    nay: readNonNegativeInt(raw, "nay", where),
    nv: readNonNegativeInt(raw, "nv", where),
    absent: readNonNegativeInt(raw, "absent", where),
    total: readNonNegativeInt(raw, "total", where),
    passed: raw.passed === 1 || raw.passed === true,
    chamber: chamberRaw === "H" ? "house" : "senate",
    votes,
  };
  // An EMPTY member list beside non-zero tallies is a real publication
  // state, not a feed defect: the Texas Senate prints summary tallies with
  // no positions on non-record votes (2,701 of TX 89R's 9,726 roll calls,
  // measured 2026-08-24, zero of them a genuine summary/list disagreement).
  // The summary then stands alone with nothing to cross-check; such a roll
  // is UNRECORDED (votes.length === 0) — the fetcher skips it, since
  // per-member positions are the whole point of the import. The
  // cross-checks below run whenever positions ARE listed, so a partial or
  // contradictory list still fails loudly.
  if (votes.length > 0) {
    for (const [field, voteId] of [
      ["yea", LEGISCAN_VOTE_YEA],
      ["nay", LEGISCAN_VOTE_NAY],
      ["nv", LEGISCAN_VOTE_NV],
      ["absent", LEGISCAN_VOTE_ABSENT],
    ] as const) {
      if (rollCall[field] !== counts[voteId]) {
        throw new Error(`${where}: ${field} says ${rollCall[field]} but the member list holds ${counts[voteId]}`);
      }
    }
    if (rollCall.total !== votes.length) {
      throw new Error(`${where}: total says ${rollCall.total} but the member list holds ${votes.length}`);
    }
  }
  return rollCall;
}

/** The two member lists (people_ids) plus the no-position counts. */
export function legiscanMemberVotes(rollCall: LegiscanRollCall): {
  yeas: number[];
  nays: number[];
  notVoting: number;
  absent: number;
} {
  return {
    yeas: rollCall.votes.filter((vote) => vote.voteId === LEGISCAN_VOTE_YEA).map((vote) => vote.peopleId),
    nays: rollCall.votes.filter((vote) => vote.voteId === LEGISCAN_VOTE_NAY).map((vote) => vote.peopleId),
    notVoting: rollCall.nv,
    absent: rollCall.absent,
  };
}

// ---------------------------------------------------------------------------
// Classification

export type LegiscanRollCallClassification = {
  isFloorVote: boolean | null;
  questionClass: LegiscanQuestionClass | null;
  reason: string;
};

/**
 * The queue filter (plan §1, LegiScan variant): the state's desc patterns
 * decide the question class, the tally-vs-chamber-size check decides
 * floor-ness. Excluded patterns run first (the specific carve-outs), then
 * kept patterns; an unknown desc with a committee-sized tally is rejected
 * as committee (never stored, counted in the report, like Ohio's crpt_*
 * codes), and everything unresolved classifies null — stored, surfaced for
 * a human, never queued.
 */
export function classifyLegiscanRollCall(input: {
  desc: string;
  total: number;
  chamber: LegislativeVoteChamber;
  billType: string;
  config: LegiscanStateConfig;
}): LegiscanRollCallClassification {
  if (!LEGISCAN_KEPT_BILL_TYPES.includes(input.billType)) {
    return { isFloorVote: false, questionClass: null, reason: `excluded_measure:${input.billType}` };
  }
  const normalized = input.desc.toLowerCase().replace(/\s+/g, " ").trim();
  if (input.config.excludedQuestions.some((pattern) => pattern.test(normalized))) {
    return { isFloorVote: false, questionClass: null, reason: "excluded_question" };
  }
  const kept = input.config.keptQuestions.find((rule) => rule.pattern.test(normalized));
  const size = input.config.chamberSizes[input.chamber];
  if (kept) {
    if (size === undefined) {
      return { isFloorVote: null, questionClass: kept.questionClass, reason: `unknown_chamber_size:${input.chamber}` };
    }
    if (input.total >= size * LEGISCAN_FLOOR_MIN_RATIO) {
      return { isFloorVote: true, questionClass: kept.questionClass, reason: `kept:${kept.questionClass}` };
    }
    return { isFloorVote: null, questionClass: kept.questionClass, reason: `kept_small_tally:${input.total}/${size}` };
  }
  if (size !== undefined && input.total < size * LEGISCAN_COMMITTEE_MAX_RATIO) {
    return { isFloorVote: false, questionClass: null, reason: `committee_tally:${input.total}/${size}` };
  }
  return { isFloorVote: null, questionClass: null, reason: "unknown_question" };
}

// ---------------------------------------------------------------------------
// Evidence

/**
 * The stored hash pins the roll_call ELEMENT as the dataset file held it —
 * the vote is final once printed, but a re-downloaded dataset re-serializes
 * every file, so the pin is on the element, not the file bytes.
 * JSON.parse/stringify round-trips are byte-stable in Node for one input,
 * so fetcher and importer agree.
 */
export function legiscanRollCallSha256(rollCallElement: unknown): string {
  return createHash("sha256").update(JSON.stringify(rollCallElement)).digest("hex");
}

// One evidence file per stored vote: `ls-<st>-<chamber>-<sessionId>-roll<rollCallId>.json`.
export const LEGISCAN_EVIDENCE_FILE_PATTERN = /^ls-([a-z]{2})-(house|senate)-(\d+)-roll(\d+)\.json$/;

export function legiscanEvidenceFileName(
  state: string,
  chamber: LegislativeVoteChamber,
  sessionId: number,
  rollCallId: number
): string {
  return `ls-${state.toLowerCase()}-${chamber}-${sessionId}-roll${rollCallId}.json`;
}

export type LegiscanVoteEvidence = {
  jurisdiction: string;
  sessionId: number;
  chamber: LegislativeVoteChamber;
  rollNumber: number;
  // Verbatim feed spelling (`SB0544`) and the stored spelling (`SB 544`).
  bill: string;
  measureId: string;
  machineUrl: string;
  fetchedAt: string;
  // The roll_call element verbatim as the dataset file held it;
  // source_sha256 is legiscanRollCallSha256 over exactly this value.
  rollCall: unknown;
};

/** Reads and checks one evidence file's JSON against its file name. */
export function parseLegiscanVoteEvidence(
  raw: unknown,
  expected: { jurisdiction: string; chamber: LegislativeVoteChamber; sessionId: number; rollNumber: number }
): LegiscanVoteEvidence {
  if (!isRecord(raw)) {
    throw new Error("LegiScan evidence file is not an object");
  }
  for (const [field, value] of [
    ["jurisdiction", expected.jurisdiction],
    ["sessionId", expected.sessionId],
    ["chamber", expected.chamber],
    ["rollNumber", expected.rollNumber],
  ] as const) {
    if (raw[field] !== value) {
      throw new Error(`LegiScan evidence ${field} is ${JSON.stringify(raw[field])}, but the file name says ${value}`);
    }
  }
  for (const field of ["bill", "measureId", "machineUrl", "fetchedAt"] as const) {
    if (typeof raw[field] !== "string" || (raw[field] as string).trim().length === 0) {
      throw new Error(`LegiScan evidence ${field} is missing`);
    }
  }
  if (!isRecord(raw.rollCall)) {
    throw new Error("LegiScan evidence rollCall is missing");
  }
  return raw as LegiscanVoteEvidence;
}

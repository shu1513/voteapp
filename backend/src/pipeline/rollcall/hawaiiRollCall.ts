import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import type { LegiscanPerson } from "./legiscanMemberResolver.js";
import { classifyLegiscanDatasetFile, parseLegiscanBill, type LegiscanBillSummary } from "./legiscanRollCall.js";
import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Hawaii's own roll-call source (docs/plans/roll-call-vote-import.md §5,
// the second state with a dedicated pipeline after Ohio).
//
// LegiScan's Hawaii vote feed holds ONLY committee votes — measured over
// every roll call in the 2025 (2,826) and 2026 (3,909) datasets on
// 2026-09-09, the largest tally in either is a 17-member conference
// committee against 51 House / 25 Senate seats. The floor votes exist, but
// only as TEXT in each bill's `history[]` action lines, and Hawaii's journal
// names only the members who did NOT vote aye:
//
//   Senate: "Passed Final Reading, as amended (CD 1). Ayes, 23; Aye(s) with
//            reservations: Senator(s) Rhoads. Noes, 1 (Senator(s) DeCorte).
//            Excused, 1 (Senator(s) McKelvey)."
//   House:  "Passed Final Reading as amended in CD 1 with Representative(s)
//            Souza voting aye with reservations; Representative(s) Garcia,
//            Pierick voting no (2) and Representative(s) Cochran excused (1)."
//
// So this module reads those lines, resolves the named members against the
// dataset's people file, and RECONSTRUCTS the aye list as every sitting
// member of the chamber who is not named as a no or as excused. That
// inference is checked two ways before anything is stored: the three lists
// must add up to the chamber's seat count, and where the journal prints an
// aye count (the Senate does) the reconstruction must equal it.
//
// The identity layer is the same people_id crosswalk the LegiScan states
// use (legiscanMemberResolver.ts), because the people file IS LegiScan's.
// Hawaii must never be registered in legiscanStateConfigs.ts — see
// JURISDICTIONS_WITH_DEDICATED_PIPELINES there.

export const HAWAII_JURISDICTION = "HI";

export const HAWAII_CHAMBER_SEATS: Readonly<Record<LegislativeVoteChamber, number>> = { house: 51, senate: 25 };

// Bills only. Hawaii proposes constitutional amendments as ordinary bills
// (type B, passed by two thirds), so nothing is lost; CR/R resolutions are
// never law and are excluded, the campaign's standing rule.
export const HAWAII_KEPT_BILL_TYPES: readonly string[] = ["B"];

// ---------------------------------------------------------------------------
// Dataset

export type HawaiiDatasetBill = {
  summary: LegiscanBillSummary;
  // LegiScan's status code (4 = passed into law), report-only: selection
  // reads the bill's own history for the "Act N" line, never this flag.
  status: number | null;
  // The session's first calendar year (`session.year_start`). Hawaii bills
  // carry over within a biennium, so the 2026 dataset's histories also hold
  // the 2025 floor votes the 2025 dataset already carries; a vote is filed
  // under the session it was cast in, so rows dated before this year are
  // skipped (reported as prior_session).
  yearStart: number;
  // The `history[]` array verbatim; every floor vote lives in here.
  history: unknown[];
};

export type HawaiiDataset = {
  billsById: Map<number, HawaiiDatasetBill>;
  people: Record<string, unknown>[];
  fileErrors: { file: string; error: string }[];
};

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Walks an extracted LegiScan dataset directory and keeps each bill's
 * summary AND its raw history (the generic reader drops history, and
 * history is Hawaii's whole vote source). Vote files are ignored here —
 * they are committee votes and never floor votes.
 */
export function readHawaiiDataset(datasetDir: string): HawaiiDataset {
  const dataset: HawaiiDataset = { billsById: new Map(), people: [], fileErrors: [] };
  const entries = readdirSync(datasetDir, { recursive: true }) as string[];
  for (const entry of entries.filter((file) => file.endsWith(".json")).sort()) {
    let payload;
    try {
      payload = classifyLegiscanDatasetFile(JSON.parse(readFileSync(join(datasetDir, entry), "utf8")) as unknown);
    } catch (error) {
      dataset.fileErrors.push({ file: entry, error: errorMessage(error) });
      continue;
    }
    if (payload.kind === "bill") {
      try {
        const summary = parseLegiscanBill(payload.bill);
        if (dataset.billsById.has(summary.billId)) {
          throw new Error(`bill_id ${summary.billId} appears in more than one file`);
        }
        const history = payload.bill.history;
        if (!Array.isArray(history)) {
          throw new Error(`bill ${summary.billNumber} has no history array`);
        }
        const status = typeof payload.bill.status === "number" ? payload.bill.status : null;
        const session = payload.bill.session as Record<string, unknown>;
        const yearStart = session.year_start;
        if (typeof yearStart !== "number" || !Number.isSafeInteger(yearStart) || yearStart < 2000) {
          throw new Error(`bill ${summary.billNumber} session has no year_start`);
        }
        dataset.billsById.set(summary.billId, { summary, status, yearStart, history });
      } catch (error) {
        dataset.fileErrors.push({ file: entry, error: errorMessage(error) });
      }
    } else if (payload.kind === "person") {
      dataset.people.push(payload.person);
    }
  }
  return dataset;
}

// ---------------------------------------------------------------------------
// History rows

export type HawaiiHistoryRow = {
  index: number;
  date: string;
  chamber: LegislativeVoteChamber;
  action: string;
};

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

/** One `history[]` element, checked. The row's own `chamber` field (H/S) names the voting chamber. */
export function parseHawaiiHistoryRow(raw: unknown, index: number): HawaiiHistoryRow {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error(`history[${index}] is not an object`);
  }
  const row = raw as Record<string, unknown>;
  const date = row.date;
  if (typeof date !== "string" || !ISO_DATE.test(date)) {
    throw new Error(`history[${index}] date is not YYYY-MM-DD: ${JSON.stringify(date)}`);
  }
  const action = row.action;
  if (typeof action !== "string" || action.trim().length === 0) {
    throw new Error(`history[${index}] action is missing`);
  }
  const chamberRaw = row.chamber;
  if (chamberRaw !== "H" && chamberRaw !== "S") {
    throw new Error(`history[${index}] chamber is ${JSON.stringify(chamberRaw)}, not H or S`);
  }
  return { index, date, chamber: chamberRaw === "H" ? "house" : "senate", action: action.trim() };
}

// ---------------------------------------------------------------------------
// Classification

export type HawaiiQuestionClass = "third_reading" | "final_reading";

export type HawaiiActionClassification = {
  isFloorVote: boolean;
  questionClass: HawaiiQuestionClass | null;
  reason: string;
};

// Vocabulary measured over every history line of both sessions, 2026-09-09.
// Floor passage is spelled four ways:
//   "Passed Third Reading ..."                       (House, no report)
//   "Report adopted; Passed Third Reading ..."       (Senate; also "Report Adopted;")
//   "Passed Final Reading ..."                       (both chambers)
// Everything else that mentions a reading is NOT the chamber's vote on the
// measure: "Passed Second Reading ... placed on the calendar for Third
// Reading" (the House names its second-reading dissenters too), "Reported
// from FIN ... recommending passage on Third Reading" (a committee report),
// "Received notice of passage on Final Reading" (the other chamber's
// message), "Senate agrees with House amendments. Placed on the calendar
// for Final Reading" (scheduling).
const FLOOR_PASSAGE = /^(?:report adopted;\s*)?passed (third|final) reading\b/i;

export function classifyHawaiiAction(action: string, billType: string): HawaiiActionClassification {
  const match = FLOOR_PASSAGE.exec(action.trim());
  if (!match) {
    return { isFloorVote: false, questionClass: null, reason: "not_floor_passage" };
  }
  const questionClass: HawaiiQuestionClass = match[1]!.toLowerCase() === "third" ? "third_reading" : "final_reading";
  if (!HAWAII_KEPT_BILL_TYPES.includes(billType)) {
    return { isFloorVote: false, questionClass, reason: `excluded_measure:${billType}` };
  }
  return { isFloorVote: true, questionClass, reason: `kept:${questionClass}` };
}

// "as amended (CD 1)" / "as amended in HD 2" → "CD 1". Hawaii moves a bill
// through numbered drafts (HD n = House draft, SD n = Senate draft, CD n =
// conference draft); the draft named on the passage line IS the text that
// chamber voted, which makes the version check exact. null = the line
// names no draft: the chamber passed the text as it stood.
const DRAFT = /\bas amended(?:\s+in|\s*\()\s*(HD|SD|CD)\s*(\d+)\)?/i;

export function hawaiiVotedDraft(action: string): string | null {
  const match = DRAFT.exec(action);
  return match ? `${match[1]!.toUpperCase()} ${Number(match[2])}` : null;
}

// ---------------------------------------------------------------------------
// The vote text

export type HawaiiFloorVoteText = {
  form: "senate" | "house";
  reading: HawaiiQuestionClass;
  draft: string | null;
  // The Senate prints its aye count; the House does not.
  ayes: number | null;
  // Members named on the line, as printed ("Awa", "Lee, M.").
  reservations: string[];
  noes: string[];
  excused: string[];
};

// Senate:  "Ayes, 23; Aye(s) with reservations: Senator(s) Rhoads. Noes, 1
//          (Senator(s) DeCorte). Excused, 1 (Senator(s) McKelvey)."
// The noes and excused segments come in two spellings each, count-first
// ("0 No(es): none.", "1 No(es): Senator(s) McKelvey.", "1 Excused:
// Senator(s) Chang.") and count-in-the-middle ("Noes, 1 (Senator(s)
// DeCorte).", "Excused, 0 (none)."); "Aye(s) with reservations: none ."
// carries a stray space before its period.
const SENATE_AYES = /\bAyes,\s*(\d+)\s*;/;
const SENATE_RESERVATIONS = /Aye\(s\) with reservations:\s*(.*?)\s*\.?\s*(?=\d+\s*No\(es\)|Noes,)/;
const SENATE_NOES =
  /(?:(\d+)\s*No\(es\):\s*(none|Senator\(s\)\s+.*?)|Noes,\s*(\d+)\s*\((.*?)\))\s*\.?\s*(?=\d+\s*Excused|Excused,)/;
const SENATE_EXCUSED = /(?:(\d+)\s*Excused:\s*(none|Senator\(s\)\s+.*?)|Excused,\s*(\d+)\s*\((.*?)\))\s*\.?\s*(?=Transmitted|$)/;

// House:   "with Representative(s) Souza voting aye with reservations;
//          Representative(s) Garcia, Pierick voting no (2) and
//          Representative(s) Cochran excused (1)."
const HOUSE_LISTS =
  /\bwith\s+(none|Representative\(s\)\s+.*?)\s+voting aye with reservations;\s*(none|Representative\(s\)\s+.*?)\s+voting no\s*\((\d+)\)\s+and\s+(none|Representative\(s\)\s+.*?)\s+excused\s*\((\d+)\)\s*\./;

const NAME_PREFIX = /^(?:Senator|Representative)\(s\)\s+/;

/**
 * "Garcia, Pierick" → ["Garcia", "Pierick"]; "Lee, M., Garcia" → ["Lee, M.",
 * "Garcia"] — a lone initial after a comma belongs to the surname before
 * it, which is how the journal tells two members of one surname apart.
 */
export function splitHawaiiNameList(raw: string): string[] {
  const body = raw.trim().replace(NAME_PREFIX, "").trim();
  if (body.length === 0 || body.toLowerCase() === "none") {
    return [];
  }
  const names: string[] = [];
  for (const part of body.split(",")) {
    const token = part.trim().replace(/\.$/, "").trim();
    if (token.length === 0) {
      continue;
    }
    if (/^[A-Z]$/.test(token) && names.length > 0) {
      names[names.length - 1] = `${names[names.length - 1]}, ${token}.`;
      continue;
    }
    names.push(token);
  }
  return names;
}

/** Reads the passage line's tallies and named members; throws on any shape it does not know. */
export function parseHawaiiFloorVoteText(action: string, chamber: LegislativeVoteChamber): HawaiiFloorVoteText {
  const text = action.trim();
  const passage = FLOOR_PASSAGE.exec(text);
  if (!passage) {
    throw new Error(`not a floor passage line: ${text.slice(0, 80)}`);
  }
  const reading: HawaiiQuestionClass = passage[1]!.toLowerCase() === "third" ? "third_reading" : "final_reading";
  const draft = hawaiiVotedDraft(text);

  const house = HOUSE_LISTS.exec(text);
  const senateAyes = SENATE_AYES.exec(text);
  if (house && senateAyes) {
    throw new Error("passage line reads as both the House and the Senate form");
  }
  if (house) {
    if (chamber !== "house") {
      throw new Error(`House-form passage line on a ${chamber} history row`);
    }
    const noes = splitHawaiiNameList(house[2]!);
    const excused = splitHawaiiNameList(house[4]!);
    if (noes.length !== Number(house[3])) {
      throw new Error(`House line names ${noes.length} no votes but counts ${house[3]}`);
    }
    if (excused.length !== Number(house[5])) {
      throw new Error(`House line names ${excused.length} excused but counts ${house[5]}`);
    }
    return { form: "house", reading, draft, ayes: null, reservations: splitHawaiiNameList(house[1]!), noes, excused };
  }
  if (senateAyes) {
    if (chamber !== "senate") {
      throw new Error(`Senate-form passage line on a ${chamber} history row`);
    }
    const reservations = SENATE_RESERVATIONS.exec(text);
    const noes = SENATE_NOES.exec(text);
    const excused = SENATE_EXCUSED.exec(text);
    if (!reservations || !noes || !excused) {
      throw new Error(`Senate line is missing its reservations, noes or excused segment: ${text.slice(0, 160)}`);
    }
    const noesNames = splitHawaiiNameList(noes[2] ?? noes[4] ?? "");
    const noesCount = Number(noes[1] ?? noes[3]);
    if (noesNames.length !== noesCount) {
      throw new Error(`Senate line names ${noesNames.length} no votes but counts ${noesCount}`);
    }
    const excusedNames = splitHawaiiNameList(excused[2] ?? excused[4] ?? "");
    const excusedCount = Number(excused[1] ?? excused[3]);
    if (excusedNames.length !== excusedCount) {
      throw new Error(`Senate line names ${excusedNames.length} excused but counts ${excusedCount}`);
    }
    return {
      form: "senate",
      reading,
      draft,
      ayes: Number(senateAyes[1]),
      reservations: splitHawaiiNameList(reservations[1]!),
      noes: noesNames,
      excused: excusedNames,
    };
  }
  throw new Error(`passage line matches neither chamber's vote form: ${text.slice(0, 160)}`);
}

// ---------------------------------------------------------------------------
// Members

function normalizeName(value: string): string {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z]+/g, " ")
    .trim();
}

function lastToken(value: string): string {
  const tokens = normalizeName(value).split(" ").filter((token) => token.length > 0);
  return tokens[tokens.length - 1] ?? "";
}

/** The chamber a person sits in, from `district` (HD-/SD-), never from `role` — the district field is the reliable one. */
export function hawaiiPersonChamber(person: LegiscanPerson): LegislativeVoteChamber | null {
  const district = person.district ?? "";
  if (/^HD-/i.test(district)) {
    return "house";
  }
  if (/^SD-/i.test(district)) {
    return "senate";
  }
  return null;
}

/**
 * One printed name → one person of the chamber. The journal prints the
 * surname only ("Awa"), a surname plus initial where two members share one
 * ("Lee, M."), and sometimes a surname longer or shorter than LegiScan's:
 * "Belatti" for `au Belatti`, "Lamosao" for `Fernandez Lamosao`, "Dela
 * Cruz" for `Cruz`. So the match is: the whole surname first, then its last
 * token against the last token of the person's last name; an initial, when
 * printed, must open the person's first name or nickname. Exactly one
 * person must remain, or the line cannot be read.
 */
export function resolveHawaiiMemberName(printed: string, chamberPeople: readonly LegiscanPerson[]): LegiscanPerson {
  const initialMatch = /^(.*?),?\s+([A-Z])\.?$/.exec(printed.trim());
  const surname = initialMatch ? initialMatch[1]! : printed.trim();
  const initial = initialMatch ? initialMatch[2]!.toLowerCase() : null;
  const wanted = normalizeName(surname);
  let candidates = chamberPeople.filter((person) => normalizeName(person.lastName) === wanted);
  if (candidates.length === 0) {
    const wantedLast = lastToken(surname);
    candidates = chamberPeople.filter((person) => lastToken(person.lastName) === wantedLast);
  }
  if (initial !== null) {
    candidates = candidates.filter((person) =>
      [person.firstName, person.name].some((value) => normalizeName(value).startsWith(initial))
    );
  }
  if (candidates.length !== 1) {
    throw new Error(
      `"${printed}" resolves to ${candidates.length} members of the chamber` +
        (candidates.length > 1 ? ` (${candidates.map((person) => person.name).join(", ")})` : "")
    );
  }
  return candidates[0]!;
}

// A committed per-session seat file for members who did not sit the whole
// session (a mid-session appointment, a resignation): people_ids with the
// dates they were sitting, and districts with the dates they stood VACANT.
// Members not listed sat all session. Without it a chamber with 53 people
// on file cannot reconstruct a 51-seat vote — the seat-count check fails
// and the fetcher reports which date to look up. The House prints no aye
// count, so a vacancy is invisible in its text; the dates in this file must
// come from an official source (the governor's appointment notice, the
// chamber's own announcement), never from where the votes seem to stop.
export type HawaiiSeatWindow = { peopleId: number; from: string | null; to: string | null; note: string | null };

export type HawaiiVacancy = { district: string; chamber: LegislativeVoteChamber; from: string | null; to: string | null; note: string | null };

export type HawaiiSeatFile = {
  sessionId: number;
  windows: ReadonlyMap<number, HawaiiSeatWindow>;
  vacancies: readonly HawaiiVacancy[];
};

/** The seats a chamber had filled on a date: the constitutional count minus declared vacancies. */
export function hawaiiExpectedSeats(chamber: LegislativeVoteChamber, date: string, seats: HawaiiSeatFile | null): number {
  const vacant = (seats?.vacancies ?? []).filter(
    (vacancy) => vacancy.chamber === chamber && (vacancy.from === null || vacancy.from <= date) && (vacancy.to === null || date <= vacancy.to)
  ).length;
  return HAWAII_CHAMBER_SEATS[chamber] - vacant;
}

export function parseHawaiiSeatFile(raw: unknown, expectedSessionId: number): HawaiiSeatFile {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error("seat file must be an object");
  }
  const record = raw as Record<string, unknown>;
  if (record.jurisdiction !== HAWAII_JURISDICTION) {
    throw new Error(`seat file jurisdiction must be "HI", got ${JSON.stringify(record.jurisdiction)}`);
  }
  if (record.session_id !== expectedSessionId) {
    throw new Error(`seat file session_id is ${JSON.stringify(record.session_id)}, run is ${expectedSessionId}`);
  }
  if (!Array.isArray(record.windows)) {
    throw new Error("seat file windows must be an array");
  }
  const windows = new Map<number, HawaiiSeatWindow>();
  for (const [index, element] of record.windows.entries()) {
    if (typeof element !== "object" || element === null || Array.isArray(element)) {
      throw new Error(`seat file windows[${index}] is not an object`);
    }
    const entry = element as Record<string, unknown>;
    const peopleId = entry.people_id;
    if (typeof peopleId !== "number" || !Number.isSafeInteger(peopleId) || peopleId < 1) {
      throw new Error(`seat file windows[${index}] people_id must be a positive integer`);
    }
    if (windows.has(peopleId)) {
      throw new Error(`seat file names people_id ${peopleId} twice`);
    }
    for (const field of ["from", "to"] as const) {
      const value = entry[field];
      if (value !== null && value !== undefined && (typeof value !== "string" || !ISO_DATE.test(value))) {
        throw new Error(`seat file windows[${index}] ${field} must be YYYY-MM-DD or null`);
      }
    }
    windows.set(peopleId, {
      peopleId,
      from: typeof entry.from === "string" ? entry.from : null,
      to: typeof entry.to === "string" ? entry.to : null,
      note: typeof entry.note === "string" && entry.note.trim().length > 0 ? entry.note.trim() : null,
    });
  }
  const vacancies: HawaiiVacancy[] = [];
  const rawVacancies = record.vacancies ?? [];
  if (!Array.isArray(rawVacancies)) {
    throw new Error("seat file vacancies must be an array when present");
  }
  for (const [index, element] of rawVacancies.entries()) {
    if (typeof element !== "object" || element === null || Array.isArray(element)) {
      throw new Error(`seat file vacancies[${index}] is not an object`);
    }
    const entry = element as Record<string, unknown>;
    const district = entry.district;
    if (typeof district !== "string" || !/^(HD|SD)-\d+$/i.test(district.trim())) {
      throw new Error(`seat file vacancies[${index}] district must look like HD-018 or SD-019`);
    }
    for (const field of ["from", "to"] as const) {
      const value = entry[field];
      if (value !== null && value !== undefined && (typeof value !== "string" || !ISO_DATE.test(value))) {
        throw new Error(`seat file vacancies[${index}] ${field} must be YYYY-MM-DD or null`);
      }
    }
    vacancies.push({
      district: district.trim().toUpperCase(),
      chamber: /^HD/i.test(district.trim()) ? "house" : "senate",
      from: typeof entry.from === "string" ? entry.from : null,
      to: typeof entry.to === "string" ? entry.to : null,
      note: typeof entry.note === "string" && entry.note.trim().length > 0 ? entry.note.trim() : null,
    });
  }
  return { sessionId: expectedSessionId, windows, vacancies };
}

/** The members of one chamber sitting on a date. */
export function hawaiiSittingMembers(
  people: readonly LegiscanPerson[],
  chamber: LegislativeVoteChamber,
  date: string,
  seats: HawaiiSeatFile | null
): LegiscanPerson[] {
  return people.filter((person) => {
    if (hawaiiPersonChamber(person) !== chamber) {
      return false;
    }
    const window = seats?.windows.get(person.peopleId);
    if (!window) {
      return true;
    }
    return (window.from === null || window.from <= date) && (window.to === null || date <= window.to);
  });
}

export type HawaiiMemberLists = {
  yeas: number[];
  nays: number[];
  excused: number[];
  // Named ayes-with-reservations; already inside `yeas`.
  reservations: number[];
};

/**
 * The reconstruction: every sitting member of the chamber not named as a no
 * or as excused voted aye. Checked before it is returned — the three lists
 * must fill the chamber exactly, no member may appear twice, and a printed
 * aye count must equal the reconstructed one.
 */
export function reconstructHawaiiMembers(
  text: HawaiiFloorVoteText,
  chamber: LegislativeVoteChamber,
  sitting: readonly LegiscanPerson[],
  // The chamber's seat count; overridable only so tests can use a small chamber.
  seatsExpected: number = HAWAII_CHAMBER_SEATS[chamber]
): HawaiiMemberLists {
  const resolve = (names: readonly string[]): number[] => names.map((name) => resolveHawaiiMemberName(name, sitting).peopleId);
  const nays = resolve(text.noes);
  const excused = resolve(text.excused);
  const reservations = resolve(text.reservations);
  const nonAye = new Set([...nays, ...excused]);
  if (nonAye.size !== nays.length + excused.length) {
    throw new Error("a member is named both as a no vote and as excused");
  }
  if (reservations.some((peopleId) => nonAye.has(peopleId))) {
    throw new Error("a member is named both as aye with reservations and as a no vote or excused");
  }
  const yeas = sitting.map((person) => person.peopleId).filter((peopleId) => !nonAye.has(peopleId));
  const total = yeas.length + nays.length + excused.length;
  if (total !== seatsExpected) {
    throw new Error(
      `${sitting.length} sitting members give ${yeas.length}-${nays.length} with ${excused.length} excused = ${total}, ` +
        `but the ${chamber} had ${seatsExpected} seats filled; fix the seat file for this date`
    );
  }
  if (text.ayes !== null && text.ayes !== yeas.length) {
    throw new Error(`the journal prints Ayes ${text.ayes} but the reconstruction has ${yeas.length}`);
  }
  return { yeas: yeas.sort((a, b) => a - b), nays: nays.sort((a, b) => a - b), excused: excused.sort((a, b) => a - b), reservations: reservations.sort((a, b) => a - b) };
}

// ---------------------------------------------------------------------------
// Roll numbers, URLs, evidence

// Hawaii prints no roll-call numbers. The surrogate stored on
// legislative_votes is bill_id * 200 + the history row's index: unique per
// vote, deterministic from the dataset, decodable by eye, and int4-safe
// while bill_ids stay under ten million (they are around two million).
const HISTORY_INDEX_LIMIT = 200;
const MAX_ROLL_NUMBER = 2_147_000_000;

export function hawaiiRollNumber(billId: number, historyIndex: number): number {
  if (!Number.isSafeInteger(historyIndex) || historyIndex < 0 || historyIndex >= HISTORY_INDEX_LIMIT) {
    throw new Error(`history index ${historyIndex} is outside 0..${HISTORY_INDEX_LIMIT - 1}`);
  }
  const roll = billId * HISTORY_INDEX_LIMIT + historyIndex;
  if (!Number.isSafeInteger(roll) || roll <= 0 || roll > MAX_ROLL_NUMBER) {
    throw new Error(`bill_id ${billId} gives a roll number outside the storable range`);
  }
  return roll;
}

export function decodeHawaiiRollNumber(roll: number): { billId: number; historyIndex: number } {
  return { billId: Math.floor(roll / HISTORY_INDEX_LIMIT), historyIndex: roll % HISTORY_INDEX_LIMIT };
}

/**
 * The bill's official status page on capitol.hawaii.gov (the dataset's
 * `state_link`), which lists every history line including the vote. One
 * URL per BILL, the Ohio shape; rollCallRecordUrls.ts folds it to a
 * per-bill key, which is sound because the fetcher refuses two kept floor
 * votes of one chamber on one bill and day.
 */
export function hawaiiMeasureUrl(bill: LegiscanBillSummary): string {
  if (bill.stateLink === null || !/capitol\.hawaii\.gov/i.test(bill.stateLink)) {
    throw new Error(`bill ${bill.billNumber} has no capitol.hawaii.gov state_link`);
  }
  return bill.stateLink;
}

/**
 * The stored hash pins the history ELEMENT as the dataset file held it —
 * the vote is final once printed, but a re-downloaded dataset re-serializes
 * every file, so the pin is on the element, not the file bytes.
 */
export function hawaiiHistorySha256(historyElement: unknown): string {
  return createHash("sha256").update(JSON.stringify(historyElement)).digest("hex");
}

export const HAWAII_EVIDENCE_FILE_PATTERN = /^hi-(house|senate)-(\d+)-roll(\d+)\.json$/;

export function hawaiiEvidenceFileName(chamber: LegislativeVoteChamber, sessionId: number, rollNumber: number): string {
  return `hi-${chamber}-${sessionId}-roll${rollNumber}.json`;
}

export type HawaiiVoteEvidence = {
  jurisdiction: typeof HAWAII_JURISDICTION;
  sessionId: number;
  chamber: LegislativeVoteChamber;
  rollNumber: number;
  billId: number;
  // Verbatim feed spelling (`HB957`) and the stored spelling (`HB 957`).
  bill: string;
  measureId: string;
  historyIndex: number;
  machineUrl: string;
  fetchedAt: string;
  reading: HawaiiQuestionClass;
  draft: string | null;
  // The history element verbatim; source_sha256 is hawaiiHistorySha256 over exactly this value.
  history: unknown;
  // As printed on the line.
  named: { reservations: string[]; noes: string[]; excused: string[] };
  // As reconstructed from the people snapshot (+ seat file); the importer
  // re-derives these and refuses the file if they differ.
  members: HawaiiMemberLists;
};

/** Reads and checks one evidence file's JSON against its file name. */
export function parseHawaiiVoteEvidence(
  raw: unknown,
  expected: { chamber: LegislativeVoteChamber; sessionId: number; rollNumber: number }
): HawaiiVoteEvidence {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error("Hawaii evidence file is not an object");
  }
  const evidence = raw as Record<string, unknown>;
  if (evidence.jurisdiction !== HAWAII_JURISDICTION) {
    throw new Error(`Hawaii evidence jurisdiction is ${JSON.stringify(evidence.jurisdiction)}`);
  }
  for (const [field, value] of [
    ["sessionId", expected.sessionId],
    ["chamber", expected.chamber],
    ["rollNumber", expected.rollNumber],
  ] as const) {
    if (evidence[field] !== value) {
      throw new Error(`Hawaii evidence ${field} is ${JSON.stringify(evidence[field])}, but the file name says ${value}`);
    }
  }
  for (const field of ["bill", "measureId", "machineUrl", "fetchedAt", "reading"] as const) {
    if (typeof evidence[field] !== "string" || (evidence[field] as string).trim().length === 0) {
      throw new Error(`Hawaii evidence ${field} is missing`);
    }
  }
  for (const field of ["billId", "historyIndex"] as const) {
    if (typeof evidence[field] !== "number" || !Number.isSafeInteger(evidence[field] as number)) {
      throw new Error(`Hawaii evidence ${field} is missing`);
    }
  }
  if (typeof evidence.history !== "object" || evidence.history === null) {
    throw new Error("Hawaii evidence history is missing");
  }
  const members = evidence.members as Record<string, unknown> | undefined;
  for (const field of ["yeas", "nays", "excused", "reservations"] as const) {
    const list = members?.[field];
    if (!Array.isArray(list) || list.some((value) => typeof value !== "number")) {
      throw new Error(`Hawaii evidence members.${field} is not a number array`);
    }
  }
  const named = evidence.named as Record<string, unknown> | undefined;
  for (const field of ["reservations", "noes", "excused"] as const) {
    const list = named?.[field];
    if (!Array.isArray(list) || list.some((value) => typeof value !== "string")) {
      throw new Error(`Hawaii evidence named.${field} is not a string array`);
    }
  }
  return evidence as HawaiiVoteEvidence;
}

/**
 * Preflight for one bill's kept floor votes: the `chamber:date` keys that
 * hold MORE than one. The per-bill source URL cannot tell two same-day
 * floor votes of one chamber apart (a reconsideration), so the fetcher
 * stores NEITHER — the Ohio rule.
 */
export function hawaiiKeptFloorDayCollisions(rows: readonly { chamber: LegislativeVoteChamber; date: string }[]): Set<string> {
  const perDay = new Map<string, number>();
  for (const row of rows) {
    const key = `${row.chamber}:${row.date}`;
    perDay.set(key, (perDay.get(key) ?? 0) + 1);
  }
  return new Set([...perDay.entries()].filter(([, count]) => count > 1).map(([key]) => key));
}

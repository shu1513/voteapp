import type { Pool } from "pg";

import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Member → candidate resolution for the LegiScan states (plan §2, phase-4
// variant). LegiScan identifies a member by `people_id` — a real, stable
// numeric id that survives sessions and even a chamber change, unlike
// Ohio's name-derived lpids — but it still shares no id with our
// candidates. So the identity layer stays what the Ohio pilot proved: a
// COMMITTED CROSSWALK FILE, the review artifact mapping each people_id to
// one candidate id (or explicitly to null = reviewed, no candidate).
// rollcall:legiscan:resolve PROPOSES entries from name matching with
// seat corroboration; a human reviews and commits the file; the importer
// attaches only what the file says. Nothing ever auto-attaches on a name.
//
// Because people_id is stable, the crosswalk pins only the jurisdiction —
// not the session, as Ohio's pins the General Assembly — so one file
// serves a state across sessions.

export type LegiscanPerson = {
  peopleId: number;
  name: string;
  firstName: string;
  lastName: string;
  party: string | null;
  // From `role`: Rep → house, Sen → senate; anything else (a delegate, a
  // future vocabulary change) stays null and still resolves by crosswalk.
  chamber: LegislativeVoteChamber | null;
  // Verbatim (`HD-063`, `SD-01`, `HD-Hillsborough-37`).
  district: string | null;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readTrimmedString(record: Record<string, unknown>, key: string, where: string): string {
  const value = record[key];
  // The manual's own sample prints " Joseph " — LegiScan pads some name
  // fields with stray spaces, so every string is trimmed on the way in.
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${where}: ${key} is missing or not a string`);
  }
  return value.trim();
}

/**
 * One dataset person element. Committee pseudo-persons (committee_sponsor
 * flag) are not people and never vote — the caller skips them via null.
 */
export function parseLegiscanPerson(raw: Record<string, unknown>): LegiscanPerson | null {
  if (raw.committee_sponsor === 1 || raw.committee_sponsor === true) {
    return null;
  }
  const peopleId = raw.people_id;
  if (typeof peopleId !== "number" || !Number.isSafeInteger(peopleId) || peopleId < 1) {
    throw new Error(`LegiScan person: people_id is not a positive integer: ${JSON.stringify(peopleId)}`);
  }
  const where = `LegiScan person ${peopleId}`;
  const role = typeof raw.role === "string" ? raw.role.trim() : "";
  return {
    peopleId,
    name: readTrimmedString(raw, "name", where),
    firstName: readTrimmedString(raw, "first_name", where),
    lastName: readTrimmedString(raw, "last_name", where),
    party: typeof raw.party === "string" && raw.party.trim().length > 0 ? raw.party.trim() : null,
    chamber: role === "Rep" ? "house" : role === "Sen" ? "senate" : null,
    district: typeof raw.district === "string" && raw.district.trim().length > 0 ? raw.district.trim() : null,
  };
}

// The committed people snapshot (`legiscan-people-<st>-<sessionId>.json`),
// written by rollcall:legiscan:resolve from the dataset so the importer can
// run off committed evidence alone.
export type LegiscanPeopleSnapshot = {
  jurisdiction: string;
  sessionId: number;
  byPeopleId: ReadonlyMap<number, LegiscanPerson>;
};

export function parseLegiscanPeopleSnapshot(
  raw: unknown,
  expected: { jurisdiction: string; sessionId: number }
): LegiscanPeopleSnapshot {
  if (!isRecord(raw)) {
    throw new Error("people snapshot must be an object");
  }
  if (raw.jurisdiction !== expected.jurisdiction) {
    throw new Error(`people snapshot jurisdiction is ${JSON.stringify(raw.jurisdiction)}, run is ${expected.jurisdiction}`);
  }
  if (raw.sessionId !== expected.sessionId) {
    throw new Error(`people snapshot sessionId is ${JSON.stringify(raw.sessionId)}, run is ${expected.sessionId}`);
  }
  if (!Array.isArray(raw.people) || raw.people.length === 0) {
    throw new Error("people snapshot people must be a non-empty array");
  }
  const byPeopleId = new Map<number, LegiscanPerson>();
  for (const [index, element] of raw.people.entries()) {
    if (!isRecord(element)) {
      throw new Error(`people snapshot people[${index}] is not an object`);
    }
    const person = parseLegiscanPerson(element);
    if (person === null) {
      continue;
    }
    if (byPeopleId.has(person.peopleId)) {
      throw new Error(`people snapshot lists people_id ${person.peopleId} twice`);
    }
    byPeopleId.set(person.peopleId, person);
  }
  if (byPeopleId.size === 0) {
    throw new Error("people snapshot holds no persons");
  }
  return { jurisdiction: expected.jurisdiction, sessionId: expected.sessionId, byPeopleId };
}

// ---------------------------------------------------------------------------
// Crosswalk file

export type LegiscanCrosswalkEntry = {
  peopleId: number;
  // null = a human reviewed the member and found no VoteApp candidate.
  candidateId: string | null;
  note: string | null;
};

export type LegiscanCrosswalk = {
  jurisdiction: string;
  byPeopleId: ReadonlyMap<number, LegiscanCrosswalkEntry>;
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Reads and checks the committed crosswalk file. Every people_id appears
 * once. Duplicate candidate ids are allowed (LegiScan occasionally issues a
 * second people_id to one person); the importer still refuses a roll call
 * in which two member rows land on one candidate.
 */
export function parseLegiscanCrosswalkFile(raw: unknown, expectedJurisdiction: string): LegiscanCrosswalk {
  if (!isRecord(raw)) {
    throw new Error("crosswalk file must be an object");
  }
  if (raw.source !== "legiscan") {
    throw new Error(`crosswalk source must be "legiscan", got ${JSON.stringify(raw.source)}`);
  }
  if (raw.jurisdiction !== expectedJurisdiction) {
    throw new Error(`crosswalk jurisdiction is ${JSON.stringify(raw.jurisdiction)}, run is ${expectedJurisdiction}`);
  }
  const { entries } = raw as { entries?: unknown };
  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error("crosswalk entries must be a non-empty array");
  }
  const byPeopleId = new Map<number, LegiscanCrosswalkEntry>();
  for (const [index, element] of entries.entries()) {
    if (!isRecord(element)) {
      throw new Error(`crosswalk entries[${index}] is not an object`);
    }
    const peopleId = element.people_id;
    if (typeof peopleId !== "number" || !Number.isSafeInteger(peopleId) || peopleId < 1) {
      throw new Error(`crosswalk entries[${index}]: people_id must be a positive integer`);
    }
    if (byPeopleId.has(peopleId)) {
      throw new Error(`crosswalk names people_id ${peopleId} twice`);
    }
    const candidateId = element.candidate_id;
    if (candidateId !== null && (typeof candidateId !== "string" || !UUID_PATTERN.test(candidateId))) {
      throw new Error(`crosswalk entries[${index}] (people_id ${peopleId}): candidate_id must be a UUID or null`);
    }
    const note = element.note;
    if (note !== undefined && note !== null && typeof note !== "string") {
      throw new Error(`crosswalk entries[${index}] (people_id ${peopleId}): note must be a string`);
    }
    byPeopleId.set(peopleId, {
      peopleId,
      candidateId: candidateId === null ? null : candidateId.toLowerCase(),
      note: typeof note === "string" && note.trim().length > 0 ? note.trim() : null,
    });
  }
  return { jurisdiction: expectedJurisdiction, byPeopleId };
}

// ---------------------------------------------------------------------------
// Resolution

export type LegiscanCandidateInfo = {
  candidateId: string;
  name: string;
  // On a Nov-2026-or-later office election, same rule as the federal index.
  inScope: boolean;
};

export type LegiscanMemberResolutionOutcome =
  | "matched"
  // The people_id is not in the crosswalk file: not yet reviewed.
  | "no_crosswalk"
  // The crosswalk says a human reviewed the member and found no candidate.
  | "unmatched_reviewed"
  // The crosswalk names a candidate who is not on a Nov-2026+ election.
  | "out_of_scope";

export type LegiscanMemberResolution = {
  peopleId: number;
  side: "yea" | "nay";
  outcome: LegiscanMemberResolutionOutcome;
  person: LegiscanPerson | null;
  candidate: LegiscanCandidateInfo | null;
  detail: string;
};

/**
 * Resolves one roll call's yea/nay people_ids through the crosswalk. The
 * caller has already checked (loadLegiscanCrosswalkCandidates) that every
 * crosswalk candidate exists; a people_id missing from the PEOPLE snapshot
 * is fine here — the snapshot is a session-membership list and a member
 * who resigned mid-session still appears on old roll calls.
 */
export function resolveLegiscanMembers(
  votes: { yeas: readonly number[]; nays: readonly number[] },
  crosswalk: LegiscanCrosswalk,
  peopleById: ReadonlyMap<number, LegiscanPerson>,
  candidatesById: ReadonlyMap<string, LegiscanCandidateInfo>
): LegiscanMemberResolution[] {
  const resolutions: LegiscanMemberResolution[] = [];
  for (const side of ["yea", "nay"] as const) {
    for (const peopleId of side === "yea" ? votes.yeas : votes.nays) {
      const person = peopleById.get(peopleId) ?? null;
      const entry = crosswalk.byPeopleId.get(peopleId);
      if (!entry) {
        resolutions.push({
          peopleId,
          side,
          outcome: "no_crosswalk",
          person,
          candidate: null,
          detail: `people_id ${peopleId} is not in the crosswalk`,
        });
        continue;
      }
      if (entry.candidateId === null) {
        resolutions.push({
          peopleId,
          side,
          outcome: "unmatched_reviewed",
          person,
          candidate: null,
          detail: entry.note ?? "reviewed; no candidate",
        });
        continue;
      }
      const candidate = candidatesById.get(entry.candidateId);
      if (!candidate) {
        // loadLegiscanCrosswalkCandidates makes this unreachable; kept as a
        // guard so a skipped validation cannot silently drop members.
        throw new Error(`crosswalk maps people_id ${peopleId} to unknown candidate ${entry.candidateId}`);
      }
      if (!candidate.inScope) {
        resolutions.push({
          peopleId,
          side,
          outcome: "out_of_scope",
          person,
          candidate,
          detail: `${candidate.name} is not on a Nov-2026-or-later election`,
        });
        continue;
      }
      resolutions.push({ peopleId, side, outcome: "matched", person, candidate, detail: candidate.name });
    }
  }
  return resolutions;
}

type Queryable = Pick<Pool, "query">;

/**
 * Loads every candidate the crosswalk names, with the same in-scope rule as
 * the federal index, and fails on any id that is missing, deleted, or
 * merged — a stale crosswalk must stop the run, not skip members silently.
 */
export async function loadLegiscanCrosswalkCandidates(
  db: Queryable,
  crosswalk: LegiscanCrosswalk,
  scopeFrom: string
): Promise<Map<string, LegiscanCandidateInfo>> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(scopeFrom)) {
    throw new Error(`scopeFrom must be an ISO date, got: ${scopeFrom}`);
  }
  const ids = [...new Set([...crosswalk.byPeopleId.values()].flatMap((entry) => (entry.candidateId ? [entry.candidateId] : [])))];
  const byId = new Map<string, LegiscanCandidateInfo>();
  if (ids.length === 0) {
    return byId;
  }
  const result = await db.query<{ candidate_id: string; name: string; in_scope: boolean }>(
    `
      SELECT
        c.id AS candidate_id,
        coalesce(c.display_name, c.first_name || ' ' || c.last_name) AS name,
        EXISTS (
          SELECT 1
          FROM candidate_elections AS ce
          JOIN elections AS e ON e.id = ce.election_id
          WHERE (ce.candidate_id = c.id OR ce.running_mate_candidate_id = c.id)
            AND e.race_type = 'office'
            AND e.election_date >= $2::date
            AND ce.status NOT IN ('withdrawn', 'lost')
        ) AS in_scope
      FROM candidates AS c
      WHERE c.id = ANY($1::uuid[])
        AND c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
    `,
    [ids, scopeFrom]
  );
  for (const row of result.rows) {
    byId.set(row.candidate_id, { candidateId: row.candidate_id, name: row.name, inScope: row.in_scope });
  }
  const missing = ids.filter((id) => !byId.has(id));
  if (missing.length > 0) {
    throw new Error(`crosswalk names candidates that do not exist (deleted or merged?): ${missing.join(", ")}`);
  }
  return byId;
}

// ---------------------------------------------------------------------------
// Crosswalk PROPOSALS (rollcall:legiscan:resolve). Report-only name
// matching with seat corroboration; the human decides what enters the
// committed file. The name rules are the Ohio pilot's, which the review
// there proved conservative enough; LegiScan's last_name is a clean field
// (no `"Hall, D."` display disambiguators), so the pilot's shared-surname
// blind spot does not carry over.

export type LegiscanCandidateForMatching = {
  candidateId: string;
  name: string;
  // 'state_lower' | 'state_upper' of the Nov-2026 candidacy; identity only —
  // a sitting representative may be RUNNING for the other chamber.
  scope: string;
  districtName: string;
};

export type LegiscanCrosswalkProposal = {
  peopleId: number;
  rosterName: string;
  rosterSeat: string;
  candidateId: string;
  candidateName: string;
  candidacy: string;
  // first_and_last = both names agree exactly; first_prefix = last names
  // agree and one first name is a prefix of the other (Al/Alessandro).
  confidence: "first_and_last" | "first_prefix";
  // Whether the member's seat is the candidacy's seat: true/false when both
  // sides parse to a chamber + district number, null when either does not
  // (a false does NOT veto — the member may be running for another seat —
  // it is a flag for the reviewer to look twice).
  seatAgrees: boolean | null;
};

function nameTokens(name: string): string[] {
  return name
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z]+/g, " ")
    .trim()
    .split(" ")
    .filter((token) => token.length > 0 && !["jr", "sr", "ii", "iii", "iv"].includes(token));
}

function lastNameMatches(rosterLast: string, candidateTokens: readonly string[]): boolean {
  const lastTokens = nameTokens(rosterLast);
  if (lastTokens.length === 0 || candidateTokens.length < lastTokens.length + 1) {
    return false;
  }
  const tail = candidateTokens.slice(-lastTokens.length);
  return lastTokens.every((token, index) => token === tail[index]);
}

function firstNameConfidence(
  rosterFirst: string,
  candidateFirstToken: string | undefined
): LegiscanCrosswalkProposal["confidence"] | null {
  const first = nameTokens(rosterFirst)[0];
  if (!first || !candidateFirstToken) {
    return null;
  }
  if (first === candidateFirstToken) {
    return "first_and_last";
  }
  if (first.length >= 2 && candidateFirstToken.length >= 2 && (first.startsWith(candidateFirstToken) || candidateFirstToken.startsWith(first))) {
    return "first_prefix";
  }
  return null;
}

/** `HD-063` / `SD-01` → its chamber and district number; null for the named-district states (`HD-Hillsborough-37`). */
export function parseLegiscanDistrict(district: string): { chamber: LegislativeVoteChamber; number: number } | null {
  const match = /^([HS])D-0*(\d+)$/.exec(district.trim());
  if (!match) {
    return null;
  }
  return { chamber: match[1] === "H" ? "house" : "senate", number: Number(match[2]) };
}

/** `State House District 83 (2024); Texas` → 83; null when no plain district number is present. */
export function parseCandidateDistrictNumber(districtName: string): number | null {
  const match = /\bdistrict\s+0*(\d+)\b/i.exec(districtName);
  return match ? Number(match[1]) : null;
}

function seatAgrees(person: LegiscanPerson, candidate: LegiscanCandidateForMatching): boolean | null {
  const memberSeat = person.district === null ? null : parseLegiscanDistrict(person.district);
  const candidateNumber = parseCandidateDistrictNumber(candidate.districtName);
  const candidateChamber =
    candidate.scope === "state_lower" ? "house" : candidate.scope === "state_upper" ? "senate" : null;
  if (memberSeat === null || candidateNumber === null || candidateChamber === null) {
    return null;
  }
  return memberSeat.chamber === candidateChamber && memberSeat.number === candidateNumber;
}

/**
 * Proposes people_id → candidate pairs by name: the member's last name must
 * be the tail of the candidate's name tokens and the first names must agree
 * exactly or by prefix, and the pair must be unique in BOTH directions — a
 * member matching two candidates, or a candidate matching two members,
 * proposes nothing. Suggestions for the human review only.
 */
export function proposeLegiscanCrosswalk(
  people: readonly LegiscanPerson[],
  candidates: readonly LegiscanCandidateForMatching[]
): {
  proposals: LegiscanCrosswalkProposal[];
  unmatchedPeople: LegiscanPerson[];
  unmatchedCandidates: LegiscanCandidateForMatching[];
} {
  const candidateTokens = candidates.map((candidate) => nameTokens(candidate.name));
  const pairs: { person: LegiscanPerson; candidateIndex: number; confidence: LegiscanCrosswalkProposal["confidence"] }[] = [];
  for (const person of people) {
    for (const [candidateIndex, tokens] of candidateTokens.entries()) {
      if (!lastNameMatches(person.lastName, tokens)) {
        continue;
      }
      const confidence = firstNameConfidence(person.firstName, tokens[0]);
      if (confidence) {
        pairs.push({ person, candidateIndex, confidence });
      }
    }
  }
  const byPeopleId = new Map<number, number>();
  const byCandidate = new Map<number, number>();
  for (const pair of pairs) {
    byPeopleId.set(pair.person.peopleId, (byPeopleId.get(pair.person.peopleId) ?? 0) + 1);
    byCandidate.set(pair.candidateIndex, (byCandidate.get(pair.candidateIndex) ?? 0) + 1);
  }
  const proposals: LegiscanCrosswalkProposal[] = [];
  const proposedPeople = new Set<number>();
  const proposedCandidates = new Set<string>();
  for (const pair of pairs) {
    if (byPeopleId.get(pair.person.peopleId) !== 1 || byCandidate.get(pair.candidateIndex) !== 1) {
      continue;
    }
    const candidate = candidates[pair.candidateIndex]!;
    proposals.push({
      peopleId: pair.person.peopleId,
      rosterName: pair.person.name,
      rosterSeat: `${pair.person.chamber ?? "?"} ${pair.person.district ?? "?"}`,
      candidateId: candidate.candidateId,
      candidateName: candidate.name,
      candidacy: `${candidate.scope}: ${candidate.districtName}`,
      confidence: pair.confidence,
      seatAgrees: seatAgrees(pair.person, candidate),
    });
    proposedPeople.add(pair.person.peopleId);
    proposedCandidates.add(candidate.candidateId);
  }
  proposals.sort((a, b) => a.peopleId - b.peopleId);
  return {
    proposals,
    unmatchedPeople: people.filter((person) => !proposedPeople.has(person.peopleId)),
    unmatchedCandidates: candidates.filter((candidate) => !proposedCandidates.has(candidate.candidateId)),
  };
}

/**
 * The Nov-2026+ state-legislative candidate pool the proposals draw from.
 * A sitting member running for a NON-legislative office in Nov 2026 is not
 * in this pool; the human adds such a pair to the file by hand.
 */
export async function loadLegiscanStateLegCandidates(
  db: Queryable,
  state: string,
  scopeFrom: string
): Promise<LegiscanCandidateForMatching[]> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(scopeFrom)) {
    throw new Error(`scopeFrom must be an ISO date, got: ${scopeFrom}`);
  }
  if (!/^[A-Z]{2}$/.test(state)) {
    throw new Error(`state must be a postal abbreviation, got: ${state}`);
  }
  const result = await db.query<{ candidate_id: string; name: string; scope: string; district_name: string }>(
    `
      SELECT DISTINCT
        c.id AS candidate_id,
        coalesce(c.display_name, c.first_name || ' ' || c.last_name) AS name,
        o.scope,
        d.name AS district_name
      FROM candidate_elections AS ce
      JOIN elections AS e ON e.id = ce.election_id
      JOIN districts AS d ON d.id = e.district_id
      JOIN offices AS o ON o.id = e.office_id
      JOIN candidates AS c ON c.id = ce.candidate_id
      WHERE d.state = $2
        AND e.race_type = 'office'
        AND e.election_date >= $1::date
        AND o.scope IN ('state_lower', 'state_upper')
        AND ce.status NOT IN ('withdrawn', 'lost')
        AND c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
      ORDER BY name
    `,
    [scopeFrom, state]
  );
  return result.rows.map((row) => ({
    candidateId: row.candidate_id,
    name: row.name,
    scope: row.scope,
    districtName: row.district_name,
  }));
}

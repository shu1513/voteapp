import type { Pool } from "pg";

import type { LegislativeVoteChamber } from "./legislativeVotes.js";

// Member → candidate resolution for the Ohio fan-out (plan §2, state
// variant). Ohio has no FEC-id analog: the feed identifies a member only by
// lpid (`sen_wilson_steve_1`), a name-derived slug. So the state pilot's
// identity layer is a COMMITTED CROSSWALK FILE, the review artifact that
// maps each lpid to one candidate id (or explicitly to null = reviewed, no
// candidate). rollcall:oh:resolve PROPOSES entries from roster-vs-candidate
// name matching; a human reviews and commits the file; the importer
// attaches only what the file says. Nothing ever auto-attaches on a name —
// the same rule the federal resolver enforces, with the file playing the
// role of candidates.fec_ids.

export type OhioLegislator = {
  lpid: string;
  firstName: string;
  lastName: string;
  displayName: string;
  district: string;
  party: string | null;
  chamber: LegislativeVoteChamber;
  active: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(record: Record<string, unknown>, key: string, where: string): string {
  const value = record[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${where}: ${key} is missing or not a string`);
  }
  return value.trim();
}

/** The `/legislators/` roster feed: every member of the General Assembly, by lpid. */
export function parseOhioLegislators(raw: unknown): OhioLegislator[] {
  if (typeof raw !== "object" || raw === null) {
    throw new Error("Ohio legislators feed is not an object or array");
  }
  const rows = Object.values(raw);
  if (rows.length === 0) {
    throw new Error("Ohio legislators feed is empty");
  }
  const legislators: OhioLegislator[] = [];
  const seen = new Set<string>();
  for (const [index, row] of rows.entries()) {
    if (!isRecord(row)) {
      throw new Error(`Ohio legislators feed row ${index} is not an object`);
    }
    const where = `Ohio legislator ${index}`;
    const lpid = readString(row, "lpid", where);
    // A vacant seat is a placeholder row (`rep_district_26`, lastname
    // "Vacant", empty first/display name), not a person; no journal action
    // ever names its lpid.
    if (row.lastname === "Vacant" && (typeof row.firstname !== "string" || row.firstname.trim().length === 0)) {
      continue;
    }
    if (seen.has(lpid)) {
      throw new Error(`Ohio legislators feed lists ${lpid} twice`);
    }
    seen.add(lpid);
    const chamberRaw = readString(row, "chamber", where);
    if (chamberRaw !== "House" && chamberRaw !== "Senate") {
      throw new Error(`${where} (${lpid}): chamber is ${chamberRaw}`);
    }
    legislators.push({
      lpid,
      firstName: readString(row, "firstname", where),
      lastName: readString(row, "lastname", where),
      displayName: readString(row, "displayname", where),
      district: readString(row, "district", where),
      party: typeof row.party === "string" && row.party.trim().length > 0 ? row.party.trim() : null,
      chamber: chamberRaw === "House" ? "house" : "senate",
      active: row.active === true,
    });
  }
  return legislators;
}

export type OhioCrosswalkEntry = {
  lpid: string;
  // null = a human reviewed the member and found no VoteApp candidate.
  candidateId: string | null;
  note: string | null;
};

export type OhioCrosswalk = {
  generalAssembly: number;
  byLpid: ReadonlyMap<string, OhioCrosswalkEntry>;
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Reads and checks the committed crosswalk file. Every lpid appears once.
 * One candidate may legitimately carry TWO lpids in one General Assembly
 * (a representative appointed to the Senate mid-term keeps both slugs), so
 * duplicate candidate ids are allowed here; the importer still refuses a
 * roll call in which two member rows land on one candidate.
 */
export function parseOhioCrosswalkFile(raw: unknown): OhioCrosswalk {
  if (!isRecord(raw)) {
    throw new Error("crosswalk file must be an object");
  }
  if (raw.jurisdiction !== "OH") {
    throw new Error(`crosswalk jurisdiction must be "OH", got ${JSON.stringify(raw.jurisdiction)}`);
  }
  const generalAssembly = raw.general_assembly;
  if (typeof generalAssembly !== "number" || !Number.isSafeInteger(generalAssembly) || generalAssembly < 1) {
    throw new Error("crosswalk general_assembly must be a positive integer");
  }
  const { entries } = raw as { entries?: unknown };
  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error("crosswalk entries must be a non-empty array");
  }
  const byLpid = new Map<string, OhioCrosswalkEntry>();
  for (const [index, element] of entries.entries()) {
    if (!isRecord(element)) {
      throw new Error(`crosswalk entries[${index}] is not an object`);
    }
    const lpid = readString(element, "lpid", `crosswalk entries[${index}]`);
    if (byLpid.has(lpid)) {
      throw new Error(`crosswalk names ${lpid} twice`);
    }
    const candidateId = element.candidate_id;
    if (candidateId !== null && (typeof candidateId !== "string" || !UUID_PATTERN.test(candidateId))) {
      throw new Error(`crosswalk entries[${index}] (${lpid}): candidate_id must be a UUID or null`);
    }
    const note = element.note;
    if (note !== undefined && note !== null && typeof note !== "string") {
      throw new Error(`crosswalk entries[${index}] (${lpid}): note must be a string`);
    }
    byLpid.set(lpid, {
      lpid,
      candidateId: candidateId === null ? null : candidateId.toLowerCase(),
      note: typeof note === "string" && note.trim().length > 0 ? note.trim() : null,
    });
  }
  return { generalAssembly, byLpid };
}

export type OhioCandidateInfo = {
  candidateId: string;
  name: string;
  // On a Nov-2026-or-later office election, same rule as the federal index.
  inScope: boolean;
};

export type OhioMemberResolutionOutcome =
  | "matched"
  // The lpid is not in the crosswalk file: not yet reviewed.
  | "no_crosswalk"
  // The crosswalk says a human reviewed the member and found no candidate.
  | "unmatched_reviewed"
  // The crosswalk names a candidate who is not on a Nov-2026+ election.
  | "out_of_scope";

export type OhioMemberResolution = {
  lpid: string;
  side: "yea" | "nay";
  outcome: OhioMemberResolutionOutcome;
  legislator: OhioLegislator | null;
  candidate: OhioCandidateInfo | null;
  detail: string;
};

/**
 * Resolves one roll call's yea/nay lpids through the crosswalk. The caller
 * has already checked (validateOhioCrosswalk) that every crosswalk
 * candidate exists; an lpid missing from the ROSTER is fine here — the
 * roster is a current-membership snapshot and a member who resigned
 * mid-session still appears in old journal actions.
 */
export function resolveOhioMembers(
  votes: { yeas: readonly string[]; nays: readonly string[] },
  crosswalk: OhioCrosswalk,
  rosterByLpid: ReadonlyMap<string, OhioLegislator>,
  candidatesById: ReadonlyMap<string, OhioCandidateInfo>
): OhioMemberResolution[] {
  const resolutions: OhioMemberResolution[] = [];
  for (const side of ["yea", "nay"] as const) {
    for (const lpid of side === "yea" ? votes.yeas : votes.nays) {
      const legislator = rosterByLpid.get(lpid) ?? null;
      const entry = crosswalk.byLpid.get(lpid);
      if (!entry) {
        resolutions.push({ lpid, side, outcome: "no_crosswalk", legislator, candidate: null, detail: `${lpid} is not in the crosswalk` });
        continue;
      }
      if (entry.candidateId === null) {
        resolutions.push({
          lpid,
          side,
          outcome: "unmatched_reviewed",
          legislator,
          candidate: null,
          detail: entry.note ?? "reviewed; no candidate",
        });
        continue;
      }
      const candidate = candidatesById.get(entry.candidateId);
      if (!candidate) {
        // validateOhioCrosswalk makes this unreachable; kept as a guard so a
        // skipped validation cannot silently drop members.
        throw new Error(`crosswalk maps ${lpid} to unknown candidate ${entry.candidateId}`);
      }
      if (!candidate.inScope) {
        resolutions.push({
          lpid,
          side,
          outcome: "out_of_scope",
          legislator,
          candidate,
          detail: `${candidate.name} is not on a Nov-2026-or-later election`,
        });
        continue;
      }
      resolutions.push({ lpid, side, outcome: "matched", legislator, candidate, detail: candidate.name });
    }
  }
  return resolutions;
}

type Queryable = Pick<Pool, "query">;

/**
 * Loads every candidate the crosswalk names, with the same in-scope rule as
 * loadCandidateFecIndex, and fails on any id that is missing, deleted, or
 * merged — a stale crosswalk must stop the run, not skip members silently.
 */
export async function loadOhioCrosswalkCandidates(
  db: Queryable,
  crosswalk: OhioCrosswalk,
  scopeFrom: string
): Promise<Map<string, OhioCandidateInfo>> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(scopeFrom)) {
    throw new Error(`scopeFrom must be an ISO date, got: ${scopeFrom}`);
  }
  const ids = [...new Set([...crosswalk.byLpid.values()].flatMap((entry) => (entry.candidateId ? [entry.candidateId] : [])))];
  const byId = new Map<string, OhioCandidateInfo>();
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
// Crosswalk PROPOSALS (rollcall:oh:resolve). Report-only name matching; the
// human decides what enters the committed file.

export type OhioCandidateForMatching = {
  candidateId: string;
  name: string;
  // 'state_lower' | 'state_upper' of the Nov-2026 candidacy; identity only —
  // a sitting representative may be RUNNING for the other chamber.
  scope: string;
  districtName: string;
};

export type OhioCrosswalkProposal = {
  lpid: string;
  rosterName: string;
  rosterSeat: string;
  candidateId: string;
  candidateName: string;
  candidacy: string;
  // first_and_last = both names agree exactly; first_prefix = last names
  // agree and one first name is a prefix of the other (Al/Alessandro).
  confidence: "first_and_last" | "first_prefix";
};

function nameTokens(name: string): string[] {
  return name
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
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
): OhioCrosswalkProposal["confidence"] | null {
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

/**
 * Proposes lpid → candidate pairs by name: the roster last name must be the
 * tail of the candidate's name tokens and the first names must agree
 * exactly or by prefix, and the pair must be unique in BOTH directions —
 * a roster member matching two candidates, or a candidate matching two
 * members, proposes nothing. Suggestions for the human review only.
 */
export function proposeOhioCrosswalk(
  roster: readonly OhioLegislator[],
  candidates: readonly OhioCandidateForMatching[]
): { proposals: OhioCrosswalkProposal[]; unmatchedRoster: OhioLegislator[]; unmatchedCandidates: OhioCandidateForMatching[] } {
  const candidateTokens = candidates.map((candidate) => nameTokens(candidate.name));
  const pairs: { legislator: OhioLegislator; candidateIndex: number; confidence: OhioCrosswalkProposal["confidence"] }[] = [];
  for (const legislator of roster) {
    for (const [candidateIndex, tokens] of candidateTokens.entries()) {
      if (!lastNameMatches(legislator.lastName, tokens)) {
        continue;
      }
      const confidence = firstNameConfidence(legislator.firstName, tokens[0]);
      if (confidence) {
        pairs.push({ legislator, candidateIndex, confidence });
      }
    }
  }
  const byLpid = new Map<string, number>();
  const byCandidate = new Map<number, number>();
  for (const pair of pairs) {
    byLpid.set(pair.legislator.lpid, (byLpid.get(pair.legislator.lpid) ?? 0) + 1);
    byCandidate.set(pair.candidateIndex, (byCandidate.get(pair.candidateIndex) ?? 0) + 1);
  }
  const proposals: OhioCrosswalkProposal[] = [];
  const proposedLpids = new Set<string>();
  const proposedCandidates = new Set<string>();
  for (const pair of pairs) {
    if (byLpid.get(pair.legislator.lpid) !== 1 || byCandidate.get(pair.candidateIndex) !== 1) {
      continue;
    }
    const candidate = candidates[pair.candidateIndex]!;
    proposals.push({
      lpid: pair.legislator.lpid,
      rosterName: pair.legislator.displayName,
      rosterSeat: `${pair.legislator.chamber} ${pair.legislator.district}`,
      candidateId: candidate.candidateId,
      candidateName: candidate.name,
      candidacy: `${candidate.scope}: ${candidate.districtName}`,
      confidence: pair.confidence,
    });
    proposedLpids.add(pair.legislator.lpid);
    proposedCandidates.add(candidate.candidateId);
  }
  proposals.sort((a, b) => a.lpid.localeCompare(b.lpid));
  return {
    proposals,
    unmatchedRoster: roster.filter((legislator) => !proposedLpids.has(legislator.lpid)),
    unmatchedCandidates: candidates.filter((candidate) => !proposedCandidates.has(candidate.candidateId)),
  };
}

/**
 * The Nov-2026+ Ohio state-legislative candidate pool the proposals draw
 * from. A sitting member running for a NON-legislative office in Nov 2026
 * is not in this pool; the human adds such a pair to the file by hand.
 */
export async function loadOhioStateLegCandidates(db: Queryable, scopeFrom: string): Promise<OhioCandidateForMatching[]> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(scopeFrom)) {
    throw new Error(`scopeFrom must be an ISO date, got: ${scopeFrom}`);
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
      WHERE d.state = 'OH'
        AND e.race_type = 'office'
        AND e.election_date >= $1::date
        AND o.scope IN ('state_lower', 'state_upper')
        AND ce.status NOT IN ('withdrawn', 'lost')
        AND c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
      ORDER BY name
    `,
    [scopeFrom]
  );
  return result.rows.map((row) => ({
    candidateId: row.candidate_id,
    name: row.name,
    scope: row.scope,
    districtName: row.district_name,
  }));
}

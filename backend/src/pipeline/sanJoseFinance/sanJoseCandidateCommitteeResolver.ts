// Candidate → controlled-committee resolution for San José (plan Phase 2).
// The efile.systems export has NO cover sheet, so committee identity comes
// from committee-NAME parsing gated by the CAL committee-type code — never
// from the name alone. Constraints below were audited against the live
// 2025+2026 exports (plan "Committee resolution", dry-run 2026-08-10):
//
// - Only `Cmtte_Type=C` (candidate-controlled) may feed direct totals. An
//   outside committee can carry the candidate's name ("South Bay Working
//   Families Supporting Ortiz for City Council 2026" is `P`), so type is the
//   safety gate. Codes outside the observed {C, P, G} set fail closed.
// - `Filer_ID` is text and sometimes the literal "Pending" — never a durable
//   identity; such committees cannot auto-link.
// - District is NOT reliably in the name ("Nora Campos for San Jose City
//   Council 2026", "Van Le for City Council 2026") — a confirming veto when
//   present, never a match requirement. Same for the election year.
// - Person names match on parsed word tokens (given AND surname, nickname
//   expansion on the VoteApp side only) via the shared middle-evidence and
//   generational-suffix gates — substring probes are unsafe ("Le" is inside
//   "Electrical" and "Valley").
//
// Anything else — no match, two committees matching one candidate, one
// committee matching two candidates — fails closed for manual review.

import type { EfileCalFilingRowBase } from "../efileCalFinance/efileCalWorkbookParser.js";
import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import {
  committeeNameMiddleEvidenceRowNames,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import { isSanJoseCityCouncilSeatNumber } from "./sanJoseFinanceEligibleOffices.js";

export const SAN_JOSE_PENDING_FILER_ID = "Pending";

/** One committee as observed across export rows (grouped by Filer_ID). */
export type SanJoseExportCommittee = {
  /** FPPC ID as text; may be the literal "Pending". */
  filerId: string;
  /** Distinct Filer_NamL spellings observed (accents/whitespace vary upstream). */
  committeeNames: readonly string[];
  /** Distinct non-null Cmtte_Type codes observed. */
  committeeTypes: readonly string[];
};

/**
 * Groups export rows into committees by Filer_ID. "Pending" is not an
 * identity, so Pending rows group by normalized committee name instead —
 * two ID-less committees never collapse into one.
 */
export function collectSanJoseExportCommittees(
  rows: readonly Pick<EfileCalFilingRowBase, "filerId" | "filerName" | "cmtteType">[],
): SanJoseExportCommittee[] {
  const byKey = new Map<string, { filerId: string; names: Set<string>; types: Set<string> }>();
  for (const row of rows) {
    const key =
      row.filerId === SAN_JOSE_PENDING_FILER_ID
        ? `${SAN_JOSE_PENDING_FILER_ID}::${normalizeSanJoseTextKey(row.filerName)}`
        : row.filerId;
    const group = byKey.get(key) ?? { filerId: row.filerId, names: new Set(), types: new Set() };
    group.names.add(row.filerName);
    if (row.cmtteType !== null) group.types.add(row.cmtteType);
    byKey.set(key, group);
  }
  return [...byKey.keys()].sort().map((key) => {
    const group = byKey.get(key)!;
    return {
      filerId: group.filerId,
      committeeNames: [...group.names].sort(),
      committeeTypes: [...group.types].sort(),
    };
  });
}

export type SanJoseAppCandidate = {
  candidateId: string;
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district seat (1–10); null for Mayor. */
  seatNumber: number | null;
  electionYear: number;
  /** FPPC ids from candidates.state_filing_ids ([] when none). */
  stateFilingIds: readonly string[];
};

export type SanJoseCandidateCommitteeResolution = {
  candidate: SanJoseAppCandidate;
} & (
  | { status: "matched"; filerId: string; committeeName: string; matchedBy: "fppc_id" | "name" }
  | { status: "unmatched"; reason: string }
  | { status: "ambiguous"; reason: string }
);

export function normalizeSanJoseTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeSanJosePersonName(value: string): string {
  return normalizeSanJoseTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Expand nicknames on the VoteApp side ONLY (the personFirstNameNicknames
// design rule): two distinct filed names can never meet at a shared key.
function firstNamesEquivalent(candidateFirst: string, rowFirst: string): boolean {
  return candidateFirst === rowFirst || firstNameVariants(candidateFirst).includes(rowFirst);
}

// Roster display names carry quoted call names ('Emanuel "Manny" Yekutiel');
// rewritten to the parenthetical form personNameParseVariants understands so
// the call name becomes a first-name variant, not a phantom middle token.
function committeeNameMatchesCandidate(committeeName: string, candidateDisplayName: string): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    rowNames: committeeNameMiddleEvidenceRowNames(committeeName),
    normalizePersonName: normalizeSanJosePersonName,
    firstNamesEquivalent,
  });
}

/**
 * Plain person-name comparison against a roster display name, under the same
 * San José normalization, nickname expansion, middle-evidence, and suffix
 * gates the committee resolver uses. The outside-spending aggregator matches
 * S496/Schedule-D target names ("Peter Ortiz", "BIEN DOAN", "Ortiz, Peter")
 * with this — token-based, never substring.
 */
export function sanJosePersonNameMatchesCandidate(
  rowPersonName: string,
  candidateDisplayName: string,
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    rowNames: [rowPersonName],
    normalizePersonName: normalizeSanJosePersonName,
    firstNamesEquivalent,
  });
}

const KNOWN_COMMITTEE_TYPE_CODES = new Set(["C", "P", "G"]);

// "candidate" = exactly C; "outside" = only P/G (expected to carry candidate
// names — ignored for direct linking); "unknown" fails closed: no observed
// type, a code outside the known set, or C conflicting with another code.
function committeeTypeGate(types: readonly string[]): "candidate" | "outside" | "unknown" {
  if (types.length === 0) return "unknown";
  if (types.some((code) => !KNOWN_COMMITTEE_TYPE_CODES.has(code))) return "unknown";
  if (types.includes("C")) return types.length === 1 ? "candidate" : "unknown";
  return "outside";
}

// Office/district/year evidence read from a committee name. District and year
// are vetoes only when present; office words also veto across offices, and
// non-San-José office words (a candidate's committee for a DIFFERENT office
// files copies with the city — "David Cohen for California Senate District 10
// 2026" observed live) always veto.
type CommitteeNameEvidence = {
  districts: Set<number>;
  years: Set<number>;
  mentionsCouncil: boolean;
  mentionsMayor: boolean;
  mentionsForeignOffice: boolean;
};

const FOREIGN_OFFICE_WORD =
  /\b(?:SENATE|SENATOR|ASSEMBLY[A-Z]*|SUPERVISORS?|CONGRESS[A-Z]*|GOVERNOR|JUDGE|SHERIFF|ASSESSOR|TREASURER|SCHOOL|COLLEGE|TRUSTEE|BART)\b/;

function committeeNameEvidence(committeeName: string): CommitteeNameEvidence {
  const normalized = normalizeSanJoseTextKey(committeeName);
  const districts = new Set<number>();
  for (const match of normalized.matchAll(/\bDISTRICT (?:NO )?(\d{1,2})\b/g)) {
    districts.add(Number(match[1]));
  }
  for (const match of normalized.matchAll(/\bD(\d{1,2})\b/g)) {
    districts.add(Number(match[1]));
  }
  const years = new Set<number>();
  for (const match of normalized.matchAll(/\b((?:19|20)\d{2})\b/g)) {
    years.add(Number(match[1]));
  }
  return {
    districts,
    years,
    mentionsCouncil: /\bCOUNCIL[A-Z]*\b/.test(normalized),
    mentionsMayor: /\bMAYOR[A-Z]*\b/.test(normalized),
    mentionsForeignOffice: FOREIGN_OFFICE_WORD.test(normalized),
  };
}

function committeeConflictsWithCandidate(
  committee: SanJoseExportCommittee,
  candidate: SanJoseAppCandidate,
): boolean {
  return committee.committeeNames.some((name) => {
    const evidence = committeeNameEvidence(name);
    if (evidence.mentionsForeignOffice) return true;
    if (candidate.officeName === "Mayor") {
      if (evidence.mentionsCouncil || evidence.districts.size > 0) return true;
    } else {
      if (evidence.mentionsMayor) return true;
      if (evidence.districts.size > 0 && !evidence.districts.has(candidate.seatNumber!)) return true;
    }
    return evidence.years.size > 0 && !evidence.years.has(candidate.electionYear);
  });
}

/**
 * Resolves each candidate against the export's committees. Council candidates
 * without a valid seat number fail closed (district evidence would be
 * unverifiable). Committees whose names person-match but cannot link —
 * Pending id, unknown type — surface in the unmatched reason instead of
 * being silently dropped.
 */
export function resolveSanJoseCandidateCommittees(input: {
  candidates: readonly SanJoseAppCandidate[];
  committees: readonly SanJoseExportCommittee[];
}): SanJoseCandidateCommitteeResolution[] {
  const resolutions: SanJoseCandidateCommitteeResolution[] = input.candidates.map((candidate) => {
    if (candidate.officeName === "City Council Member" && !isSanJoseCityCouncilSeatNumber(candidate.seatNumber)) {
      return {
        candidate,
        status: "unmatched",
        reason: "Council candidate has no valid district seat number; cannot verify district evidence",
      };
    }

    // state_filing_ids lives on the candidate PERSON row and accumulates
    // across every race the person runs (roster research and other finance
    // modules append there), so a stored id is not race-scoped authority: a
    // council candidate can carry their own state-senate committee's FPPC id,
    // and that committee files copies with the city. Contradictory name
    // evidence therefore knocks a committee out of the id tier — falling
    // through to the fully-gated name tier — while a committee whose name
    // carries no evidence (legal-name registrations, the id tier's purpose)
    // still links.
    const idMatches = input.committees.filter(
      (committee) =>
        committee.filerId !== SAN_JOSE_PENDING_FILER_ID &&
        candidate.stateFilingIds.includes(committee.filerId) &&
        !committeeConflictsWithCandidate(committee, candidate),
    );
    if (idMatches.length > 1) {
      return {
        candidate,
        status: "ambiguous",
        reason: `state_filing_ids match ${idMatches.length} export committees`,
      };
    }
    if (idMatches.length === 1) {
      const committee = idMatches[0]!;
      if (committeeTypeGate(committee.committeeTypes) !== "candidate") {
        return {
          candidate,
          status: "unmatched",
          reason: `state filing id ${committee.filerId} points at committee type(s) [${committee.committeeTypes.join(", ")}], not a lone C; review manually`,
        };
      }
      return {
        candidate,
        status: "matched",
        filerId: committee.filerId,
        committeeName: committee.committeeNames[0]!,
        matchedBy: "fppc_id",
      };
    }

    const linkable: SanJoseExportCommittee[] = [];
    const blocked: { committee: SanJoseExportCommittee; why: string }[] = [];
    for (const committee of input.committees) {
      const gate = committeeTypeGate(committee.committeeTypes);
      if (gate === "outside") continue;
      if (!committee.committeeNames.some((name) => committeeNameMatchesCandidate(name, candidate.displayName))) {
        continue;
      }
      if (committeeConflictsWithCandidate(committee, candidate)) continue;
      if (gate === "unknown") {
        blocked.push({
          committee,
          why: `committee ${committee.filerId} (${committee.committeeNames[0]}) has unusable Cmtte_Type [${committee.committeeTypes.join(", ")}]`,
        });
        continue;
      }
      if (committee.filerId === SAN_JOSE_PENDING_FILER_ID) {
        blocked.push({
          committee,
          why: `committee "${committee.committeeNames[0]}" has FPPC id Pending — not a durable identity`,
        });
        continue;
      }
      linkable.push(committee);
    }

    if (linkable.length > 1) {
      return {
        candidate,
        status: "ambiguous",
        reason: `${linkable.length} candidate-controlled committees name-match: ${linkable.map((committee) => committee.filerId).join(", ")}`,
      };
    }
    // A name-matching committee that cannot link makes the single linkable
    // match uncertain too — never auto-pick over an unresolved sibling.
    if (blocked.length > 0) {
      return {
        candidate,
        status: "unmatched",
        reason: `cannot auto-link: ${blocked.map((entry) => entry.why).join("; ")}`,
      };
    }
    if (linkable.length === 1) {
      const committee = linkable[0]!;
      return {
        candidate,
        status: "matched",
        filerId: committee.filerId,
        committeeName: committee.committeeNames[0]!,
        matchedBy: "name",
      };
    }
    return {
      candidate,
      status: "unmatched",
      reason: "no candidate-controlled committee name-matches in the export",
    };
  });

  // One committee resolving to two candidates would attach the same money to
  // both — fail all of its links closed instead of letting order decide.
  const candidatesByFilerId = new Map<string, number>();
  for (const resolution of resolutions) {
    if (resolution.status === "matched") {
      candidatesByFilerId.set(resolution.filerId, (candidatesByFilerId.get(resolution.filerId) ?? 0) + 1);
    }
  }
  return resolutions.map((resolution) => {
    if (resolution.status === "matched" && (candidatesByFilerId.get(resolution.filerId) ?? 0) > 1) {
      return {
        candidate: resolution.candidate,
        status: "ambiguous",
        reason: `committee ${resolution.filerId} resolves to multiple roster candidates; link manually`,
      };
    }
    return resolution;
  });
}

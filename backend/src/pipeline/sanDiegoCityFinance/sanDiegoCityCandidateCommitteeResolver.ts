// Candidate → controlled-committee resolution for San Diego city (plan
// Phase 2), copy-adapted from sanJoseCandidateCommitteeResolver. The
// efile.systems export has NO cover sheet, so committee identity comes from
// committee-NAME parsing gated by the CAL committee-type code — never from
// the name alone. The SJ constraints all held on the live San Diego
// 2025+2026 exports (Phase 0 probe, 2026-08-10..12); San Diego adds and
// changes:
//
// - A TOP evidence tier from the City Clerk's official candidate log
//   (sandiego.gov/city-clerk/elections/city/electioninfo): the log links each
//   qualified candidate to a vendor filer GUID whose filing list names the
//   committee. Curated below for exactly the candidates whose committee names
//   defeat token matching by design — "Re-Elect X…" prefixes and surname-only
//   names ("POWELL FOR CITY COUNCIL 2026") carry no given name at the name
//   position. The export must still confirm the committee exists under that
//   FPPC id with a matching name; a contradicted clerk-log entry fails closed
//   (never falls through to weaker evidence — an official mapping the export
//   contradicts is a data problem for manual review).
// - The plan's CAL-ACCESS registration tier is deliberately NOT wired: it
//   needs the CAL-ACCESS raw-data artifact cache at link time and its office
//   model is state-offices-only, while the probe resolved 8/8 November
//   candidates without it. Revisit only if a rostered candidate is
//   unresolvable by clerk log + name evidence.
// - Nine council districts (not SJ's ten), and ATTORNEY joins the
//   foreign-office veto words: Municipal Attorney (City Attorney) committees
//   file with the same clerk and appear in the same export, but the office is
//   outside the Phase 2 whitelist.
//
// Unchanged SJ constraints:
// - Only `Cmtte_Type=C` (candidate-controlled) may feed direct totals; codes
//   outside the observed {C, P, G} set fail closed.
// - `Filer_ID` is text and sometimes the literal "Pending" — never a durable
//   identity; such committees cannot auto-link.
// - District/year in a committee name are confirming vetoes when present,
//   never match requirements.
// - Person names match on parsed word tokens (given AND surname, nickname
//   expansion on the VoteApp side only) via the shared middle-evidence and
//   generational-suffix gates — substring probes are unsafe.
//
// Anything else — no match, two committees matching one candidate, one
// committee matching two candidates — fails closed for manual review.

import type { EfileCalFilingRowBase } from "../efileCalFinance/efileCalWorkbookParser.js";
import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import {
  committeeNameMiddleEvidenceRowNames,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import { isSanDiegoCityCouncilSeatNumber } from "./sanDiegoCityFinanceEligibleOffices.js";
import { SAN_DIEGO_PENDING_FILER_ID } from "./sanDiegoCityFinanceWriter.js";

/** One committee as observed across export rows (grouped by Filer_ID). */
export type SanDiegoCityExportCommittee = {
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
export function collectSanDiegoCityExportCommittees(
  rows: readonly Pick<EfileCalFilingRowBase, "filerId" | "filerName" | "cmtteType">[],
): SanDiegoCityExportCommittee[] {
  const byKey = new Map<string, { filerId: string; names: Set<string>; types: Set<string> }>();
  for (const row of rows) {
    const key =
      row.filerId === SAN_DIEGO_PENDING_FILER_ID
        ? `${SAN_DIEGO_PENDING_FILER_ID}::${normalizeSanDiegoCityTextKey(row.filerName)}`
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

export type SanDiegoCityAppCandidate = {
  candidateId: string;
  displayName: string;
  officeName: "Mayor" | "City Council Member";
  /** Council district seat (1–9); null for Mayor. */
  seatNumber: number | null;
  electionYear: number;
  /** FPPC ids from candidates.state_filing_ids ([] when none). */
  stateFilingIds: readonly string[];
};

/**
 * One Clerk-log candidate → committee mapping, hand-curated from the official
 * candidate log (each entry read live before adding). Entries exist ONLY for
 * candidates the name tier cannot resolve; everything is still export-
 * confirmed at resolve time, so a stale entry fails closed instead of
 * mislinking.
 */
export type SanDiegoCityClerkLogCommittee = {
  /** normalizeSanDiegoCityTextKey of the roster display name. */
  candidateNameKey: string;
  /** Council seat (1–9); null for Mayor. */
  seatNumber: number | null;
  electionYear: number;
  filerId: string;
  committeeName: string;
  /** Vendor filer GUID from the Clerk log's eFile link (audit pointer:
   * /public/search/campaign/filings/<guid>?type=coe on efile.sandiego.gov). */
  clerkGuid: string;
};

/** Read from the Clerk's official candidate log 2026-08-10, committee names
 * and FPPC ids confirmed against the live 2025+2026 bulk exports 2026-08-12
 * (Phase 0 probe gate 5). */
export const SAN_DIEGO_CITY_CLERK_LOG_COMMITTEES: readonly SanDiegoCityClerkLogCommittee[] = [
  {
    candidateNameKey: "HENRY FOSTER III",
    seatNumber: 4,
    electionYear: 2026,
    filerId: "1481166",
    committeeName: "Re-Elect Henry Foster III for San Diego City Council 2026",
    clerkGuid: "1b96adf9-a028-4423-adbd-2297686d3821",
  },
  {
    candidateNameKey: "KENT LEE",
    seatNumber: 6,
    electionYear: 2026,
    filerId: "1478315",
    committeeName: "Re-Elect Kent Lee for City Council 2026",
    clerkGuid: "b9ee798b-173b-4c16-8738-44e91266c843",
  },
  {
    candidateNameKey: "MARK POWELL",
    seatNumber: 6,
    electionYear: 2026,
    filerId: "1485884",
    committeeName: "POWELL FOR CITY COUNCIL 2026",
    clerkGuid: "00c6b8e4-6982-456c-8e40-56df9c7b011a",
  },
];

export type SanDiegoCityCandidateCommitteeResolution = {
  candidate: SanDiegoCityAppCandidate;
} & (
  | {
      status: "matched";
      filerId: string;
      committeeName: string;
      matchedBy: "clerk_log" | "fppc_id" | "name";
    }
  | { status: "unmatched"; reason: string }
  | { status: "ambiguous"; reason: string }
);

export function normalizeSanDiegoCityTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeSanDiegoCityPersonName(value: string): string {
  return normalizeSanDiegoCityTextKey(value)
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
    normalizePersonName: normalizeSanDiegoCityPersonName,
    firstNamesEquivalent,
  });
}

/**
 * Plain person-name comparison against a roster display name, under the same
 * San Diego normalization, nickname expansion, middle-evidence, and suffix
 * gates the committee resolver uses. The outside-spending aggregator (Phase 3)
 * matches S496/Schedule-D target names with this — token-based, never
 * substring.
 */
export function sanDiegoCityPersonNameMatchesCandidate(
  rowPersonName: string,
  candidateDisplayName: string,
): boolean {
  return personNamesMatchWithMiddleEvidence({
    candidateName: candidateDisplayName.replace(/"([^"]+)"/g, " ($1) "),
    rowNames: [rowPersonName],
    normalizePersonName: normalizeSanDiegoCityPersonName,
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
// non-city office words (a candidate's committee for a DIFFERENT office files
// copies with the city — the SJ "Cohen for California Senate" case) always
// veto. ATTORNEY is foreign here: Municipal Attorney committees share the
// export but the office is outside the Phase 2 whitelist.
type CommitteeNameEvidence = {
  districts: Set<number>;
  years: Set<number>;
  mentionsCouncil: boolean;
  mentionsMayor: boolean;
  mentionsForeignOffice: boolean;
};

const FOREIGN_OFFICE_WORD =
  /\b(?:SENATE|SENATOR|ASSEMBLY[A-Z]*|SUPERVISORS?|CONGRESS[A-Z]*|GOVERNOR|JUDGE|SHERIFF|ASSESSOR|TREASURER|ATTORNEY|SCHOOL|COLLEGE|TRUSTEE|BART)\b/;

function committeeNameEvidence(committeeName: string): CommitteeNameEvidence {
  const normalized = normalizeSanDiegoCityTextKey(committeeName);
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
  committee: SanDiegoCityExportCommittee,
  candidate: SanDiegoCityAppCandidate,
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
 * Resolves each candidate against the export's committees: clerk-log tier,
 * then stored-FPPC-id tier, then the fully gated name tier. Council
 * candidates without a valid seat number fail closed (district evidence
 * would be unverifiable). Committees whose names person-match but cannot
 * link — Pending id, unknown type — surface in the unmatched reason instead
 * of being silently dropped.
 */
export function resolveSanDiegoCityCandidateCommittees(input: {
  candidates: readonly SanDiegoCityAppCandidate[];
  committees: readonly SanDiegoCityExportCommittee[];
  /** Overridable for tests; defaults to the curated Clerk-log table. */
  clerkLogCommittees?: readonly SanDiegoCityClerkLogCommittee[];
}): SanDiegoCityCandidateCommitteeResolution[] {
  const clerkLogCommittees = input.clerkLogCommittees ?? SAN_DIEGO_CITY_CLERK_LOG_COMMITTEES;
  const resolutions: SanDiegoCityCandidateCommitteeResolution[] = input.candidates.map((candidate) => {
    if (
      candidate.officeName === "City Council Member" &&
      !isSanDiegoCityCouncilSeatNumber(candidate.seatNumber)
    ) {
      return {
        candidate,
        status: "unmatched",
        reason: "Council candidate has no valid district seat number; cannot verify district evidence",
      };
    }

    // Clerk-log tier: an official candidate→committee mapping outranks every
    // inference, but only for the exact contest it was curated for, and the
    // export must confirm the committee (id present, name agrees, lone C,
    // no contradictory name evidence). Any contradiction fails closed rather
    // than falling through — see the module header.
    const clerkEntries = clerkLogCommittees.filter(
      (entry) =>
        entry.candidateNameKey === normalizeSanDiegoCityTextKey(candidate.displayName) &&
        entry.electionYear === candidate.electionYear &&
        entry.seatNumber === candidate.seatNumber,
    );
    if (clerkEntries.length > 1) {
      return {
        candidate,
        status: "ambiguous",
        reason: `${clerkEntries.length} clerk-log entries match this candidate; fix the curated table`,
      };
    }
    if (clerkEntries.length === 1) {
      const entry = clerkEntries[0]!;
      const committee = input.committees.find(
        (candidateCommittee) => candidateCommittee.filerId === entry.filerId,
      );
      if (committee === undefined) {
        return {
          candidate,
          status: "unmatched",
          reason: `clerk-log committee ${entry.filerId} ("${entry.committeeName}") is not in the export; review manually`,
        };
      }
      if (
        !committee.committeeNames.some(
          (name) => normalizeSanDiegoCityTextKey(name) === normalizeSanDiegoCityTextKey(entry.committeeName),
        )
      ) {
        return {
          candidate,
          status: "unmatched",
          reason: `clerk-log committee ${entry.filerId} name "${entry.committeeName}" does not match the export's [${committee.committeeNames.join("; ")}]; review manually`,
        };
      }
      if (committeeTypeGate(committee.committeeTypes) !== "candidate") {
        return {
          candidate,
          status: "unmatched",
          reason: `clerk-log committee ${entry.filerId} has committee type(s) [${committee.committeeTypes.join(", ")}], not a lone C; review manually`,
        };
      }
      if (committeeConflictsWithCandidate(committee, candidate)) {
        return {
          candidate,
          status: "unmatched",
          reason: `clerk-log committee ${entry.filerId} carries contradictory name evidence for this contest; review manually`,
        };
      }
      return {
        candidate,
        status: "matched",
        filerId: entry.filerId,
        committeeName: entry.committeeName,
        matchedBy: "clerk_log",
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
        committee.filerId !== SAN_DIEGO_PENDING_FILER_ID &&
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

    const linkable: SanDiegoCityExportCommittee[] = [];
    const blocked: { committee: SanDiegoCityExportCommittee; why: string }[] = [];
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
      if (committee.filerId === SAN_DIEGO_PENDING_FILER_ID) {
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

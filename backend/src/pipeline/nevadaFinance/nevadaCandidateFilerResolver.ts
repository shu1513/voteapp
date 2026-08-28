import { firstNamesConflict } from "../finance/personFirstNameNicknames.js";
import {
  hasMiddleNameConflict,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import type { NevadaReportListRow } from "./nevadaReportSummary.js";

// Matches VoteApp Nevada candidates to AURORA filers harvested from the
// individual search. The filer profile's Office field is the CURRENT seat, so
// candidacy office/district confirmation reads the election-year report rows'
// per-row Office column instead (Cannizzaro fixture: profile "State Senate,
// District 6", 2026 rows "Attorney General"). Ambiguity always skips: links
// are only written when the name match and the office confirmation are unique.

export function normalizeNevadaPersonName(value: string): string {
  return value
    .toLocaleUpperCase("en-US")
    .replace(/[.,'’-]/g, " ")
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function nevadaCandidateNamesMatch(candidateName: string, auroraName: string): boolean {
  const normalizedCandidate = normalizeNevadaPersonName(candidateName);
  const normalizedAurora = normalizeNevadaPersonName(auroraName);
  if (!normalizedCandidate || !normalizedAurora) return false;
  if (
    normalizedCandidate === normalizedAurora &&
    !hasMiddleNameConflict({
      candidateName,
      rowNames: [auroraName],
      normalizePersonName: normalizeNevadaPersonName,
    })
  ) {
    return true;
  }
  return personNamesMatchWithMiddleEvidence({
    candidateName,
    rowNames: [auroraName],
    normalizePersonName: normalizeNevadaPersonName,
    firstNamesEquivalent: (candidateFirst, rowFirst) => !firstNamesConflict(candidateFirst, rowFirst),
  });
}

export type NevadaParsedOffice = {
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeCanonicalName: string;
  districtNumber: number | null;
};

/**
 * Parses an AURORA report-row Office string into VoteApp office terms.
 * Unknown strings (county/city offices, blank) return null.
 */
export function parseNevadaAuroraOffice(officeText: string): NevadaParsedOffice | null {
  const text = officeText.trim().replace(/\s+/g, " ");
  if (!text) return null;

  const senate = text.match(/^State Senate, District (\d+)/i);
  if (senate) {
    return {
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      districtNumber: Number(senate[1]),
    };
  }
  const assembly = text.match(/^State Assembly, District (\d+)/i);
  if (assembly) {
    return {
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      districtNumber: Number(assembly[1]),
    };
  }
  if (/\b(justice|judge)\b/i.test(text)) {
    return { officeScope: "statewide", officeCanonicalName: "State Level Judge", districtNumber: null };
  }
  const statewide: Record<string, string> = {
    governor: "Governor",
    "lieutenant governor": "Lieutenant Governor",
    "attorney general": "Attorney General",
    "secretary of state": "Secretary of State",
    "state treasurer": "State Treasurer",
    treasurer: "State Treasurer",
    "state controller": "Comptroller",
    controller: "Comptroller",
  };
  const canonical = statewide[text.toLocaleLowerCase("en-US")];
  if (canonical) {
    return { officeScope: "statewide", officeCanonicalName: canonical, districtNumber: null };
  }
  return null;
}

export function nevadaDistrictNumberFromName(districtName: string | null | undefined): number | null {
  const match = districtName?.match(/\bDistrict (\d+)\b/);
  return match ? Number(match[1]) : null;
}

export type NevadaResolverCandidate = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateName: string;
  officeScope: string;
  officeCanonicalName: string;
  districtName: string | null;
};

export type NevadaRosterEntry = {
  name: string;
  slug: string;
  detailToken: string;
  reportRows: readonly NevadaReportListRow[];
};

export type NevadaCandidateFilerMatch = {
  candidate: NevadaResolverCandidate;
  roster: NevadaRosterEntry;
  confirmedOffice: string;
};

export type NevadaCandidateFilerSkip = {
  candidate: NevadaResolverCandidate;
  reason:
    | "no_name_match"
    | "ambiguous_roster_match"
    | "roster_entry_contested"
    | "no_election_year_reports"
    | "office_mismatch";
  detail: string;
};

export type NevadaCandidateFilerResolution = {
  matches: NevadaCandidateFilerMatch[];
  skips: NevadaCandidateFilerSkip[];
};

function officeConfirmation(
  candidate: NevadaResolverCandidate,
  roster: NevadaRosterEntry
): { ok: true; confirmedOffice: string } | { ok: false; reason: "no_election_year_reports" | "office_mismatch"; detail: string } {
  const electionYearRows = roster.reportRows.filter((row) => row.year === candidate.electionYear);
  if (electionYearRows.length === 0) {
    return {
      ok: false,
      reason: "no_election_year_reports",
      detail: `${roster.name} has no year-${candidate.electionYear} report rows to confirm the candidacy office`,
    };
  }
  const candidateDistrict = nevadaDistrictNumberFromName(candidate.districtName);
  if (candidate.officeScope !== "statewide" && candidateDistrict === null) {
    // Fail closed: without a parsed district number the per-row district
    // comparison below cannot run, and any same-office row (wrong district
    // included) would confirm the candidacy.
    return {
      ok: false,
      reason: "office_mismatch",
      detail:
        `VoteApp district ${JSON.stringify(candidate.districtName)} has no parseable district ` +
        `number; refusing to confirm a ${candidate.officeScope} candidacy without one`,
    };
  }
  for (const row of electionYearRows) {
    const parsed = parseNevadaAuroraOffice(row.office);
    if (!parsed) continue;
    if (parsed.officeScope !== candidate.officeScope) continue;
    if (parsed.officeCanonicalName !== candidate.officeCanonicalName) continue;
    if (
      parsed.districtNumber !== null &&
      candidateDistrict !== null &&
      parsed.districtNumber !== candidateDistrict
    ) {
      continue;
    }
    if (parsed.districtNumber === null && candidate.officeScope !== "statewide") continue;
    return { ok: true, confirmedOffice: row.office };
  }
  return {
    ok: false,
    reason: "office_mismatch",
    detail:
      `${roster.name}'s year-${candidate.electionYear} report offices ` +
      `[${[...new Set(electionYearRows.map((row) => row.office))].join(" | ")}] do not confirm ` +
      `${candidate.officeScope}::${candidate.officeCanonicalName} district ${candidateDistrict ?? "-"}`,
  };
}

/**
 * Name-first resolution with hard ambiguity gates: a candidate matching two
 * roster filers is skipped, and a roster filer matched by two candidates
 * skips them all (the filer name is the CSV join key, so a contested name can
 * never be linked).
 */
export function resolveNevadaCandidateFilers(input: {
  candidates: readonly NevadaResolverCandidate[];
  rosterEntries: readonly NevadaRosterEntry[];
}): NevadaCandidateFilerResolution {
  const matches: NevadaCandidateFilerMatch[] = [];
  const skips: NevadaCandidateFilerSkip[] = [];
  const rosterMatchesByCandidate = new Map<NevadaResolverCandidate, NevadaRosterEntry[]>();
  const candidateCountBySlug = new Map<string, number>();

  for (const candidate of input.candidates) {
    const matched = input.rosterEntries.filter((entry) =>
      nevadaCandidateNamesMatch(candidate.candidateName, entry.name)
    );
    rosterMatchesByCandidate.set(candidate, matched);
    for (const entry of matched) {
      candidateCountBySlug.set(entry.slug, (candidateCountBySlug.get(entry.slug) ?? 0) + 1);
    }
  }

  for (const candidate of input.candidates) {
    const matched = rosterMatchesByCandidate.get(candidate) ?? [];
    if (matched.length === 0) {
      skips.push({
        candidate,
        reason: "no_name_match",
        detail: `no AURORA filer name matches ${candidate.candidateName}`,
      });
      continue;
    }
    if (matched.length > 1) {
      skips.push({
        candidate,
        reason: "ambiguous_roster_match",
        detail: `${candidate.candidateName} matches ${matched.map((entry) => entry.name).join(" | ")}`,
      });
      continue;
    }
    const roster = matched[0];
    if ((candidateCountBySlug.get(roster.slug) ?? 0) > 1) {
      skips.push({
        candidate,
        reason: "roster_entry_contested",
        detail: `AURORA filer ${roster.name} matches multiple VoteApp candidates`,
      });
      continue;
    }
    const confirmation = officeConfirmation(candidate, roster);
    if (!confirmation.ok) {
      skips.push({ candidate, reason: confirmation.reason, detail: confirmation.detail });
      continue;
    }
    matches.push({ candidate, roster, confirmedOffice: confirmation.confirmedOffice });
  }

  return { matches, skips };
}

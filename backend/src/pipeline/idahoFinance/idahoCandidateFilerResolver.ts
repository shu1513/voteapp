// Candidate -> Idaho Sunshine registration resolution
// (docs/plans/idaho-finance.md, Phase 1). Pure functions over the candidate
// grid; the auto-link pulls the grid once per run and calls these.
//
// A registration is one candidate + one office + one election cycle, keyed by
// its guid. Rules (plan "Architecture"):
// - grid electionYear must equal the race's election year (cycle attribution
//   is the registration, never filing-year arithmetic);
// - exact grid office for the VoteApp race, plus the district evidence the
//   office kind needs: legislative district number, county, and for county
//   commissioners the seat number from the ballot title;
// - full-name evidence through the shared middle-name gate, with one-sided
//   roster->grid nickname expansion; the grid's quoted call name
//   ("Bertling, Timothy 'Tim' Paul") is offered as an alias; a bare surname
//   never links;
// - only an Active registration links automatically: a terminated
//   re-registration beside the live one is skipped, a lone terminated or
//   inactive registration is reported for manual review, and two live
//   registrations are ambiguous;
// - never link from committeeName text.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import { idahoRegistrationProfileUrl, type IdahoCandidateRegistrationRow } from "./idahoCfsClient.js";
import { idahoSunshineOfficeForRace, type IdahoSunshineOffice } from "./idahoFinanceEligibleOffices.js";

export type IdahoCandidateFilerResolverInput = {
  /** Roster spellings to try, most specific first (display name, then "First Last"). */
  candidateNames: readonly string[];
  officeScope: string;
  officeName: string;
  /** VoteApp district name ("Ada County, Idaho"); null for statewide races. */
  district: string | null;
  /** Legislative district number from the district geoid; null for other offices. */
  legislativeDistrict: number | null;
  /** Election ballot title ("County Commissioner District 2") — carries the commissioner seat. */
  ballotTitle: string | null;
  electionYear: number;
  registrations: readonly IdahoCandidateRegistrationRow[];
};

export type IdahoCandidateFilerMatch = {
  registrationGuid: string;
  filerEntityId: number;
  filerRegistrationId: number;
  filerName: string;
  /** Grid filerStatus ("Active", "Terminated", "Inactive"). */
  status: string;
  /** Grid office text. */
  officeName: string;
  /** Storage label: "16" (Senate), "16B" (House seat), "Ada" (county), "Ada 2" (commissioner); null statewide. */
  district: string | null;
  /** "name_nickname" marks a match that needed the one-sided first-name expansion. */
  confidence: "name_exact" | "name_nickname";
  source: "sunshine_grid";
  sourceUrl: string;
};

export type IdahoCandidateFilerResolution =
  | { status: "matched"; match: IdahoCandidateFilerMatch }
  | { status: "ambiguous"; reason: "multiple_active_registrations"; matches: IdahoCandidateFilerMatch[] }
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_required_district"
        | "no_registration_match"
        | "no_active_registration";
    };

/** Match-time person-name normalization (generational suffixes stripped). */
export function normalizeIdahoPersonNameForMatching(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .trim();
}

/** "Ada County, Idaho" -> "Ada"; null when the name is not a county. */
export function idahoCountyFromDistrictName(value: string | null | undefined): string | null {
  const match = /^(.+?) County, Idaho$/i.exec(value?.trim() ?? "");
  return match ? match[1]!.trim() : null;
}

/** "County Commissioner District 2" -> 2; null when the title carries no district number. */
export function idahoCommissionerDistrictFromBallotTitle(value: string | null | undefined): number | null {
  const match = /\bDistrict\s+(\d+)\b/i.exec(value ?? "");
  if (!match) return null;
  const parsed = Number.parseInt(match[1]!, 10);
  return parsed > 0 ? parsed : null;
}

/**
 * The grid row as a comma-form name the shared matcher understands:
 * "Last, First (Nick) Middle". The grid quotes call names inside filerName
 * ("Bertling, Timothy 'Tim' Paul"); the parenthetical form is the alias
 * syntax personNameParseVariants treats as a first-name substitute.
 */
export function idahoRegistrationRowName(registration: IdahoCandidateRegistrationRow): string {
  if (!registration.lastName || !registration.firstName) return registration.filerName;
  const nickname = /["']([^"']{2,})["']/.exec(registration.filerName)?.[1]?.trim() ?? null;
  const given = [registration.firstName, nickname ? `(${nickname})` : null, registration.middleName]
    .filter((part): part is string => part !== null && part.length > 0)
    .join(" ");
  return `${registration.lastName}, ${given}`;
}

// One-sided nickname expansion (roster side only, the shared module's rule):
// the grid carries legal first names while the roster carries campaign names.
function rosterFirstNameMatchesGrid(candidateFirst: string, rowFirst: string): boolean {
  return candidateFirst === rowFirst || firstNameVariants(candidateFirst).includes(rowFirst);
}

function nameConfidence(
  candidateNames: readonly string[],
  rowName: string
): IdahoCandidateFilerMatch["confidence"] | null {
  for (const candidateName of candidateNames) {
    if (
      personNamesMatchWithMiddleEvidence({
        candidateName,
        rowNames: [rowName],
        normalizePersonName: normalizeIdahoPersonNameForMatching,
      })
    ) {
      return "name_exact";
    }
  }
  for (const candidateName of candidateNames) {
    if (
      personNamesMatchWithMiddleEvidence({
        candidateName,
        rowNames: [rowName],
        normalizePersonName: normalizeIdahoPersonNameForMatching,
        firstNamesEquivalent: rosterFirstNameMatchesGrid,
      })
    ) {
      return "name_nickname";
    }
  }
  return null;
}

type DistrictEvidence =
  | { kind: "statewide" }
  | { kind: "legislative"; number: number }
  | { kind: "county"; county: string }
  | { kind: "county_commissioner"; county: string; seat: number };

function districtEvidence(
  office: IdahoSunshineOffice,
  input: Pick<IdahoCandidateFilerResolverInput, "district" | "legislativeDistrict" | "ballotTitle">
): DistrictEvidence | null {
  switch (office.districtKind) {
    case "statewide":
      return { kind: "statewide" };
    case "legislative":
      return input.legislativeDistrict !== null && input.legislativeDistrict > 0
        ? { kind: "legislative", number: input.legislativeDistrict }
        : null;
    case "county": {
      const county = idahoCountyFromDistrictName(input.district);
      return county ? { kind: "county", county } : null;
    }
    case "county_commissioner": {
      const county = idahoCountyFromDistrictName(input.district);
      const seat = idahoCommissionerDistrictFromBallotTitle(input.ballotTitle);
      return county && seat !== null ? { kind: "county_commissioner", county, seat } : null;
    }
  }
}

/** The storage district label when the grid row carries the required evidence; undefined otherwise. */
function rowDistrictLabel(row: IdahoCandidateRegistrationRow, evidence: DistrictEvidence): string | null | undefined {
  switch (evidence.kind) {
    case "statewide":
      return row.districtType === "State" ? null : undefined;
    case "legislative":
      return row.districtType === "Legislative" && row.district === `Legislative District ${evidence.number}`
        ? `${evidence.number}${row.seatZone ?? ""}`
        : undefined;
    case "county":
      return row.districtType === "County" && normalizeTextKey(row.jurisdiction) === normalizeTextKey(evidence.county)
        ? evidence.county
        : undefined;
    case "county_commissioner":
      return row.districtType === "County" &&
        normalizeTextKey(row.jurisdiction) === normalizeTextKey(evidence.county) &&
        row.seatZone?.trim() === String(evidence.seat)
        ? `${evidence.county} ${evidence.seat}`
        : undefined;
  }
}

export function resolveIdahoCandidateFiler(input: IdahoCandidateFilerResolverInput): IdahoCandidateFilerResolution {
  const candidateNames = [...new Set(input.candidateNames.map((name) => name.trim()).filter(Boolean))];
  if (candidateNames.length === 0) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }
  const office = idahoSunshineOfficeForRace({ officeScope: input.officeScope, officeCanonicalName: input.officeName });
  if (office === null) {
    return { status: "unmatched", reason: "unsupported_office" };
  }
  const evidence = districtEvidence(office, input);
  if (evidence === null) {
    return { status: "unmatched", reason: "missing_required_district" };
  }

  const matches = new Map<string, IdahoCandidateFilerMatch>();
  for (const row of input.registrations) {
    if (row.electionYear !== input.electionYear || row.office !== office.gridOffice) continue;
    const district = rowDistrictLabel(row, evidence);
    if (district === undefined) continue;
    const confidence = nameConfidence(candidateNames, idahoRegistrationRowName(row));
    if (confidence === null) continue;
    matches.set(row.registrationGuid, {
      registrationGuid: row.registrationGuid,
      filerEntityId: row.filerEntityId,
      filerRegistrationId: row.filerRegistrationId,
      filerName: row.filerName,
      status: row.status,
      officeName: row.office,
      district,
      confidence,
      source: "sunshine_grid",
      sourceUrl: idahoRegistrationProfileUrl(row.registrationGuid),
    });
  }

  const sorted = [...matches.values()].sort((left, right) => left.filerRegistrationId - right.filerRegistrationId);
  if (sorted.length === 0) {
    return { status: "unmatched", reason: "no_registration_match" };
  }
  // Only a live registration links automatically. A terminated
  // re-registration beside the live one is the common shape; a lone
  // terminated or inactive registration goes to manual review (an operator
  // can still link it — the money stays public); two live registrations for
  // one race are a filing problem, not ours to pick.
  const active = sorted.filter((match) => match.status === "Active");
  if (active.length === 1) {
    return { status: "matched", match: active[0]! };
  }
  if (active.length === 0) {
    return { status: "unmatched", reason: "no_active_registration" };
  }
  return { status: "ambiguous", reason: "multiple_active_registrations", matches: active };
}

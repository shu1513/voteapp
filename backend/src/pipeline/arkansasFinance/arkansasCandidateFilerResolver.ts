// Candidate → CFIS registration resolution for Arkansas campaign finance.
//
// Pure functions over the full registration sweep
// (PublicFilerDetails/GetCandidateCommitteDetails, every page). A match
// needs exact evidence on every axis (plan-arkansas-finance.md, Phase 2):
// candidate filer type, cycle `electionYear`, CFIS office name from the
// enumerated vocabulary, statewide-numbered district, state jurisdiction,
// and the person name with the shared middle-name / suffix gate. Party is
// corroboration only: a conflict vetoes, agreement proves nothing.
//
// Fail-closed rules: SFI filers, PACs, and exploratory committees are never
// candidates; registrations without a cycle year (every county and
// municipal filer in the live registry) never match; two filers matching
// the same candidate are ambiguous, full stop — the public registration
// row carries no identity key, so nothing (not money, not paper-filer
// status) proves two filerEntityIds are one person. Live 2026 duplicates
// (a paper-filer twin at $0 for Wooten HD59 and Caldwell SD10) are linked
// by hand through the writer.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import type { ArkansasFilerRegistrationRow } from "./arkansasCfisClient.js";
import { arkansasCfisOfficeNameForOffice } from "./arkansasFinanceEligibleOffices.js";

export type ArkansasCandidateFilerResolverInput = {
  candidateName: string;
  candidateParty?: string | null;
  officeScope: string;
  officeName: string;
  /** VoteApp district name ("State House District 68 (2024); Arkansas"). */
  district?: string | null;
  electionYear: number;
  registrationRows: readonly ArkansasFilerRegistrationRow[];
  sourceUrl?: string | null;
};

export type ArkansasCandidateFilerMatch = {
  filingEntityId: number;
  registrationGuid: string;
  /** Link display name: the committee name, else the structured person name. */
  filerName: string;
  officialName: string;
  officeName: string;
  district: string | null;
  politicalParty: string | null;
  electionYear: number;
  totalRaised: number;
  totalSpent: number;
  balanceOfFunds: number;
  confidence: "exact";
  source: "cfis_registration";
  sourceUrl: string | null;
};

export type ArkansasCandidateFilerResolution =
  | ({ status: "matched" } & ArkansasCandidateFilerMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_required_district"
        | "no_candidate_filer_match";
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      matches: ArkansasCandidateFilerMatch[];
    };

const ARKANSAS_STATE_JURISDICTION = "Arkansas";

// Stored-name normalization (the Delaware convention): diacritics stripped,
// uppercased, non-alphanumerics collapsed to single spaces.
export function normalizeArkansasCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeArkansasCandidateNameForStorage(value)
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * District number from a VoteApp legislative district name ("State House
 * District 68 (2024); Arkansas" → "68"); null when none is present.
 */
export function arkansasDistrictNumberFromDistrictName(name: string | null | undefined): string | null {
  const match = /\bDistrict\s+0*(\d+)\b/i.exec(name ?? "");
  return match ? match[1]! : null;
}

export function arkansasRegistrationDistrictNumber(value: string | null): string | null {
  const match = /^\s*0*(\d+)\s*$/.exec(value ?? "");
  return match ? match[1]! : null;
}

// "Julius Walker, Jr." — the comma marks a suffix, not a Last, First
// boundary; rewrite it so the shared parser keeps natural word order (and
// still sees the suffix for Sr-vs-Jr conflicts).
function stripSuffixComma(value: string): string {
  return value.replace(/,\s*((?:JR|SR|II|III|IV)\.?)\s*$/i, " $1");
}

// Roster names in natural order get their first token expanded through the
// shared nickname table: CFIS registrations carry legal first names
// ("Joshua" for roster "Josh Longmire", live 2026-09-02). Comma-form roster
// names are left alone (their first token is a surname).
function candidateNameVariants(candidateName: string): string[] {
  const cleaned = stripSuffixComma(candidateName).trim();
  const variants = [cleaned];
  const tokens = cleaned.split(/\s+/);
  if (tokens.length >= 2 && !cleaned.includes(",")) {
    for (const variant of firstNameVariants(normalizePersonName(tokens[0]!))) {
      variants.push([variant, ...tokens.slice(1)].join(" "));
    }
  }
  return variants;
}

// The structured firstName sometimes carries the filer's own declared
// nickname in quotes (`James "Jay"`, `Melinda "Mindy"`): both the legal
// name and the declared nickname are source-side facts, not expansions.
function structuredFirstNames(firstName: string): string[] {
  const legal = firstName.replace(/"[^"]*"/g, " ").replace(/\s+/g, " ").trim();
  const names = legal ? [legal] : [];
  for (const quoted of firstName.matchAll(/"([^"]+)"/g)) {
    const nickname = quoted[1]!.trim();
    if (nickname) names.push(nickname);
  }
  return names;
}

// The comma-form `filerName` ("Sanders, Governor. Sarah H.",
// "Hawk, Mr. . Robert J., II") is the only field with middle initials, but
// a free-text title precedes the first name. Anchor on the structured legal
// first name: everything before it is title text and is dropped; if the
// first name is not found the field contributes no evidence.
function commaFormName(row: ArkansasFilerRegistrationRow, legalFirstName: string): string | null {
  const filerName = row.filerName?.trim();
  const lastName = row.lastName?.trim();
  if (!filerName || !lastName) return null;
  const commaIndex = filerName.indexOf(",");
  if (commaIndex <= 0) return null;
  if (normalizePersonName(filerName.slice(0, commaIndex)) !== normalizePersonName(lastName)) return null;
  const rest = filerName.slice(commaIndex + 1);
  const firstToken = legalFirstName.split(/\s+/)[0]!;
  const pattern = new RegExp(`(?:^|[\\s.])${firstToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
  const anchored = pattern.exec(rest);
  if (!anchored) return null;
  const given = rest.slice(anchored.index).replace(/^[\s.]+/, "").trim();
  return given ? `${lastName}, ${given}` : null;
}

/** Person-name forms a registration row offers as identity evidence. */
export function arkansasRegistrationRowNames(row: ArkansasFilerRegistrationRow): string[] {
  const lastName = row.lastName?.trim();
  const firstName = row.firstName?.trim();
  if (!lastName || !firstName) return [];
  // The free-text suffix field holds honorifics, degrees, and stray text
  // ("Mr.", "Ph.D.", "N/A" — live 2026-09-02); only generational suffixes
  // are name evidence.
  const suffix = /^(?:JR|SR|II|III|IV)\.?$/i.test(row.suffix?.trim() ?? "") ? row.suffix!.trim() : null;
  const names: string[] = [];
  const firstNames = structuredFirstNames(firstName);
  for (const first of firstNames) {
    names.push([first, lastName, suffix].filter(Boolean).join(" "));
  }
  const legalFirstName = firstNames[0];
  if (legalFirstName) {
    const commaForm = commaFormName(row, legalFirstName);
    if (commaForm) names.push(commaForm);
  }
  return names;
}

function candidateNamesMatch(candidateName: string, rowNames: readonly string[]): boolean {
  if (rowNames.length === 0) return false;
  return candidateNameVariants(candidateName).some((variant) =>
    personNamesMatchWithMiddleEvidence({ candidateName: variant, rowNames, normalizePersonName })
  );
}

type PartyFamily = "REPUBLICAN" | "DEMOCRATIC" | "LIBERTARIAN" | "INDEPENDENT" | "GREEN";

function partyFamily(value: string | null | undefined): PartyFamily | null {
  const key = (value ?? "").toUpperCase();
  if (/REPUBLICAN/.test(key)) return "REPUBLICAN";
  if (/DEMOCRAT/.test(key)) return "DEMOCRATIC";
  if (/LIBERTARIAN/.test(key)) return "LIBERTARIAN";
  if (/INDEPENDENT/.test(key)) return "INDEPENDENT";
  if (/GREEN/.test(key)) return "GREEN";
  return null;
}

function partiesConflict(candidateParty: string | null | undefined, rowParty: string | null): boolean {
  const left = partyFamily(candidateParty);
  const right = partyFamily(rowParty);
  return left !== null && right !== null && left !== right;
}

function isCandidateRegistration(row: ArkansasFilerRegistrationRow): boolean {
  return row.filerTypeCode === "CAN";
}

function toMatch(input: {
  row: ArkansasFilerRegistrationRow;
  officialName: string;
  sourceUrl: string | null;
}): ArkansasCandidateFilerMatch {
  const { row } = input;
  return {
    filingEntityId: row.filerEntityId,
    registrationGuid: row.registrationGuid,
    filerName: row.committeeName?.trim() || input.officialName,
    officialName: input.officialName,
    officeName: row.office ?? "",
    district: arkansasRegistrationDistrictNumber(row.officeDistrictName),
    politicalParty: row.politicalParty,
    electionYear: row.electionYear ?? 0,
    totalRaised: row.totalRaised,
    totalSpent: row.totalSpent,
    balanceOfFunds: row.balanceOfFunds,
    confidence: "exact",
    source: "cfis_registration",
    sourceUrl: input.sourceUrl,
  };
}

export function resolveArkansasCandidateFiler(
  input: ArkansasCandidateFilerResolverInput
): ArkansasCandidateFilerResolution {
  if (!Number.isInteger(input.electionYear) || input.electionYear <= 0) {
    throw new Error(`Invalid Arkansas candidate filer election year: ${input.electionYear}`);
  }
  const candidateNameNormalized = normalizeArkansasCandidateNameForStorage(input.candidateName);
  if (!candidateNameNormalized) {
    return { status: "unmatched", reason: "missing_candidate_name", candidateNameNormalized };
  }
  const cfisOffice = arkansasCfisOfficeNameForOffice({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
  });
  if (cfisOffice === null) {
    return { status: "unmatched", reason: "unsupported_office", candidateNameNormalized };
  }
  const legislative = input.officeScope === "state_upper" || input.officeScope === "state_lower";
  const district = legislative ? arkansasDistrictNumberFromDistrictName(input.district) : null;
  if (legislative && district === null) {
    return { status: "unmatched", reason: "missing_required_district", candidateNameNormalized };
  }

  const matchesByEntity = new Map<number, ArkansasCandidateFilerMatch>();
  for (const row of input.registrationRows) {
    if (!isCandidateRegistration(row) || row.electionYear !== input.electionYear) continue;
    if (row.office !== cfisOffice || row.jurisdictionName !== ARKANSAS_STATE_JURISDICTION) continue;
    if (arkansasRegistrationDistrictNumber(row.officeDistrictName) !== district) continue;
    if (partiesConflict(input.candidateParty, row.politicalParty)) continue;
    const rowNames = arkansasRegistrationRowNames(row);
    if (!candidateNamesMatch(input.candidateName, rowNames)) continue;
    if (matchesByEntity.has(row.filerEntityId)) {
      throw new Error(
        `Arkansas registration sweep carries entity ${row.filerEntityId} twice for cycle ${input.electionYear}`
      );
    }
    matchesByEntity.set(
      row.filerEntityId,
      toMatch({ row, officialName: rowNames[0]!, sourceUrl: input.sourceUrl ?? null })
    );
  }

  const matches = [...matchesByEntity.values()].sort((left, right) => left.filingEntityId - right.filingEntityId);
  if (matches.length === 0) {
    return { status: "unmatched", reason: "no_candidate_filer_match", candidateNameNormalized };
  }
  if (matches.length === 1) {
    return { status: "matched", ...matches[0]! };
  }
  return { status: "ambiguous", reason: "multiple_matching_filers", candidateNameNormalized, matches };
}

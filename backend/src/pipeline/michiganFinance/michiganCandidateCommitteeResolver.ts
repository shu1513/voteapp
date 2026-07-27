import {
  type MichiganMitnOfficeSearchInput,
  normalizeMichiganMitnLegislativeDistrict,
  toMichiganMitnOfficeSearchInput,
} from "./michiganFinanceEligibleOffices.js";
import { normalizeMichiganMitnLegacyArchiveYear } from "./michiganMitnLegacyRowTypes.js";
import type { MichiganMitnLegacyContributionRow } from "./michiganMitnLegacyRowTypes.js";

export type MichiganCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  /**
   * The candidate's current office from the VoteApp profile, when known.
   * Michigan is a one-candidate-one-committee state (MCL 169.221), so an
   * office-mover's committee keeps a name mentioning their PREVIOUS office —
   * a committee name matching the candidate's current office is the same
   * person's committee, not a conflicting one.
   */
  currentOffice?: string | null;
  contributionRows: readonly MichiganMitnLegacyContributionRow[];
  sourceUrl?: string | null;
};

export type MichiganCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  commonName: string | null;
  confidence: "exact";
  source: "mitn";
  sourceUrl: string | null;
  matchedContributionRowCount: number;
};

export type MichiganCandidateCommitteeResolution =
  | ({ status: "matched" } & MichiganCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "no_candidate_committee_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: MichiganCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  committeeId: string;
  committeeName: string;
  commonName: string | null;
  rows: MichiganMitnLegacyContributionRow[];
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|COMMITTEE|FRIENDS|TO|ELECT)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeMichiganCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  function addName(raw: string): void {
    const hasComma = raw.includes(",");
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
    }

    const parts = normalized.split(" ").filter(Boolean);
    if (!hasComma && parts.length >= 2) {
      keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
    }

    const commaParts = raw
      .split(",")
      .map((part) => normalizePersonName(part))
      .filter(Boolean);
    if (commaParts.length >= 2) {
      const lastName = commaParts[0] ?? "";
      const firstNames = commaParts.slice(1).join(" ").trim();
      const flipped = normalizePersonName(`${firstNames} ${lastName}`);
      if (flipped) {
        keys.add(flipped);
        const flippedParts = flipped.split(" ").filter(Boolean);
        if (flippedParts.length >= 2) {
          keys.add(`${flippedParts[0]} ${flippedParts[flippedParts.length - 1]}`);
        }
      }
    }
  }

  addName(trimmed.replace(/\([^()]+\)/g, " "));
  for (const match of trimmed.matchAll(/\(([^()]+)\)/g)) {
    if (match[1]) {
      addName(match[1]);
    }
  }

  return keys;
}

function candidateNameNormalized(value: string): string {
  return [...normalizeMichiganCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

export function normalizeMichiganCandidateNameForStorage(value: string): string {
  return candidateNameNormalized(value);
}

function candidateNameKeysFromContributionRow(row: MichiganMitnLegacyContributionRow): Set<string> {
  const structuredName = [row.can_first_name, row.can_last_name].filter(Boolean).join(" ");
  return normalizeMichiganCandidateNameKeys(structuredName);
}

function rowMatchesCandidateName(input: {
  row: MichiganMitnLegacyContributionRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of candidateNameKeysFromContributionRow(input.row)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function committeeNameFromRow(row: MichiganMitnLegacyContributionRow): string {
  return row.com_legal_name.trim() || row.common_name.trim();
}

/**
 * The state's own committee-type designation: CAN = candidate committee,
 * GUB = gubernatorial candidate committee. Everything else (IND, POL, COU,
 * STA, BAL, DIS) is not a candidate's own committee even when it prints a
 * candidate name on a row.
 */
const MICHIGAN_CANDIDATE_COMMITTEE_TYPES = new Set(["CAN", "GUB"]);

function isLikelyCandidateCommitteeRow(row: MichiganMitnLegacyContributionRow): boolean {
  if (!row.cfr_com_id.trim() || !committeeNameFromRow(row)) {
    return false;
  }
  if (!row.can_first_name.trim() || !row.can_last_name.trim()) {
    return false;
  }
  return MICHIGAN_CANDIDATE_COMMITTEE_TYPES.has(row.com_type.trim().toUpperCase());
}

type MichiganOfficeClass =
  | "governor"
  | "lieutenant_governor"
  | "secretary_of_state"
  | "attorney_general"
  | "senate"
  | "house";

// Patterns run against normalizeTextKey output, which strips OF/FOR/THE —
// "SECRETARY OF STATE" arrives as "SECRETARY STATE". Multi-word classes are
// tested first and consumed so "LIEUTENANT GOVERNOR" does not also claim
// "GOVERNOR".
const MICHIGAN_OFFICE_CLASS_PATTERNS: readonly (readonly [MichiganOfficeClass, RegExp])[] = [
  ["lieutenant_governor", /\b(?:LIEUTENANT|LT) GOVERNOR\b/],
  ["secretary_of_state", /\bSECRETARY STATE\b/],
  ["attorney_general", /\bATTORNEY GENERAL\b/],
  ["governor", /\bGOVERNOR\b/],
  ["senate", /\bSEN(?:ATE|ATOR)\b/],
  ["house", /\bSTATE HOUSE\b|\bSTATE REP(?:RESENTATIVE)?\b|\bREPRESENTATIVE\b|\bHOUSE\b/],
];

function officeClassesFromText(normalizedText: string): Set<MichiganOfficeClass> {
  const classes = new Set<MichiganOfficeClass>();
  let remaining = normalizedText;
  for (const [officeClass, pattern] of MICHIGAN_OFFICE_CLASS_PATTERNS) {
    if (pattern.test(remaining)) {
      classes.add(officeClass);
      remaining = remaining.replace(new RegExp(pattern.source, "g"), " ");
    }
  }
  return classes;
}

function districtClaimsFromText(normalizedText: string): Set<string> {
  const districts = new Set<string>();
  for (const match of normalizedText.matchAll(/\b(?:DISTRICT|DIST|HD|SD)\s*0*([1-9][0-9]{0,2})\b/g)) {
    if (match[1]) {
      districts.add(match[1]);
    }
  }
  for (const match of normalizedText.matchAll(/\b0*([1-9][0-9]{0,2})(?:ST|ND|RD|TH)?\s+(?:SENATE|HOUSE)?\s*DIST(?:RICT)?\b/g)) {
    if (match[1]) {
      districts.add(match[1]);
    }
  }
  return districts;
}

function officeClassForMitnOffice(mitnOffice: string): MichiganOfficeClass | null {
  switch (mitnOffice) {
    case "Governor":
      return "governor";
    case "Lieutenant Governor":
      return "lieutenant_governor";
    case "Secretary of State":
      return "secretary_of_state";
    case "Attorney General":
      return "attorney_general";
    case "State Senate":
      return "senate";
    case "State House":
      return "house";
    default:
      return null;
  }
}

/**
 * Michigan committee names rarely carry an office and never a district
 * ("ANGELA JONES COMMITTEE TO ELECT"), so office text works as a VETO, not a
 * requirement: identity rests on the state's own CAN/GUB designation plus the
 * structured candidate-name attribution. A committee whose text claims a
 * different office (or district) than the race is refused — unless the claim
 * matches the candidate's CURRENT office, which marks an office-mover's
 * committee (one committee per candidate under MCL 169.221), not a stranger's.
 */
function rowOfficeContextAllows(input: {
  row: MichiganMitnLegacyContributionRow;
  officeSearchInput: MichiganMitnOfficeSearchInput;
  allowedOfficeClasses: ReadonlySet<MichiganOfficeClass>;
  allowedDistricts: ReadonlySet<string>;
}): boolean {
  const text = normalizeTextKey(
    [committeeNameFromRow(input.row), input.row.common_name, input.row.extra_desc].join(" ")
  );
  const claimedClasses = officeClassesFromText(text);
  if (claimedClasses.size > 0 && [...claimedClasses].every((claimed) => !input.allowedOfficeClasses.has(claimed))) {
    return false;
  }
  const claimedDistricts = districtClaimsFromText(text);
  if (claimedDistricts.size > 0 && [...claimedDistricts].every((claimed) => !input.allowedDistricts.has(claimed))) {
    return false;
  }
  return true;
}

function isLegislativeInput(input: { officeScope: string; officeName: string }): boolean {
  return (
    (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") ||
    (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator")
  );
}

function hasValidLegislativeDistrict(input: { officeScope: string; officeName: string; district?: string | null }): boolean {
  if (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") {
    return normalizeMichiganMitnLegislativeDistrict(input.district, 38) !== null;
  }
  if (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator") {
    return normalizeMichiganMitnLegislativeDistrict(input.district, 110) !== null;
  }
  return true;
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  sourceUrl: string | null;
}): MichiganCandidateCommitteeMatch {
  return {
    committeeId: input.accumulator.committeeId,
    committeeName: input.accumulator.committeeName,
    commonName: input.accumulator.commonName,
    confidence: "exact",
    source: "mitn",
    sourceUrl: input.sourceUrl,
    matchedContributionRowCount: input.accumulator.rows.length,
  };
}

export function resolveMichiganCandidateCommittee(
  input: MichiganCandidateCommitteeResolverInput
): MichiganCandidateCommitteeResolution {
  normalizeMichiganMitnLegacyArchiveYear(input.electionYear);
  const candidateNameKeys = normalizeMichiganCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toMichiganMitnOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.mitnOffice ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeSearchInput) {
    return {
      status: "unmatched",
      reason:
        isLegislativeInput(input) && !hasValidLegislativeDistrict(input)
          ? "missing_legislative_district"
          : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const allowedOfficeClasses = new Set<MichiganOfficeClass>();
  const targetOfficeClass = officeClassForMitnOffice(officeSearchInput.mitnOffice);
  if (targetOfficeClass) {
    allowedOfficeClasses.add(targetOfficeClass);
  }
  const allowedDistricts = new Set<string>();
  if (officeSearchInput.district) {
    allowedDistricts.add(officeSearchInput.district.replace(/^0+/, ""));
  }
  const currentOfficeText = normalizeTextKey(input.currentOffice ?? "");
  if (currentOfficeText) {
    for (const officeClass of officeClassesFromText(currentOfficeText)) {
      allowedOfficeClasses.add(officeClass);
    }
    for (const district of districtClaimsFromText(currentOfficeText)) {
      allowedDistricts.add(district);
    }
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();
  for (const row of input.contributionRows) {
    const committeeId = row.cfr_com_id.trim().toUpperCase();
    const committeeName = committeeNameFromRow(row);
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isLikelyCandidateCommitteeRow(row)) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
      continue;
    }
    if (!rowOfficeContextAllows({ row, officeSearchInput, allowedOfficeClasses, allowedDistricts })) {
      continue;
    }

    const accumulator = rowsByCommittee.get(committeeId) ?? {
      committeeId,
      committeeName,
      commonName: row.common_name.trim() || null,
      rows: [],
    };
    accumulator.rows.push(row);
    rowsByCommittee.set(committeeId, accumulator);
  }

  if (rowsByCommittee.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const sourceUrl = input.sourceUrl?.trim() || null;
  const matches = [...rowsByCommittee.values()]
    .map((accumulator) => toCommitteeMatch({ accumulator, sourceUrl }))
    .sort((left, right) => left.committeeId.localeCompare(right.committeeId));

  if (matches.length === 1) {
    return {
      status: "matched",
      ...matches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_committees",
    candidateNameNormalized: candidateNameKey,
    officeNameNormalized,
    matches,
  };
}

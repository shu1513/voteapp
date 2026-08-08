import {
  fetchGeorgiaCandidateIndexRows,
  GEORGIA_ETHICS_RECORDS_SEARCH_URL,
  type GeorgiaCandidateIndexRow,
  type GeorgiaEthicsTransport,
} from "./georgiaEthicsClient.js";

// Resolves an app candidate election to its PeachFile candidate registration
// (georgia_plan.md D2/D7: link identity is the PeachFile filerEntityId, so
// resolution runs against the PeachFile candidate index only — F3). Exact
// single-registration matches only; anything else fails closed (tennessee
// pattern). The archive side of the identity chain is discovered later, at
// map-building time, not at link time.

export type GeorgiaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  candidateIndexRows: readonly GeorgiaCandidateIndexRow[];
};

export type GeorgiaCandidateCommitteeSearchInput = Omit<
  GeorgiaCandidateCommitteeResolverInput,
  "candidateIndexRows"
>;

export type GeorgiaCandidateCommitteeMatch = {
  filerEntityId: string;
  registrationGuid: string;
  committeeName: string;
  filerName: string;
  office: string | null;
  districtName: string | null;
  filerStatusCode: string | null;
  confidence: "exact";
  source: "peachfile_candidate_index";
  sourceUrl: string;
  matchedRowCount: number;
};

export type GeorgiaCandidateCommitteeResolution =
  | ({ status: "matched" } & GeorgiaCandidateCommitteeMatch)
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
      reason: "multiple_matching_registrations";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: GeorgiaCandidateCommitteeMatch[];
    };

// PeachFile office labels for the eligible offices (georgia_plan.md D9).
// Labels marked "observed" are pinned from spike bytes — 1,100+ IE target
// rows plus report inventories. Agriculture and Labor never appeared in the
// probed sample, so both plausible orderings are listed; a wrong alias can
// only produce unmatched (fail closed), never a wrong link, because the
// candidate-name and cycle gates still apply.
const PEACHFILE_OFFICE_LABELS_BY_OFFICE_KEY: Readonly<Record<string, readonly string[]>> = {
  "statewide::Governor": ["Governor"], // observed
  "statewide::Lieutenant Governor": ["Lieutenant Governor"], // observed
  "statewide::Secretary of State": ["Secretary of State"], // observed
  "statewide::Attorney General": ["Attorney General"], // observed
  "statewide::Commissioner of Agriculture": ["Commissioner of Agriculture", "Agriculture Commissioner"],
  "statewide::Commissioner of Insurance": ["Commissioner of Insurance"], // observed
  "statewide::Labor Commissioner": ["Commissioner of Labor", "Labor Commissioner"],
  "statewide::Superintendent of Public Instruction": [
    "State School Superintendent", // observed
    "Superintendent of Public Instruction",
  ],
  "statewide::Public Service Commissioner": ["Public Service Commissioner"], // observed
  "state_upper::State Senator": ["State Senator"], // observed
  "state_lower::State Lower Chamber Legislator": ["State Representative"], // observed
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Builds the set of comparable keys for a person name: the normalized full
// name, first+last, and — for "Last, First" comma forms — the flipped
// variants. PeachFile index rows render the person as "Carr, Christopher M."
// while app candidates are "Christopher Carr", so both sides expand to keys
// and match on any intersection.
export function normalizeGeorgiaCandidateNameKeys(value: string): Set<string> {
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

export function normalizeGeorgiaCandidateNameForStorage(value: string): string {
  const keys = normalizeGeorgiaCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

function candidateNameNormalized(value: string): string {
  return normalizeGeorgiaCandidateNameForStorage(value);
}

// Last-name search token: PeachFile's candidate-index filerName filter is a
// case-insensitive substring over the person display name (A3), so the last
// name alone recalls every relevant row without middle-name misses.
export function georgiaLastNameSearchToken(candidateName: string): string {
  const trimmed = candidateName.replace(/\([^()]+\)/g, " ").trim();
  if (trimmed.includes(",")) {
    const commaFirst = normalizePersonName(trimmed.split(",", 1)[0]);
    if (commaFirst) {
      return commaFirst;
    }
  }
  const normalized = normalizePersonName(trimmed);
  return normalized.split(/\s+/).filter(Boolean).at(-1) ?? trimmed;
}

function normalizeGeorgiaDistrict(value: string | null | undefined): string | null {
  const digits = (value ?? "").replace(/\D+/g, "").replace(/^0+/, "");
  return digits || null;
}

function rowMatchesCandidateName(row: GeorgiaCandidateIndexRow, candidateNameKeys: ReadonlySet<string>): boolean {
  const names = [
    row.filerName,
    row.ballotFullName,
    [row.candidateFirstName, row.candidateLastName].filter(Boolean).join(" "),
  ].filter((name): name is string => Boolean(name && name.trim()));
  for (const name of names) {
    for (const key of normalizeGeorgiaCandidateNameKeys(name)) {
      if (candidateNameKeys.has(key)) {
        return true;
      }
    }
  }
  return false;
}

// A PeachFile registration belongs to the requested election cycle when
// either cycle label leads with the election year ("2026 Candidate/Committee
// Filing Cycle", "2026 Georgia State Election").
function rowMatchesElectionYear(row: GeorgiaCandidateIndexRow, electionYear: number): boolean {
  const yearPrefix = `${electionYear} `;
  return [row.electionCycleName, row.filingCycleName].some(
    (name) => typeof name === "string" && name.startsWith(yearPrefix)
  );
}

type RegistrationAccumulator = {
  row: GeorgiaCandidateIndexRow;
  rowCount: number;
};

export function resolveGeorgiaCandidateCommittee(
  input: GeorgiaCandidateCommitteeResolverInput
): GeorgiaCandidateCommitteeResolution {
  if (!Number.isInteger(input.electionYear) || input.electionYear < 2000 || input.electionYear > 2100) {
    throw new Error(`Invalid Georgia candidate committee election year: ${input.electionYear}`);
  }
  const candidateNameKeys = normalizeGeorgiaCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeKey = `${input.officeScope.trim()}::${input.officeName.trim()}`;
  const officeLabels = PEACHFILE_OFFICE_LABELS_BY_OFFICE_KEY[officeKey];
  const officeNameNormalized = normalizeTextKey(input.officeName);
  const isLegislativeOffice = input.officeScope === "state_upper" || input.officeScope === "state_lower";
  const expectedDistrict = isLegislativeOffice ? normalizeGeorgiaDistrict(input.district) : null;

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeLabels) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (isLegislativeOffice && !expectedDistrict) {
    return {
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const officeLabelKeys = new Set(officeLabels.map((label) => normalizeTextKey(label)));
  const matchesByRegistration = new Map<string, RegistrationAccumulator>();
  for (const row of input.candidateIndexRows) {
    if (!rowMatchesElectionYear(row, input.electionYear)) {
      continue;
    }
    if (!officeLabelKeys.has(normalizeTextKey(row.office))) {
      continue;
    }
    if (expectedDistrict !== null && normalizeGeorgiaDistrict(row.districtName) !== expectedDistrict) {
      continue;
    }
    if (!rowMatchesCandidateName(row, candidateNameKeys)) {
      continue;
    }
    const registrationGuid = row.guid.trim().toLowerCase();
    const existing = matchesByRegistration.get(registrationGuid);
    if (existing) {
      existing.rowCount += 1;
    } else {
      matchesByRegistration.set(registrationGuid, { row, rowCount: 1 });
    }
  }

  if (matchesByRegistration.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matches = [...matchesByRegistration.entries()].map(([registrationGuid, accumulator]) => ({
    filerEntityId: String(accumulator.row.filerEntityId),
    registrationGuid,
    committeeName: accumulator.row.committeeName?.trim() || accumulator.row.filerName,
    filerName: accumulator.row.filerName,
    office: accumulator.row.office,
    districtName: accumulator.row.districtName,
    filerStatusCode: accumulator.row.filerStatusCode,
    confidence: "exact" as const,
    source: "peachfile_candidate_index" as const,
    sourceUrl: GEORGIA_ETHICS_RECORDS_SEARCH_URL,
    matchedRowCount: accumulator.rowCount,
  }));

  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_registrations",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
      matches,
    };
  }
  return { status: "matched", ...matches[0]! };
}

export async function searchAndResolveGeorgiaCandidateCommittee(
  input: GeorgiaCandidateCommitteeSearchInput,
  transport: GeorgiaEthicsTransport
): Promise<GeorgiaCandidateCommitteeResolution> {
  const officeKey = `${input.officeScope.trim()}::${input.officeName.trim()}`;
  if (!PEACHFILE_OFFICE_LABELS_BY_OFFICE_KEY[officeKey]) {
    // Skip the network round-trip for offices the module does not cover.
    return resolveGeorgiaCandidateCommittee({ ...input, candidateIndexRows: [] });
  }
  const candidateIndexRows = await fetchGeorgiaCandidateIndexRows(transport, "peachfile", {
    filerName: georgiaLastNameSearchToken(input.candidateName),
  });
  return resolveGeorgiaCandidateCommittee({ ...input, candidateIndexRows });
}

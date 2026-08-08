import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
import {
  searchMassachusettsOcpfCandidateFilers,
  type MassachusettsOcpfCandidateFiler,
  type MassachusettsOcpfClientOptions,
} from "./massachusettsOcpfClient.js";
import {
  normalizeMassachusettsOcpfDistrict,
  mapMassachusettsOcpfOffice,
  toMassachusettsOcpfOfficeSearchInput,
} from "./massachusettsFinanceEligibleOffices.js";

export type MassachusettsCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  filers: readonly MassachusettsOcpfCandidateFiler[];
};

export type MassachusettsCandidateCommitteeSearchInput = Omit<MassachusettsCandidateCommitteeResolverInput, "filers">;

export type MassachusettsCandidateCommitteeMatch = {
  candidateCpfId: string;
  filerName: string;
  committeeName: string | null;
  officeSought: string | null;
  confidence: "exact";
  source: "ocpf_api";
  sourceUrl: string | null;
  matchedFilerRowCount: number;
};

export type MassachusettsCandidateCommitteeResolution =
  | ({ status: "matched" } & MassachusettsCandidateCommitteeMatch)
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
      matches: MassachusettsCandidateCommitteeMatch[];
    };

type CandidateFilerAccumulator = {
  candidateCpfId: string;
  filerName: string;
  committeeName: string | null;
  officeSought: string | null;
  rows: MassachusettsOcpfCandidateFiler[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Massachusetts candidate committee election year: ${value}`);
  }
  return value;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|COMMITTEE|FRIENDS)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeMassachusettsCandidateNameKeys(value: string): Set<string> {
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
  return [...normalizeMassachusettsCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

function filerMatchesCandidateName(input: {
  filer: MassachusettsOcpfCandidateFiler;
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const names = [input.filer.filerName, input.filer.filerNameReverse, input.filer.committeeName].filter(
    (name): name is string => typeof name === "string" && name.trim().length > 0
  );
  let keyMatched = false;
  for (const name of names) {
    for (const key of normalizeMassachusettsCandidateNameKeys(name)) {
      if (input.candidateNameKeys.has(key)) {
        keyMatched = true;
        break;
      }
    }
    if (keyMatched) {
      break;
    }
  }
  if (!keyMatched) {
    return false;
  }
  // Key overlap collapses names to first+last, which would link
  // "John A. Smith" to "Smith, John B." as an "exact" match whenever the
  // office and district agree. A contradicting middle name rejects the filer
  // (georgia pattern).
  return !hasMiddleNameConflict({
    candidateName: input.candidateName,
    rowNames: names,
    normalizePersonName,
  });
}

function statusText(value: string | undefined): string {
  return normalizeTextKey(value ?? "");
}

function isCandidateFilerUsable(filer: MassachusettsOcpfCandidateFiler): boolean {
  if (!filer.cpfId.trim() || !filer.filerName.trim()) {
    return false;
  }
  if (filer.isCandidate === false || filer.isActive === false) {
    return false;
  }
  const accountType = statusText(`${filer.accountTypeCode ?? ""} ${filer.accountTypeDescription ?? ""}`);
  if (accountType && !/\b(CANDIDATE|DEPOSITORY)\b/.test(accountType)) {
    return false;
  }
  return true;
}

function filerMatchesExpectedOffice(input: {
  filer: MassachusettsOcpfCandidateFiler;
  expectedOcpfOffice: string;
  expectedDistrict: string | null;
}): boolean {
  const mappedOffice = mapMassachusettsOcpfOffice({ officeSought: input.filer.officeSought });
  if (!mappedOffice || mappedOffice.ocpfOffice !== input.expectedOcpfOffice) {
    return false;
  }
  if (input.expectedDistrict !== null) {
    return mappedOffice.district === input.expectedDistrict;
  }
  return true;
}

function toCommitteeMatch(accumulator: CandidateFilerAccumulator): MassachusettsCandidateCommitteeMatch {
  return {
    candidateCpfId: accumulator.candidateCpfId,
    filerName: accumulator.filerName,
    committeeName: accumulator.committeeName,
    officeSought: accumulator.officeSought,
    confidence: "exact",
    source: "ocpf_api",
    sourceUrl: null,
    matchedFilerRowCount: accumulator.rows.length,
  };
}

export function resolveMassachusettsCandidateCommittee(
  input: MassachusettsCandidateCommitteeResolverInput
): MassachusettsCandidateCommitteeResolution {
  normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeMassachusettsCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toMassachusettsOcpfOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.ocpfOffice ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeSearchInput) {
    const isLegislativeOffice =
      (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") ||
      (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator");
    const hasDistrict = normalizeMassachusettsOcpfDistrict(input.district) !== null;
    return {
      status: "unmatched",
      reason: isLegislativeOffice && !hasDistrict ? "missing_legislative_district" : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByCpfId = new Map<string, CandidateFilerAccumulator>();
  for (const filer of input.filers) {
    const candidateCpfId = filer.cpfId.trim();
    const filerName = filer.filerName.trim();
    if (!candidateCpfId || !filerName) {
      continue;
    }
    if (!isCandidateFilerUsable(filer)) {
      continue;
    }
    if (
      !filerMatchesExpectedOffice({
        filer,
        expectedOcpfOffice: officeSearchInput.ocpfOffice,
        expectedDistrict: officeSearchInput.district,
      })
    ) {
      continue;
    }
    if (!filerMatchesCandidateName({ filer, candidateName: input.candidateName, candidateNameKeys })) {
      continue;
    }

    const accumulator = rowsByCpfId.get(candidateCpfId) ?? {
      candidateCpfId,
      filerName,
      committeeName: filer.committeeName?.trim() || null,
      officeSought: filer.officeSought?.trim() || null,
      rows: [],
    };
    accumulator.rows.push(filer);
    rowsByCpfId.set(candidateCpfId, accumulator);
  }

  if (rowsByCpfId.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matches = [...rowsByCpfId.values()]
    .map(toCommitteeMatch)
    .sort((left, right) => left.candidateCpfId.localeCompare(right.candidateCpfId));

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

export async function searchAndResolveMassachusettsCandidateCommittee(
  input: MassachusettsCandidateCommitteeSearchInput,
  options: MassachusettsOcpfClientOptions = {}
): Promise<MassachusettsCandidateCommitteeResolution> {
  const officeSearchInput = toMassachusettsOcpfOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  if (!officeSearchInput) {
    return resolveMassachusettsCandidateCommittee({ ...input, filers: [] });
  }

  const filers = await searchMassachusettsOcpfCandidateFilers(
    {
      searchPhrase: input.candidateName,
    },
    options
  );
  return resolveMassachusettsCandidateCommittee({ ...input, filers });
}

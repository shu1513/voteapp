import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
import {
  searchWashingtonPdcCandidateSummaries,
  type WashingtonPdcCandidateSummary,
  type WashingtonPdcClientOptions,
} from "./washingtonPdcClient.js";
import {
  isWashingtonFinanceEligibleOffice,
  normalizeWashingtonPdcJurisdiction,
  normalizeWashingtonPdcLegislativeDistrict,
  mapWashingtonPdcOffice,
  toWashingtonPdcOfficeSearchInput,
  type WashingtonPdcOfficeSearchInput,
} from "./washingtonFinanceEligibleOffices.js";

export type WashingtonCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  legislativeDistrict?: string | null;
  // Place-scope (city) offices: the VoteApp district name ("Seattle city,
  // Washington") and the seat parsed from the ballot title. Never an election
  // date — PDC candidacy rows carry the general date, so a primary-election
  // row must still match.
  jurisdiction?: string | null;
  position?: string | null;
  summaries: readonly WashingtonPdcCandidateSummary[];
};

export type WashingtonCandidateCommitteeSearchInput = Omit<WashingtonCandidateCommitteeResolverInput, "summaries">;

export type WashingtonCandidateCommitteeMatch = {
  filerId: string;
  committeeId: string;
  committeeName: string;
  candidacyId?: string;
  contributionsAmount?: number;
  expendituresAmount?: number;
  independentExpendituresForAmount?: number;
  independentExpendituresAgainstAmount?: number;
  confidence: "exact";
  source: "pdc_api";
  sourceUrl: string | null;
  matchedSummaryRowCount: number;
};

export type WashingtonCandidateCommitteeResolution =
  | ({ status: "matched" } & WashingtonCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "missing_jurisdiction"
        | "no_candidate_committee_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: WashingtonCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  filerId: string;
  committeeId: string;
  committeeName: string;
  candidacyId?: string;
  sourceUrl: string | null;
  rows: WashingtonPdcCandidateSummary[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Washington candidate committee election year: ${value}`);
  }
  return value;
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeTextKey(value)
    // Bare "V" is a middle initial, not a suffix (GENERATIONAL_SUFFIX_RANK in
    // finance/personNameMiddleEvidence.ts): stripping it erased middle evidence.
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeWashingtonCandidateNameKeys(value: string): Set<string> {
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

  const parentheticalMatches = trimmed.matchAll(/\(([^()]+)\)/g);
  for (const match of parentheticalMatches) {
    if (match[1]) {
      addName(match[1]);
    }
  }

  return keys;
}

function candidateNameNormalized(value: string): string {
  return [...normalizeWashingtonCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

function candidateSummaryMatchesName(input: {
  summary: WashingtonPdcCandidateSummary;
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  let keyMatched = false;
  for (const key of normalizeWashingtonCandidateNameKeys(input.summary.filerName)) {
    if (input.candidateNameKeys.has(key)) {
      keyMatched = true;
      break;
    }
  }
  if (!keyMatched) {
    return false;
  }
  // Key overlap collapses names to first+last, so filer "Robert B. Ferguson"
  // would match candidate "Robert W. Ferguson" as an "exact" committee
  // whenever office, district, and year agree. A contradicting middle name
  // rejects the summary; parenthetical public names ("(Bob Ferguson)") are
  // parsed as their own variant, so a nickname alias still corroborates.
  return !hasMiddleNameConflict({
    candidateName: input.candidateName,
    rowNames: [input.summary.filerName],
    normalizePersonName,
  });
}

function statusText(value: string | undefined): string {
  return normalizeTextKey(value ?? "");
}

function isWithdrawnOrInactiveCandidate(summary: WashingtonPdcCandidateSummary): boolean {
  const status = statusText(summary.candidateCommitteeStatus);
  return /\b(WITHDREW|WITHDRAWN|TERMINATED|INACTIVE)\b/.test(status);
}

function summaryMatchesExpectedOffice(input: {
  summary: WashingtonPdcCandidateSummary;
  officeSearch: WashingtonPdcOfficeSearchInput;
}): boolean {
  const mappedOffice = mapWashingtonPdcOffice({
    office: input.summary.office,
    legislativeDistrict: input.summary.legislativeDistrict,
    jurisdiction: input.summary.jurisdiction,
    position: input.summary.position,
  });
  if (!mappedOffice || mappedOffice.pdcOffice !== input.officeSearch.pdcOffice) {
    return false;
  }
  if (input.officeSearch.legislativeDistrict !== null) {
    return mappedOffice.legislativeDistrict === input.officeSearch.legislativeDistrict;
  }
  if (input.officeSearch.requiresJurisdiction) {
    if (mappedOffice.jurisdiction !== input.officeSearch.jurisdiction) {
      return false;
    }
    // Seat agreement is required only when both sides carry one: PDC's
    // position column is authoritative for council and municipal-court seats,
    // while mayor and city attorney have no position at all.
    if (input.officeSearch.position !== null && mappedOffice.position !== null) {
      return mappedOffice.position === input.officeSearch.position;
    }
  }
  return true;
}

function isCandidateSummaryUsable(summary: WashingtonPdcCandidateSummary): boolean {
  if (!summary.filerId.trim() || !summary.committeeId?.trim() || !summary.filerName.trim()) {
    return false;
  }
  if (summary.committeeCategory && normalizeTextKey(summary.committeeCategory) !== "CANDIDATE") {
    return false;
  }
  if (summary.activeCandidate === false) {
    return false;
  }
  if (isWithdrawnOrInactiveCandidate(summary)) {
    return false;
  }
  return true;
}

function toCommitteeMatch(accumulator: CandidateCommitteeAccumulator): WashingtonCandidateCommitteeMatch {
  const summaryWithTotals = accumulator.rows.find(
    (row) =>
      row.contributionsAmount !== undefined ||
      row.expendituresAmount !== undefined ||
      row.independentExpendituresForAmount !== undefined ||
      row.independentExpendituresAgainstAmount !== undefined
  );
  return {
    filerId: accumulator.filerId,
    committeeId: accumulator.committeeId,
    committeeName: accumulator.committeeName,
    ...(accumulator.candidacyId ? { candidacyId: accumulator.candidacyId } : {}),
    ...(summaryWithTotals?.contributionsAmount !== undefined
      ? { contributionsAmount: summaryWithTotals.contributionsAmount }
      : {}),
    ...(summaryWithTotals?.expendituresAmount !== undefined
      ? { expendituresAmount: summaryWithTotals.expendituresAmount }
      : {}),
    ...(summaryWithTotals?.independentExpendituresForAmount !== undefined
      ? { independentExpendituresForAmount: summaryWithTotals.independentExpendituresForAmount }
      : {}),
    ...(summaryWithTotals?.independentExpendituresAgainstAmount !== undefined
      ? { independentExpendituresAgainstAmount: summaryWithTotals.independentExpendituresAgainstAmount }
      : {}),
    confidence: "exact",
    source: "pdc_api",
    sourceUrl: accumulator.sourceUrl,
    matchedSummaryRowCount: accumulator.rows.length,
  };
}

export function resolveWashingtonCandidateCommittee(
  input: WashingtonCandidateCommitteeResolverInput
): WashingtonCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeWashingtonCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toWashingtonPdcOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    legislativeDistrict: input.legislativeDistrict,
    jurisdiction: input.jurisdiction,
    position: input.position,
  });
  const officeNameNormalized = officeSearchInput?.pdcOffice ?? normalizeTextKey(input.officeName);

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
    const hasDistrict = normalizeWashingtonPdcLegislativeDistrict(input.legislativeDistrict) !== null;
    const isPlaceOffice =
      input.officeScope.trim() === "place" &&
      isWashingtonFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName });
    const hasJurisdiction = normalizeWashingtonPdcJurisdiction(input.jurisdiction) !== null;
    return {
      status: "unmatched",
      reason:
        isLegislativeOffice && !hasDistrict
          ? "missing_legislative_district"
          : isPlaceOffice && !hasJurisdiction
            ? "missing_jurisdiction"
            : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();
  for (const summary of input.summaries) {
    const filerId = summary.filerId.trim();
    const committeeId = summary.committeeId?.trim();
    const committeeName = summary.filerName.trim();
    if (!filerId || !committeeId || !committeeName) {
      continue;
    }
    if (summary.electionYear !== electionYear) {
      continue;
    }
    if (!isCandidateSummaryUsable(summary)) {
      continue;
    }
    if (!summaryMatchesExpectedOffice({ summary, officeSearch: officeSearchInput })) {
      continue;
    }
    if (!candidateSummaryMatchesName({ summary, candidateName: input.candidateName, candidateNameKeys })) {
      continue;
    }

    const committeeKey = `${filerId}\u0000${committeeId}`;
    const accumulator = rowsByCommittee.get(committeeKey) ?? {
      filerId,
      committeeId,
      committeeName,
      ...(summary.candidacyId ? { candidacyId: summary.candidacyId } : {}),
      sourceUrl: summary.sourceUrl ?? null,
      rows: [],
    };
    accumulator.rows.push(summary);
    rowsByCommittee.set(committeeKey, accumulator);
  }

  if (rowsByCommittee.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matches = [...rowsByCommittee.values()]
    .map(toCommitteeMatch)
    .sort((left, right) => left.filerId.localeCompare(right.filerId));

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

export async function searchAndResolveWashingtonCandidateCommittee(
  input: WashingtonCandidateCommitteeSearchInput,
  options: WashingtonPdcClientOptions = {}
): Promise<WashingtonCandidateCommitteeResolution> {
  const officeSearchInput = toWashingtonPdcOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    legislativeDistrict: input.legislativeDistrict,
    jurisdiction: input.jurisdiction,
    position: input.position,
  });
  if (!officeSearchInput) {
    return resolveWashingtonCandidateCommittee({ ...input, summaries: [] });
  }

  const summaries = await searchWashingtonPdcCandidateSummaries(
    {
      candidateName: input.candidateName,
      electionYear: input.electionYear,
      office: officeSearchInput.pdcOffice,
      legislativeDistrict: officeSearchInput.legislativeDistrict,
      limit: 50,
    },
    options
  );
  return resolveWashingtonCandidateCommittee({ ...input, summaries });
}

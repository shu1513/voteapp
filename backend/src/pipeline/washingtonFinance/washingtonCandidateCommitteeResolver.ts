import {
  searchWashingtonPdcCandidateSummaries,
  type WashingtonPdcCandidateSummary,
  type WashingtonPdcClientOptions,
} from "./washingtonPdcClient.js";
import {
  normalizeWashingtonPdcLegislativeDistrict,
  mapWashingtonPdcOffice,
  toWashingtonPdcOfficeSearchInput,
} from "./washingtonFinanceEligibleOffices.js";

export type WashingtonCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  legislativeDistrict?: string | null;
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
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
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
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of normalizeWashingtonCandidateNameKeys(input.summary.filerName)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
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
  expectedPdcOffice: string;
  expectedLegislativeDistrict: string | null;
}): boolean {
  const mappedOffice = mapWashingtonPdcOffice({
    office: input.summary.office,
    legislativeDistrict: input.summary.legislativeDistrict,
  });
  if (!mappedOffice || mappedOffice.pdcOffice !== input.expectedPdcOffice) {
    return false;
  }
  if (input.expectedLegislativeDistrict !== null) {
    return mappedOffice.legislativeDistrict === input.expectedLegislativeDistrict;
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
    return {
      status: "unmatched",
      reason: isLegislativeOffice && !hasDistrict ? "missing_legislative_district" : "unsupported_office",
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
    if (
      !summaryMatchesExpectedOffice({
        summary,
        expectedPdcOffice: officeSearchInput.pdcOffice,
        expectedLegislativeDistrict: officeSearchInput.legislativeDistrict,
      })
    ) {
      continue;
    }
    if (!candidateSummaryMatchesName({ summary, candidateNameKeys })) {
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

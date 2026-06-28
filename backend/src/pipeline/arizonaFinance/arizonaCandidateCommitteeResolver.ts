import {
  searchArizonaSpotlightCandidateCommittees,
  type ArizonaSpotlightCandidateCommitteeMatch,
  type ArizonaSpotlightClientOptions,
} from "./arizonaSpotlightClient.js";
import { normalizeArizonaFinanceOffice } from "./arizonaFinanceEligibleOffices.js";

export type ArizonaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  limit?: number;
};

export type ArizonaCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  confidence: "single_committee";
  source: "spotlight";
  sourceUrl: string | null;
  matchedIncomeRowCount: number;
  totalIncomeAmount: number;
};

export type ArizonaCandidateCommitteeResolution =
  | ({ status: "matched" } & ArizonaCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason: "missing_candidate_name" | "unsupported_office" | "no_candidate_committee_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: ArizonaCandidateCommitteeMatch[];
    };

export type ArizonaCandidateCommitteeResolverClient = {
  searchCandidateCommittees: typeof searchArizonaSpotlightCandidateCommittees;
};

const DEFAULT_CLIENT: ArizonaCandidateCommitteeResolverClient = {
  searchCandidateCommittees: searchArizonaSpotlightCandidateCommittees,
};

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

export function normalizeArizonaCandidateNameForStorage(value: string): string {
  return normalizeTextKey(value) || value.trim().replace(/\s+/g, " ").toUpperCase();
}

function toResolverMatch(match: ArizonaSpotlightCandidateCommitteeMatch): ArizonaCandidateCommitteeMatch {
  return {
    committeeId: match.committeeId,
    committeeName: match.committeeName,
    confidence: "single_committee",
    source: "spotlight",
    sourceUrl: match.sourceUrl,
    matchedIncomeRowCount: match.rowCount,
    totalIncomeAmount: match.amount,
  };
}

export async function resolveArizonaCandidateCommittee(
  input: ArizonaCandidateCommitteeResolverInput,
  options: ArizonaSpotlightClientOptions = {},
  client: Partial<ArizonaCandidateCommitteeResolverClient> = {}
): Promise<ArizonaCandidateCommitteeResolution> {
  const candidateNameNormalized = normalizeTextKey(input.candidateName);
  const officeNameNormalized = normalizeTextKey(input.officeName);
  if (!candidateNameNormalized) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const office = normalizeArizonaFinanceOffice({
    officeScope: input.officeScope,
    officeName: input.officeName,
  });
  if (!office) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const resolverClient = { ...DEFAULT_CLIENT, ...client };
  const matches = (
    await resolverClient.searchCandidateCommittees(
      {
        candidateName: input.candidateName,
        officeName: office.officeCanonicalName,
        electionYear: input.electionYear,
        limit: input.limit,
      },
      options
    )
  ).map(toResolverMatch);

  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized,
      officeNameNormalized,
      matches,
    };
  }
  return { status: "matched", ...matches[0]! };
}

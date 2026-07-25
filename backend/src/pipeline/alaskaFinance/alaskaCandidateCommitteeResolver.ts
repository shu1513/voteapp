import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import type { AlaskaApocCampaignIncomeRow } from "./alaskaApocClient.js";
import { parseAlaskaApocDateYear } from "./alaskaApocClient.js";

export type AlaskaCandidateCommitteeResolution =
  | {
      status: "matched";
      candidateFilerId: string;
      candidateFilerName: string;
      confidence: "exact";
      source: "apoc_csv";
      sourceUrl: string | null;
      matchedRowCount: number;
    }
  | {
      status: "unmatched";
      reason: "no_candidate_filer_match";
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      candidateFilerIds: string[];
    };

type FilerAggregate = {
  candidateFilerId: string;
  candidateFilerName: string;
  matchedRowCount: number;
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

export function normalizeAlaskaCandidateNameForStorage(value: string): string {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    throw new Error("candidate name is required");
  }
  return normalized;
}

export type AlaskaCandidateNameKeyOptions = {
  // Adds keys for common first-name nicknames ("Nick Capozzi" also keys
  // NICHOLAS CAPOZZI). Expansion is one-sided: only the VoteApp candidate
  // name is expanded, APOC filer names always match literally, so two
  // distinct formal names cannot meet at a shared nickname key.
  expandNicknames?: boolean;
};

const NAME_SUFFIX_PATTERN = /^(?:JR|SR|II|III|IV|V)$/;

// VoteApp stores "Last, First M." and "First M. Last"; order the tokens
// first-name-first and drop generational suffixes so the first and last
// tokens are the given name and surname.
function orderedNameTokens(value: string): string[] {
  const commaMatch = /^\s*([^,]+),\s*(.+?)\s*$/.exec(value);
  const ordered = commaMatch?.[1] && commaMatch[2] ? `${commaMatch[2]} ${commaMatch[1]}` : value;
  return normalizeTextKey(ordered)
    .split(" ")
    .filter((token) => token.length > 0 && !NAME_SUFFIX_PATTERN.test(token));
}

// Quoted call name in the roster spelling: Glenn M. "Mike" Prax -> MIKE.
function quotedCallName(value: string): string | null {
  const match = /["“”']([^"“”']{2,}?)["“”']/.exec(value);
  const token = match ? normalizeTextKey(match[1]).split(" ")[0] : undefined;
  return token && token.length >= 2 ? token : null;
}

export function normalizeAlaskaCandidateNameKeys(
  value: string,
  options: AlaskaCandidateNameKeyOptions = {}
): Set<string> {
  const keys = new Set<string>();
  const normalized = normalizeTextKey(value);
  if (normalized) {
    keys.add(normalized);
  }

  const commaMatch = /^\s*([^,]+),\s*(.+?)\s*$/.exec(value);
  if (commaMatch?.[1] && commaMatch[2]) {
    const reversed = normalizeTextKey(`${commaMatch[2]} ${commaMatch[1]}`);
    if (reversed) {
      keys.add(reversed);
    }
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts.at(-1)} ${parts.slice(0, -1).join(" ")}`);
  }

  const ordered = orderedNameTokens(value);
  if (ordered.length >= 2) {
    const surname = ordered.at(-1) as string;
    const givenNames = new Set<string>([ordered[0]]);
    const callName = quotedCallName(value);
    if (callName && callName !== surname) {
      givenNames.add(callName);
    }
    if (options.expandNicknames === true) {
      for (const givenName of [...givenNames]) {
        for (const variant of firstNameVariants(givenName)) {
          givenNames.add(variant);
        }
      }
    }
    for (const givenName of givenNames) {
      if (givenName === surname) {
        continue;
      }
      keys.add(`${givenName} ${surname}`);
      keys.add(`${surname} ${givenName}`);
    }
  }

  return keys;
}

function rowYear(row: AlaskaApocCampaignIncomeRow): number | null {
  return row.reportYear ?? parseAlaskaApocDateYear(row.date);
}

function isCycleYear(input: { row: AlaskaApocCampaignIncomeRow; electionYear: number }): boolean {
  const year = rowYear(input.row);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function filerId(row: AlaskaApocCampaignIncomeRow): string {
  const explicit = row.filerId.trim();
  return explicit || normalizeTextKey(row.filerName);
}

// APOC filer names often carry middle names the VoteApp side lacks
// ("Nicholas James Capozzi" vs "Nick Capozzi"), so a key matches a field when
// its tokens appear in order among the field's tokens. Fields are matched
// separately: joining them first would let a key match across the seam
// ("...MCDONALD JIEUN..." from filerName + name both holding the same value).
function isOrderedTokenSubsequence(keyTokens: readonly string[], fieldTokens: readonly string[]): boolean {
  let index = 0;
  for (const token of fieldTokens) {
    if (token === keyTokens[index]) {
      index += 1;
      if (index === keyTokens.length) {
        return true;
      }
    }
  }
  return false;
}

function rowMatchesCandidate(input: {
  row: AlaskaApocCampaignIncomeRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const fields = [input.row.filerName, input.row.name].map((field) => normalizeTextKey(field).split(" ").filter(Boolean));
  for (const key of input.candidateNameKeys) {
    if (key.length === 0) {
      continue;
    }
    const keyTokens = key.split(" ");
    if (fields.some((fieldTokens) => isOrderedTokenSubsequence(keyTokens, fieldTokens))) {
      return true;
    }
  }
  return false;
}

function isCandidateFilerType(value: string): boolean {
  return normalizeTextKey(value).includes("CANDIDATE");
}

export function resolveAlaskaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  sourceUrl?: string | null;
}): AlaskaCandidateCommitteeResolution {
  const candidateNameNormalized = normalizeAlaskaCandidateNameForStorage(input.candidateName);
  // VoteApp side expands nicknames; APOC filer names always key literally.
  const candidateNameKeys = normalizeAlaskaCandidateNameKeys(input.candidateName, { expandNicknames: true });
  const filers = new Map<string, FilerAggregate>();

  for (const row of input.incomeRows) {
    if (
      !isCandidateFilerType(row.filerType) ||
      !isCycleYear({ row, electionYear: input.electionYear }) ||
      !rowMatchesCandidate({ row, candidateNameKeys })
    ) {
      continue;
    }
    const candidateFilerId = filerId(row);
    const candidateFilerName = row.filerName.trim() || row.name.trim();
    if (!candidateFilerId || !candidateFilerName) {
      continue;
    }
    const key = normalizeTextKey(candidateFilerId);
    const existing = filers.get(key);
    if (existing) {
      existing.matchedRowCount += 1;
      continue;
    }
    filers.set(key, {
      candidateFilerId,
      candidateFilerName,
      matchedRowCount: 1,
    });
  }

  if (filers.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized,
    };
  }
  if (filers.size > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized,
      candidateFilerIds: [...filers.values()]
        .map((filer) => filer.candidateFilerId)
        .sort((left, right) => left.localeCompare(right)),
    };
  }

  const match = [...filers.values()][0];
  return {
    status: "matched",
    candidateFilerId: match.candidateFilerId,
    candidateFilerName: match.candidateFilerName,
    confidence: "exact",
    source: "apoc_csv",
    sourceUrl: input.sourceUrl ?? null,
    matchedRowCount: match.matchedRowCount,
  };
}

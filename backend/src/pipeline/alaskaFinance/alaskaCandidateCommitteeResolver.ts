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

export function normalizeAlaskaCandidateNameKeys(value: string): Set<string> {
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

function rowMatchesCandidate(input: {
  row: AlaskaApocCampaignIncomeRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const haystack = normalizeTextKey([input.row.filerName, input.row.name].join(" "));
  return [...input.candidateNameKeys].some((key) => key.length > 0 && haystack.includes(key));
}

export function resolveAlaskaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  sourceUrl?: string | null;
}): AlaskaCandidateCommitteeResolution {
  const candidateNameNormalized = normalizeAlaskaCandidateNameForStorage(input.candidateName);
  const candidateNameKeys = normalizeAlaskaCandidateNameKeys(input.candidateName);
  const filers = new Map<string, FilerAggregate>();

  for (const row of input.incomeRows) {
    if (!isCycleYear({ row, electionYear: input.electionYear }) || !rowMatchesCandidate({ row, candidateNameKeys })) {
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

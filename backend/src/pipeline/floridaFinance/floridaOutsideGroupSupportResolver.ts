import {
  normalizeFloridaDisplayText,
  normalizeFloridaTextKey,
  type FloridaContributionRow,
} from "./floridaCampaignFinanceRows.js";
import type { FloridaSupportOppose } from "./floridaOutsideGroupContributionAggregator.js";

export type FloridaOutsideGroupSupportConfidence = "high" | "medium" | "low";
export type FloridaOutsideGroupSupportSource = "manual" | "name_heuristic" | "independent_expenditure";

export type FloridaOutsideGroupSupportEvidenceInput = {
  candidateElectionId?: string | null;
  committeeId?: string | null;
  committeeName: string;
  supportOppose: FloridaSupportOppose;
  confidence?: FloridaOutsideGroupSupportConfidence;
  amount?: number | null;
  evidenceUrl?: string | null;
  evidenceNote?: string | null;
  linkSource?: FloridaOutsideGroupSupportSource;
  committeeNames?: readonly string[];
};

export type FloridaResolvedOutsideGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: FloridaSupportOppose;
  amount: number;
  sourceUrl?: string | null;
  committeeNames?: readonly string[];
  confidence: FloridaOutsideGroupSupportConfidence;
  discoverySource: FloridaOutsideGroupSupportSource | "trusted_group";
  evidenceNote?: string | null;
};

export type FloridaOutsideGroupSupportResolutionInput = {
  candidateName: string;
  trustedOutsideGroups?: readonly {
    committeeId: string;
    committeeName: string;
    supportOppose: FloridaSupportOppose;
    amount: number;
    sourceUrl?: string | null;
    committeeNames?: readonly string[];
  }[];
  supportEvidence?: readonly FloridaOutsideGroupSupportEvidenceInput[];
  outsideContributionRows?: readonly FloridaContributionRow[];
  includeNameHeuristics?: boolean;
  heuristicSourceUrl?: string | null;
};

export type FloridaOutsideGroupSupportResolutionResult = {
  outsideGroups: FloridaResolvedOutsideGroup[];
  trustedGroupCount: number;
  evidenceLinkCount: number;
  heuristicGroupCount: number;
};

type MutableResolvedGroup = Omit<FloridaResolvedOutsideGroup, "committeeNames"> & {
  committeeNames: string[];
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeAmount(value: number | null | undefined): number {
  if (value === undefined || value === null) {
    return 0;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Florida outside group amount must be nonnegative: ${value}`);
  }
  return Math.round(value * 100) / 100;
}

function normalizeConfidence(
  value: FloridaOutsideGroupSupportConfidence | undefined
): FloridaOutsideGroupSupportConfidence {
  if (value === undefined || value === "high" || value === "medium" || value === "low") {
    return value ?? "high";
  }
  throw new Error(`Invalid Florida outside group support confidence: ${value}`);
}

function normalizeSource(value: FloridaOutsideGroupSupportSource | undefined): FloridaOutsideGroupSupportSource {
  if (
    value === undefined ||
    value === "manual" ||
    value === "name_heuristic" ||
    value === "independent_expenditure"
  ) {
    return value ?? "manual";
  }
  throw new Error(`Invalid Florida outside group support source: ${value}`);
}

export function floridaOutsideGroupCommitteeIdFromName(committeeName: string): string {
  const normalized = normalizeFloridaTextKey(committeeName).replace(/\s+/g, "_");
  return requireNonEmpty(normalized, "Florida outside group committee name");
}

function groupKey(input: {
  committeeId: string;
  committeeName: string;
  supportOppose: FloridaSupportOppose;
}): string {
  return `${input.committeeId.trim().toUpperCase()}\u0000${normalizeFloridaTextKey(input.committeeName)}\u0000${input.supportOppose}`;
}

function addAlias(group: MutableResolvedGroup, alias: string | null | undefined): void {
  const normalized = normalizeFloridaDisplayText(alias ?? "");
  if (!normalized) {
    return;
  }
  if (
    !group.committeeNames.some(
      (existing) => normalizeFloridaTextKey(existing) === normalizeFloridaTextKey(normalized)
    )
  ) {
    group.committeeNames.push(normalized);
  }
}

function upsertResolvedGroup(
  groups: Map<string, MutableResolvedGroup>,
  group: MutableResolvedGroup
): boolean {
  const key = groupKey(group);
  const existing = groups.get(key);
  if (!existing) {
    groups.set(key, group);
    return true;
  }

  if (existing.discoverySource !== "trusted_group") {
    existing.amount = Math.max(existing.amount, group.amount);
  }
  existing.sourceUrl = existing.sourceUrl ?? group.sourceUrl ?? null;
  existing.evidenceNote = existing.evidenceNote ?? group.evidenceNote ?? null;
  for (const alias of group.committeeNames) {
    addAlias(existing, alias);
  }
  return false;
}

function normalizedCandidateNameKeys(candidateName: string): Set<string> {
  const normalized = normalizeFloridaTextKey(candidateName)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const keys = new Set<string>();
  if (normalized) {
    keys.add(normalized);
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  }

  const commaParts = candidateName
    .split(",")
    .map((part) => normalizeFloridaTextKey(part))
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const [lastName, ...firstNames] = commaParts;
    const flipped = [firstNames.join(" "), lastName].join(" ").trim().replace(/\s+/g, " ");
    if (flipped) {
      keys.add(flipped);
    }
  }

  return keys;
}

function committeeNameMentionsCandidate(input: {
  committeeName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const committeeKey = normalizeFloridaTextKey(input.committeeName);
  for (const candidateNameKey of input.candidateNameKeys) {
    if (candidateNameKey && committeeKey.includes(candidateNameKey)) {
      return true;
    }
  }
  return false;
}

export function supportOpposeFromFloridaCommitteeText(
  committeeName: string,
  candidateNameKeys: ReadonlySet<string>
): FloridaSupportOppose | null {
  if (!committeeNameMentionsCandidate({ committeeName, candidateNameKeys })) {
    return null;
  }
  const text = normalizeFloridaTextKey(committeeName);
  if (/\b(?:STOP|DEFEAT|OPPOSE|AGAINST|ANTI|ACCOUNTABILITY)\b/.test(text)) {
    return "oppose";
  }
  return "support";
}

function toResolvedFromEvidence(evidence: FloridaOutsideGroupSupportEvidenceInput): MutableResolvedGroup {
  const committeeName = normalizeFloridaDisplayText(
    requireNonEmpty(evidence.committeeName, "Florida outside group committee name")
  );
  const committeeId =
    normalizeFloridaDisplayText(evidence.committeeId ?? "") || floridaOutsideGroupCommitteeIdFromName(committeeName);
  const source = normalizeSource(evidence.linkSource);
  return {
    committeeId,
    committeeName,
    supportOppose: evidence.supportOppose,
    amount: normalizeAmount(evidence.amount),
    sourceUrl: evidence.evidenceUrl ?? null,
    committeeNames: [...(evidence.committeeNames ?? [])],
    confidence: normalizeConfidence(evidence.confidence),
    discoverySource: source,
    evidenceNote: evidence.evidenceNote ?? null,
  };
}

export function resolveFloridaOutsideGroupSupport(
  input: FloridaOutsideGroupSupportResolutionInput
): FloridaOutsideGroupSupportResolutionResult {
  const candidateNameKeys = normalizedCandidateNameKeys(input.candidateName);
  const groups = new Map<string, MutableResolvedGroup>();
  let trustedGroupCount = 0;
  let evidenceLinkCount = 0;
  let heuristicGroupCount = 0;

  for (const trusted of input.trustedOutsideGroups ?? []) {
    const committeeName = normalizeFloridaDisplayText(
      requireNonEmpty(trusted.committeeName, "trusted Florida outside group committee name")
    );
    const group: MutableResolvedGroup = {
      committeeId: requireNonEmpty(trusted.committeeId, "trusted Florida outside group committee id"),
      committeeName,
      supportOppose: trusted.supportOppose,
      amount: normalizeAmount(trusted.amount),
      sourceUrl: trusted.sourceUrl ?? null,
      committeeNames: [...(trusted.committeeNames ?? [])],
      confidence: "high",
      discoverySource: "trusted_group",
      evidenceNote: null,
    };
    if (upsertResolvedGroup(groups, group)) {
      trustedGroupCount += 1;
    }
  }

  for (const evidence of input.supportEvidence ?? []) {
    const group = toResolvedFromEvidence(evidence);
    if (upsertResolvedGroup(groups, group)) {
      evidenceLinkCount += 1;
    }
  }

  if (input.includeNameHeuristics && candidateNameKeys.size > 0) {
    const seenCommitteeNames = new Set<string>();
    for (const row of input.outsideContributionRows ?? []) {
      const committeeName = normalizeFloridaDisplayText(row.recipientName);
      const committeeNameKey = normalizeFloridaTextKey(committeeName);
      if (!committeeName || seenCommitteeNames.has(committeeNameKey)) {
        continue;
      }
      seenCommitteeNames.add(committeeNameKey);

      const supportOppose = supportOpposeFromFloridaCommitteeText(committeeName, candidateNameKeys);
      if (!supportOppose) {
        continue;
      }
      const group: MutableResolvedGroup = {
        committeeId: floridaOutsideGroupCommitteeIdFromName(committeeName),
        committeeName,
        supportOppose,
        amount: 0,
        sourceUrl: input.heuristicSourceUrl ?? row.sourceUrl ?? null,
        committeeNames: [],
        confidence: "low",
        discoverySource: "name_heuristic",
        evidenceNote: "Committee name contains the candidate name.",
      };
      if (upsertResolvedGroup(groups, group)) {
        heuristicGroupCount += 1;
      }
    }
  }

  return {
    outsideGroups: [...groups.values()].map((group) => ({
      ...group,
      committeeNames: group.committeeNames,
    })),
    trustedGroupCount,
    evidenceLinkCount,
    heuristicGroupCount,
  };
}

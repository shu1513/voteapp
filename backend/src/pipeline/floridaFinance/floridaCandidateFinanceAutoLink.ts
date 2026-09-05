import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
import {
  floridaElectionCycleStartYear,
  normalizeFloridaDisplayText,
  normalizeFloridaTextKey,
  parseFloridaDateYear,
  type FloridaContributionRow,
} from "./floridaCampaignFinanceRows.js";
import { FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./floridaFinanceEligibleOffices.js";
import { upsertFloridaFinanceLink } from "./floridaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type FloridaFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type FloridaCandidateCommitteeResolution =
  | {
      status: "matched";
      committeeId: string;
      committeeName: string;
      recipientNames: string[];
      sourceUrl: string | null;
    }
  | {
      status: "unmatched" | "ambiguous";
      reason:
        | "missing_candidate_name"
        | "no_contributions"
        | "no_matching_committee"
        | "multiple_matching_committees";
    };

export type FloridaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: FloridaCandidateCommitteeResolution["status"] | "linked";
      committeeId?: string;
      reason?: string;
    }
  | {
      candidateId: string;
      electionId: string;
      status: "error";
      reason: "auto_link_failed";
      error: string;
    };

type RecipientGroup = {
  committeeName: string;
  recipientNames: string[];
  rows: FloridaContributionRow[];
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeCandidateNameForStorage(value: string): string {
  return requireNonEmpty(value, "candidate name")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function floridaCandidateCommitteeIdFromName(committeeName: string): string {
  const normalized = normalizeFloridaTextKey(committeeName).replace(/\s+/g, "_");
  return requireNonEmpty(normalized, "Florida committee name");
}

function normalizeFloridaPersonName(value: string | undefined): string {
  return normalizeFloridaTextKey(value)
    // Bare "V" is a middle initial, not a suffix (GENERATIONAL_SUFFIX_RANK in
    // finance/personNameMiddleEvidence.ts): stripping it erased middle evidence.
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeFloridaCandidateNameKeys(candidateName: string): Set<string> {
  const normalized = normalizeFloridaPersonName(candidateName);
  const keys = new Set<string>();
  if (normalized) {
    keys.add(normalized);
  }

  // DOS committee names lead with the surname ("Barreiro, Bruno A. (REP)"),
  // while candidate display names are "First Last" — without the reversed
  // key a comma-less candidate name can never match a committee.
  if (!candidateName.includes(",")) {
    const words = normalized.split(" ");
    if (words.length >= 2) {
      const lastName = words[words.length - 1];
      const givenNames = words.slice(0, -1).join(" ");
      keys.add(`${lastName} ${givenNames}`);
      if (words.length > 2) {
        // Short reversed key: app middle names/initials ("Jane A. Doe") and
        // compound surnames ("Jane de la Cruz") both still match a DOS
        // "DOE, JANE" / "DE LA CRUZ, JANE" via the bounded substring check.
        keys.add(`${lastName} ${words[0]}`);
      }
    }
  }

  // Key-only trims below keep "V": they widen recall (a real "Smith, John V"
  // still keys "JOHN SMITH") and cannot create a false positive — middle
  // evidence is read from the raw string by hasMiddleNameConflict.
  const commaParts = candidateName
    .split(",")
    .map((part) => normalizeFloridaTextKey(part))
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const [lastName, ...firstNames] = commaParts;
    const normalizedLastName = (lastName ?? "")
      .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    const normalizedGivenNames = firstNames
      .join(" ")
      .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (normalizedGivenNames && normalizedLastName) {
      const [firstName] = normalizedGivenNames.split(" ");
      keys.add(`${normalizedGivenNames} ${normalizedLastName}`.trim());
      if (firstName) {
        keys.add(`${firstName} ${normalizedLastName}`);
      }
    }
  } else {
    const parts = normalized.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
    }
  }

  return keys;
}

function committeeNameMentionsCandidate(input: {
  committeeName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const committeeKey = ` ${normalizeFloridaTextKey(input.committeeName)} `;
  for (const candidateNameKey of input.candidateNameKeys) {
    const boundedCandidateNameKey = candidateNameKey ? ` ${candidateNameKey} ` : "";
    if (boundedCandidateNameKey && committeeKey.includes(boundedCandidateNameKey)) {
      return true;
    }
  }
  return false;
}

function isContributionInElectionCycle(row: FloridaContributionRow, electionYear: number): boolean {
  const rowYear = parseFloridaDateYear(row.contributionDate);
  if (rowYear === null) {
    return false;
  }
  return rowYear >= floridaElectionCycleStartYear(electionYear) && rowYear <= electionYear;
}

function addRecipientName(group: RecipientGroup, recipientName: string): void {
  if (
    !group.recipientNames.some(
      (existing) => normalizeFloridaTextKey(existing) === normalizeFloridaTextKey(recipientName)
    )
  ) {
    group.recipientNames.push(recipientName);
  }
}

function groupRowsByRecipient(rows: readonly FloridaContributionRow[]): RecipientGroup[] {
  const groups = new Map<string, RecipientGroup>();
  for (const row of rows) {
    const recipientName = normalizeFloridaDisplayText(row.recipientName);
    if (!recipientName) {
      continue;
    }
    const normalizedName = normalizeFloridaTextKey(recipientName);
    if (!normalizedName) {
      continue;
    }
    const existing = groups.get(normalizedName);
    if (existing) {
      existing.rows.push(row);
      addRecipientName(existing, recipientName);
      continue;
    }
    groups.set(normalizedName, {
      committeeName: recipientName,
      recipientNames: [recipientName],
      rows: [row],
    });
  }
  return [...groups.values()];
}

function sourceUrlFromGroup(group: RecipientGroup, fallbackSourceUrl: string | null | undefined): string | null {
  return (
    fallbackSourceUrl ??
    group.rows.find((row) => typeof row.sourceUrl === "string" && row.sourceUrl.trim().length > 0)?.sourceUrl ??
    null
  );
}

export function resolveFloridaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  contributionRows: readonly FloridaContributionRow[];
  sourceUrl?: string | null;
}): FloridaCandidateCommitteeResolution {
  const candidateNameKeys = normalizeFloridaCandidateNameKeys(input.candidateName);
  if (candidateNameKeys.size === 0) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }

  const cycleRows = input.contributionRows.filter((row) => isContributionInElectionCycle(row, input.electionYear));
  if (cycleRows.length === 0) {
    return { status: "unmatched", reason: "no_contributions" };
  }

  const matchingGroups = groupRowsByRecipient(cycleRows).filter(
    (group) =>
      committeeNameMentionsCandidate({ committeeName: group.committeeName, candidateNameKeys }) &&
      // DOS recipient names are the candidate's own name, surname first
      // ("DOE, JANE B. (DEM)(GOV)"), and the short reversed key drops the
      // middle — so "Jane A. Doe" matched the other Jane Doe's committee.
      // A contradicting middle name rejects the group (georgia pattern).
      !hasMiddleNameConflict({
        candidateName: input.candidateName,
        rowNames: group.recipientNames,
        normalizePersonName: normalizeFloridaPersonName,
      })
  );
  if (matchingGroups.length === 0) {
    return { status: "unmatched", reason: "no_matching_committee" };
  }
  if (matchingGroups.length > 1) {
    return { status: "ambiguous", reason: "multiple_matching_committees" };
  }

  const [group] = matchingGroups;
  return {
    status: "matched",
    committeeId: floridaCandidateCommitteeIdFromName(group.committeeName),
    committeeName: group.committeeName,
    recipientNames: group.recipientNames,
    sourceUrl: sourceUrlFromGroup(group, input.sourceUrl),
  };
}

export function buildFloridaCandidateNamePredicate(
  candidates: readonly FloridaFinanceAutoLinkCandidateElection[]
): (row: FloridaContributionRow) => boolean {
  const candidateNameKeysByYear = new Map<number, Set<string>>();
  for (const candidate of candidates) {
    const keys = candidateNameKeysByYear.get(candidate.electionYear) ?? new Set<string>();
    for (const key of normalizeFloridaCandidateNameKeys(candidate.candidateName)) {
      keys.add(key);
    }
    candidateNameKeysByYear.set(candidate.electionYear, keys);
  }

  return (row) => {
    const rowYear = parseFloridaDateYear(row.contributionDate);
    if (rowYear === null) {
      return false;
    }
    for (const [electionYear, keys] of candidateNameKeysByYear.entries()) {
      if (rowYear < floridaElectionCycleStartYear(electionYear) || rowYear > electionYear) {
        continue;
      }
      if (committeeNameMentionsCandidate({ committeeName: row.recipientName, candidateNameKeys: keys })) {
        return true;
      }
    }
    return false;
  };
}

export const listFloridaCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "FL",
    linksTable: "fl_candidate_finance_links",
    eligibleOfficeKeys: [...FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkFloridaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: FloridaFinanceAutoLinkCandidateElection;
  contributionRows: readonly FloridaContributionRow[];
  sourceUrl?: string | null;
  now: Date;
}): Promise<FloridaFinanceAutoLinkResult> {
  const resolution = resolveFloridaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    electionYear: input.candidateElection.electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.sourceUrl,
  });

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertFloridaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: requireNonEmpty(input.candidateElection.officeName, "office name"),
      district: input.candidateElection.district,
      committeeId: resolution.committeeId,
      committeeName: resolution.committeeName,
      linkStatus: "active",
      linkSource: "dos_export",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    committeeId: resolution.committeeId,
  };
}

export async function autoLinkMissingFloridaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  contributionRowsByYear: ReadonlyMap<number, readonly FloridaContributionRow[]>;
  sourceUrlByYear?: ReadonlyMap<number, string>;
  candidateElections?: readonly FloridaFinanceAutoLinkCandidateElection[];
}): Promise<FloridaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listFloridaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: FloridaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkFloridaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          contributionRows: input.contributionRowsByYear.get(candidateElection.electionYear) ?? [],
          sourceUrl: input.sourceUrlByYear?.get(candidateElection.electionYear) ?? null,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Florida finance auto-link failed for candidate election; continuing:", {
        candidateId: candidateElection.candidateId,
        electionId: candidateElection.electionId,
        electionYear: candidateElection.electionYear,
        error: message,
      });
      results.push({
        candidateId: candidateElection.candidateId,
        electionId: candidateElection.electionId,
        status: "error",
        reason: "auto_link_failed",
        error: message,
      });
    }
  }
  return results;
}

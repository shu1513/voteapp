import type { Pool, PoolClient } from "pg";

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

export type FloridaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

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

type CandidateElectionQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
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

function mapCandidateElectionRow(row: CandidateElectionQueryRow): FloridaFinanceAutoLinkCandidateElection {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
  };
}

export function floridaCandidateCommitteeIdFromName(committeeName: string): string {
  const normalized = normalizeFloridaTextKey(committeeName).replace(/\s+/g, "_");
  return requireNonEmpty(normalized, "Florida committee name");
}

export function normalizeFloridaCandidateNameKeys(candidateName: string): Set<string> {
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

  const matchingGroups = groupRowsByRecipient(cycleRows).filter((group) =>
    committeeNameMentionsCandidate({ committeeName: group.committeeName, candidateNameKeys })
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

export async function listFloridaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<FloridaFinanceAutoLinkCandidateElection[]> {
  const result = await db.query<CandidateElectionQueryRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        election.id::text AS election_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
        ) AS candidate_name,
        extract(year from election.election_date)::int AS election_year,
        office.scope AS office_scope,
        COALESCE(NULLIF(trim(office.canonical_name), ''), election.official_ballot_title) AS office_name,
        CASE
          WHEN district.district_type IN ('state_upper', 'state_lower') THEN
            NULLIF(
              regexp_replace(
                substring(district.geoid_compact from char_length(district.state_fips) + 1),
                '^0+',
                ''
              ),
              ''
            )
          ELSE NULL
        END AS district
      FROM public.candidate_elections AS candidate_election
      JOIN public.candidates AS candidate
        ON candidate.id = candidate_election.candidate_id
      JOIN public.elections AS election
        ON election.id = candidate_election.election_id
      JOIN public.districts AS district
        ON district.id = election.district_id
      LEFT JOIN public.offices AS office
        ON office.id = election.office_id
      WHERE candidate.deleted_at IS NULL
        AND district.state = 'FL'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.fl_candidate_finance_links AS link
          WHERE link.candidate_id = candidate.id
            AND link.election_id = election.id
            AND link.link_status = 'active'
        )
      ORDER BY election.election_date ASC, candidate.display_name ASC NULLS LAST, candidate.id ASC
      LIMIT $2::int
    `,
    [
      input.now.toISOString(),
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
      [...FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

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

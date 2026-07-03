import type { PoolClient } from "pg";

import type { CandidateProfilePayload } from "../../contracts/candidateProfilePayloadContract.js";
import {
  hasNormalizedIntersection,
  normalizeOptionalUrl,
  normalizeTwitterHandle,
} from "../../utils/candidateIdentity.js";

export type ExistingCandidateRow = {
  id: string;
  first_name: string;
  last_name: string;
  date_of_birth: string | null;
  twitter_handle: string | null;
  linkedin_url: string | null;
  official_website_url: string | null;
  fec_ids: unknown;
  state_filing_ids: unknown;
  current_office: string | null;
  state: string;
};

export type FindOrCreateCandidateFromProfileInput = {
  client: PoolClient;
  profile: CandidateProfilePayload;
  state: string;
  rosterParty: string | undefined;
  includeParty: boolean;
  allowCrossStateHardIdentifierMatch?: boolean;
  // When set, a candidate already linked to this election (directly or as a
  // running mate) with the same display_name is treated as the same person
  // even when no hard identifier matches. Rosters enforce display_name
  // uniqueness per election, so this cannot merge two different people; it
  // prevents duplicate candidates when a re-written profile changes the row's
  // only hard identifier (e.g. swapping a placeholder campaign URL).
  matchByLinkedElectionId?: string;
  // Field names whose non-empty stored values may be REPLACED by the incoming
  // profile (deliberate correction). Everything else keeps fill-if-empty
  // semantics: never overwrite a non-empty stored value.
  overwriteProfileFields?: ReadonlySet<OverwritableProfileField>;
};

export const OVERWRITABLE_PROFILE_FIELDS = [
  "date_of_birth",
  "twitter_handle",
  "linkedin_url",
  "official_website_url",
  "summary",
  "current_office",
] as const;
export type OverwritableProfileField = (typeof OVERWRITABLE_PROFILE_FIELDS)[number];

export type FindOrCreateCandidateFromProfileResult = {
  candidateId: string;
  matchedExisting: boolean;
};

function parseOptionalStringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function normalizeIdList(values: readonly string[] | undefined): string[] {
  return (values ?? [])
    .map((value) => value.trim().toLowerCase())
    .filter((value) => value.length > 0);
}

export function mergeIdentifierLists(
  existing: readonly string[] | undefined,
  incoming: readonly string[] | undefined
): string[] | undefined {
  const merged: string[] = [];
  const seen = new Set<string>();

  for (const source of [existing ?? [], incoming ?? []]) {
    for (const value of source) {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        continue;
      }
      const normalized = trimmed.toLowerCase();
      if (seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      merged.push(trimmed);
    }
  }

  return merged.length > 0 ? merged : undefined;
}

function haveSameNormalizedIdentifierSet(
  left: readonly string[] | undefined,
  right: readonly string[] | undefined
): boolean {
  const leftSet = new Set(normalizeIdList(left));
  const rightSet = new Set(normalizeIdList(right));
  if (leftSet.size !== rightSet.size) {
    return false;
  }
  for (const value of leftSet) {
    if (!rightSet.has(value)) {
      return false;
    }
  }
  return true;
}

export function hasAtLeastOneHardIdentifier(profile: CandidateProfilePayload): boolean {
  const hasFec = (profile.fec_ids?.length ?? 0) > 0;
  const hasStateFiling = (profile.state_filing_ids?.length ?? 0) > 0;
  const hasOfficialWebsite = Boolean(normalizeOptionalUrl(profile.official_website_url));
  return Boolean(
    profile.date_of_birth ||
      profile.twitter_handle ||
      profile.linkedin_url ||
      hasOfficialWebsite ||
      hasFec ||
      hasStateFiling
  );
}

export function matchesByHardIdentifier(profile: CandidateProfilePayload, row: ExistingCandidateRow): boolean {
  if (profile.date_of_birth && row.date_of_birth && profile.date_of_birth === row.date_of_birth) {
    return true;
  }

  if (profile.twitter_handle && row.twitter_handle) {
    const normalizedProfileHandle = normalizeTwitterHandle(profile.twitter_handle);
    const normalizedRowHandle = normalizeTwitterHandle(row.twitter_handle);
    if (
      normalizedProfileHandle &&
      normalizedRowHandle &&
      normalizedProfileHandle === normalizedRowHandle
    ) {
      return true;
    }
  }

  if (profile.linkedin_url && row.linkedin_url) {
    if (normalizeOptionalUrl(profile.linkedin_url) === normalizeOptionalUrl(row.linkedin_url)) {
      return true;
    }
  }

  if (profile.official_website_url && row.official_website_url) {
    if (normalizeOptionalUrl(profile.official_website_url) === normalizeOptionalUrl(row.official_website_url)) {
      return true;
    }
  }

  const profileFecIds = normalizeIdList(profile.fec_ids);
  const rowFecIds = normalizeIdList(parseOptionalStringArray(row.fec_ids));
  if (profileFecIds.length > 0 && rowFecIds.length > 0 && hasNormalizedIntersection(profileFecIds, rowFecIds)) {
    return true;
  }

  const profileStateFilingIds = normalizeIdList(profile.state_filing_ids);
  const rowStateFilingIds = normalizeIdList(parseOptionalStringArray(row.state_filing_ids));
  if (
    profileStateFilingIds.length > 0 &&
    rowStateFilingIds.length > 0 &&
    hasNormalizedIntersection(profileStateFilingIds, rowStateFilingIds)
  ) {
    return true;
  }

  return false;
}

export async function loadSameNameCandidates(
  client: Pick<PoolClient, "query">,
  profile: CandidateProfilePayload,
  state: string
): Promise<ExistingCandidateRow[]> {
  const result = await client.query<ExistingCandidateRow>(
    `
      SELECT
        id,
        first_name,
        last_name,
        date_of_birth::text AS date_of_birth,
        twitter_handle,
        linkedin_url,
        official_website_url,
        fec_ids,
        state_filing_ids,
        current_office,
        state
      FROM public.candidates
      WHERE deleted_at IS NULL
        AND lower(first_name) = lower($1)
        AND lower(last_name) = lower($2)
        AND state = $3
    `,
    [profile.first_name, profile.last_name, state]
  );

  return result.rows;
}

export async function loadSameNameCandidatesAcrossStates(
  client: Pick<PoolClient, "query">,
  profile: CandidateProfilePayload
): Promise<ExistingCandidateRow[]> {
  const result = await client.query<ExistingCandidateRow>(
    `
      SELECT
        id,
        first_name,
        last_name,
        date_of_birth::text AS date_of_birth,
        twitter_handle,
        linkedin_url,
        official_website_url,
        fec_ids,
        state_filing_ids,
        current_office,
        state
      FROM public.candidates
      WHERE deleted_at IS NULL
        AND lower(first_name) = lower($1)
        AND lower(last_name) = lower($2)
    `,
    [profile.first_name, profile.last_name]
  );

  return result.rows;
}

async function insertCandidate(
  client: PoolClient,
  profile: CandidateProfilePayload,
  state: string,
  rosterParty: string | undefined,
  includeParty: boolean
): Promise<string> {
  const storedParty = includeParty ? rosterParty ?? profile.party ?? "Unknown" : "Nonpartisan";

  const insertResult = await client.query<{ id: string }>(
    `
      INSERT INTO public.candidates (
        display_name,
        first_name,
        last_name,
        date_of_birth,
        party,
        summary,
        twitter_handle,
        linkedin_url,
        fec_ids,
        state_filing_ids,
        state,
        official_website_url,
        profile_sources,
        current_office,
        last_researched
      )
      VALUES (
        $1,
        $2,
        $3,
        $4::date,
        $5,
        $6,
        $7,
        $8,
        $9::jsonb,
        $10::jsonb,
        $11,
        $12,
        $13::jsonb,
        $14,
        now()
      )
      RETURNING id
    `,
    [
      profile.display_name,
      profile.first_name,
      profile.last_name,
      profile.date_of_birth ?? null,
      storedParty,
      profile.summary ?? null,
      profile.twitter_handle ?? null,
      profile.linkedin_url ?? null,
      profile.fec_ids ? JSON.stringify(profile.fec_ids) : null,
      profile.state_filing_ids ? JSON.stringify(profile.state_filing_ids) : null,
      state,
      profile.official_website_url ?? null,
      JSON.stringify(profile.sources),
      profile.current_office ?? null,
    ]
  );

  const id = insertResult.rows[0]?.id;
  if (!id) {
    throw new Error("candidate insert returned no id");
  }

  return id;
}

async function mergeCandidateIdentifiersForExistingCandidate(
  client: PoolClient,
  candidateId: string,
  profile: CandidateProfilePayload,
  overwriteFields?: ReadonlySet<OverwritableProfileField>
): Promise<void> {
  const locked = await client.query<{
    fec_ids: unknown;
    state_filing_ids: unknown;
  }>(
    `
      SELECT fec_ids, state_filing_ids
      FROM public.candidates
      WHERE id = $1
        AND deleted_at IS NULL
      FOR UPDATE
    `,
    [candidateId]
  );
  const current = locked.rows[0];
  if (!current) {
    return;
  }

  const existingFecIds = parseOptionalStringArray(current.fec_ids);
  const existingStateFilingIds = parseOptionalStringArray(current.state_filing_ids);

  const mergedFecIds = mergeIdentifierLists(existingFecIds, profile.fec_ids);
  const mergedStateFilingIds = mergeIdentifierLists(existingStateFilingIds, profile.state_filing_ids);

  // Scalar fields fill empty columns but never overwrite non-empty stored
  // values, unless the caller explicitly listed the field for replacement.
  // Identifier lists stay additive and profile_sources always refreshes.
  const scalarValue = (field: OverwritableProfileField, value: string | undefined | null) => ({
    value: value ?? null,
    overwrite: overwriteFields?.has(field) ?? false,
  });
  const scalars = {
    date_of_birth: scalarValue("date_of_birth", profile.date_of_birth),
    twitter_handle: scalarValue("twitter_handle", profile.twitter_handle),
    linkedin_url: scalarValue("linkedin_url", profile.linkedin_url),
    official_website_url: scalarValue("official_website_url", profile.official_website_url),
    summary: scalarValue("summary", profile.summary),
    current_office: scalarValue("current_office", profile.current_office),
  };

  await client.query(
    `
      UPDATE public.candidates
      SET fec_ids = $2::jsonb,
          state_filing_ids = $3::jsonb,
          date_of_birth = CASE
            WHEN $6::boolean AND $5::date IS NOT NULL THEN $5::date
            WHEN date_of_birth IS NULL THEN $5::date
            ELSE date_of_birth
          END,
          twitter_handle = CASE
            WHEN $8::boolean AND $7::text IS NOT NULL THEN $7::text
            WHEN twitter_handle IS NULL OR length(trim(twitter_handle)) = 0 THEN COALESCE($7::text, twitter_handle)
            ELSE twitter_handle
          END,
          linkedin_url = CASE
            WHEN $10::boolean AND $9::text IS NOT NULL THEN $9::text
            WHEN linkedin_url IS NULL OR length(trim(linkedin_url)) = 0 THEN COALESCE($9::text, linkedin_url)
            ELSE linkedin_url
          END,
          official_website_url = CASE
            WHEN $12::boolean AND $11::text IS NOT NULL THEN $11::text
            WHEN official_website_url IS NULL OR length(trim(official_website_url)) = 0 THEN COALESCE($11::text, official_website_url)
            ELSE official_website_url
          END,
          summary = CASE
            WHEN $14::boolean AND $13::text IS NOT NULL THEN $13::text
            WHEN summary IS NULL OR length(trim(summary)) = 0 THEN COALESCE($13::text, summary)
            ELSE summary
          END,
          current_office = CASE
            WHEN $16::boolean AND $15::text IS NOT NULL THEN $15::text
            WHEN current_office IS NULL OR length(trim(current_office)) = 0 THEN COALESCE($15::text, current_office)
            ELSE current_office
          END,
          profile_sources = $4::jsonb,
          last_researched = now(),
          updated_at = now()
      WHERE id = $1
    `,
    [
      candidateId,
      mergedFecIds ? JSON.stringify(mergedFecIds) : null,
      mergedStateFilingIds ? JSON.stringify(mergedStateFilingIds) : null,
      JSON.stringify(profile.sources),
      scalars.date_of_birth.value,
      scalars.date_of_birth.overwrite,
      scalars.twitter_handle.value,
      scalars.twitter_handle.overwrite,
      scalars.linkedin_url.value,
      scalars.linkedin_url.overwrite,
      scalars.official_website_url.value,
      scalars.official_website_url.overwrite,
      scalars.summary.value,
      scalars.summary.overwrite,
      scalars.current_office.value,
      scalars.current_office.overwrite,
    ]
  );
}

async function findCandidateLinkedToElectionByDisplayName(
  client: PoolClient,
  electionId: string,
  displayName: string
): Promise<string | null> {
  const result = await client.query<{ id: string }>(
    `
      SELECT DISTINCT c.id
      FROM public.candidates c
      JOIN public.candidate_elections ce
        ON (ce.candidate_id = c.id OR ce.running_mate_candidate_id = c.id)
      WHERE ce.election_id = $1
        AND c.deleted_at IS NULL
        AND lower(trim(c.display_name)) = lower(trim($2))
    `,
    [electionId, displayName]
  );
  if (result.rows.length > 1) {
    throw new Error(
      `Multiple candidates named "${displayName}" are linked to election ${electionId}; resolve the duplicate rows before re-writing this profile.`
    );
  }
  return result.rows[0]?.id ?? null;
}

export async function findOrCreateCandidateFromProfile(
  input: FindOrCreateCandidateFromProfileInput
): Promise<FindOrCreateCandidateFromProfileResult> {
  const existingCandidates = input.allowCrossStateHardIdentifierMatch
    ? await loadSameNameCandidatesAcrossStates(input.client, input.profile)
    : await loadSameNameCandidates(input.client, input.profile, input.state);

  if (hasAtLeastOneHardIdentifier(input.profile)) {
    const matched = existingCandidates.filter((row) => matchesByHardIdentifier(input.profile, row));
    if (matched.length === 1) {
      const matchedCandidate = matched[0]!;
      await mergeCandidateIdentifiersForExistingCandidate(
        input.client,
        matchedCandidate.id,
        input.profile,
        input.overwriteProfileFields
      );
      return { candidateId: matchedCandidate.id, matchedExisting: true };
    }
  }

  if (input.matchByLinkedElectionId) {
    const linkedCandidateId = await findCandidateLinkedToElectionByDisplayName(
      input.client,
      input.matchByLinkedElectionId,
      input.profile.display_name
    );
    if (linkedCandidateId) {
      await mergeCandidateIdentifiersForExistingCandidate(
        input.client,
        linkedCandidateId,
        input.profile,
        input.overwriteProfileFields
      );
      return { candidateId: linkedCandidateId, matchedExisting: true };
    }
  }

  const candidateId = await insertCandidate(
    input.client,
    input.profile,
    input.state,
    input.rosterParty,
    input.includeParty
  );
  return { candidateId, matchedExisting: false };
}

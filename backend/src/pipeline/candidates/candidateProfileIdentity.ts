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
  state: string;
};

export type FindOrCreateCandidateFromProfileInput = {
  client: PoolClient;
  profile: CandidateProfilePayload;
  state: string;
  rosterParty: string | undefined;
  includeParty: boolean;
  allowCrossStateHardIdentifierMatch?: boolean;
};

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
  profile: CandidateProfilePayload
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

  const fecIdsChanged = !haveSameNormalizedIdentifierSet(existingFecIds, mergedFecIds);
  const stateFilingChanged = !haveSameNormalizedIdentifierSet(existingStateFilingIds, mergedStateFilingIds);
  if (!fecIdsChanged && !stateFilingChanged) {
    return;
  }

  await client.query(
    `
      UPDATE public.candidates
      SET fec_ids = $2::jsonb,
          state_filing_ids = $3::jsonb,
          updated_at = now()
      WHERE id = $1
    `,
    [
      candidateId,
      mergedFecIds ? JSON.stringify(mergedFecIds) : null,
      mergedStateFilingIds ? JSON.stringify(mergedStateFilingIds) : null,
    ]
  );
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
      await mergeCandidateIdentifiersForExistingCandidate(input.client, matchedCandidate.id, input.profile);
      return { candidateId: matchedCandidate.id, matchedExisting: true };
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

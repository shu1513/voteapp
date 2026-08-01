import type { PoolClient } from "pg";

import type { CandidateProfilePayload } from "../../contracts/candidateProfilePayloadContract.js";
import { assertCandidatePartyWillNotBeDiscarded } from "../../ai/candidatePartisanship.js";
import {
  hasNormalizedIntersection,
  normalizeCandidateName,
  normalizeOptionalUrl,
  normalizeTwitterHandle,
} from "../../utils/candidateIdentity.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";

/**
 * Two or more same-name candidates match the incoming profile's hard
 * identifiers: the data already holds duplicates for this person, and
 * inserting yet another row would compound them. Callers park or surface
 * the profile so an operator can merge the duplicate rows first.
 */
export class AmbiguousCandidateIdentityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AmbiguousCandidateIdentityError";
  }
}

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
  // Field names whose stored values are set to NULL (deliberate retraction of
  // stale data, e.g. a current_office the person no longer holds). Clearing
  // only applies when an existing candidate matched — there is nothing to
  // clear on an insert — and a field cannot be both cleared and supplied.
  // Manual-wrapper-only; the AI pipeline never passes this.
  clearProfileFields?: ReadonlySet<OverwritableProfileField>;
};

export const OVERWRITABLE_PROFILE_FIELDS = [
  "party",
  "date_of_birth",
  "twitter_handle",
  "linkedin_url",
  "official_website_url",
  "summary",
  "current_office",
  // Tri-state routing fact. Fill-if-NULL by default; listing it under
  // --replace-profile-fields is the supported correction path for a stale
  // stored value (the records writers refuse to sweep past a contradiction).
  // Replacing with a payload null is the repair path for a false that was
  // manufactured from office-history-silent sources — the row returns to
  // "unanswered" instead of asserting an unresearched negative. Not listed
  // under --clear-profile-fields: the contract requires an explicit answer,
  // so a retraction travels in the payload, not as a flag-only clear.
  "has_held_public_office",
  // Provenance list. Merged as a UNION of stored + incoming by default: the
  // merge UPDATE keeps stored facts (fill-if-empty scalars, additive id
  // lists), so the sources supporting those surviving facts must survive
  // too — a narrow correction payload must not wipe the row's provenance.
  // Listing it under --replace-profile-fields swaps in exactly the payload's
  // list, the deliberate cleanup path for dead or disallowed stored URLs.
  // Not clearable: the profile contract requires sources on every payload.
  "profile_sources",
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

/**
 * Union of stored and incoming profile sources. Dedupes on the NORMALIZED
 * URL (trailing slash, hash), not the raw string: stored legacy rows predate
 * contract-side normalizeHttpUrl, so "https://a.example/page/" and its
 * normalized twin are the same source. First occurrence wins and stored
 * order comes first, so repeated re-writes are stable.
 *
 * The normalized key keeps its case: URL parsing already lowercases the
 * scheme and hostname — the components that ARE case-insensitive — while
 * paths and queries are case-sensitive per RFC 3986, so "/Bio" and "/bio"
 * may be distinct documents. Collapsing them would silently discard a
 * source, the failure this union exists to prevent; keeping a duplicate
 * URL is harmless and cleanable via --replace-profile-fields. Only the
 * non-URL fallback (legacy stored junk — the contract refuses non-URL
 * incoming sources) compares case-insensitively.
 */
export function mergeProfileSourceLists(
  existing: readonly string[],
  incoming: readonly string[]
): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();

  for (const source of [existing, incoming]) {
    for (const value of source) {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        continue;
      }
      const key = normalizeHttpUrl(trimmed) ?? trimmed.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      merged.push(trimmed);
    }
  }

  return merged;
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

export function resolveStoredCandidateParty(input: {
  includeParty: boolean;
  rosterParty: string | undefined;
  profileParty: string | undefined;
}): string {
  return input.includeParty
    ? input.rosterParty ?? input.profileParty ?? "Unknown"
    : "Nonpartisan";
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
  storedParty: string
): Promise<string> {
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
        has_held_public_office,
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
        $15::boolean,
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
      profile.has_held_public_office ?? null,
    ]
  );

  const id = insertResult.rows[0]?.id;
  if (!id) {
    throw new Error("candidate insert returned no id");
  }

  return id;
}

/**
 * Refuse a merge whose EFFECTIVE post-write state would say "holds
 * current_office X" and "has NEVER held public office" at once — the payload
 * contract already rejects that pair inside one payload, but fill-if-empty
 * merging can assemble it across writes (stored office + incoming false
 * filling a NULL column, or incoming office filling a blank while a stored
 * false survives). A contradictory row would route the record sweep onto the
 * wrong question list, which is the exact failure this column exists to end.
 * Mirrors the merge UPDATE's CASE semantics.
 */
export function assertMergedOfficeRoutingConsistent(input: {
  profile: CandidateProfilePayload;
  storedCurrentOffice: string | null;
  storedHasHeldPublicOffice: boolean | null;
  overwriteFields?: ReadonlySet<OverwritableProfileField>;
  clearFields?: ReadonlySet<OverwritableProfileField>;
}): void {
  const incomingHasHeld = input.profile.has_held_public_office ?? null;
  const effectiveHasHeld = input.overwriteFields?.has("has_held_public_office")
    ? incomingHasHeld
    : input.storedHasHeldPublicOffice ?? incomingHasHeld;

  const storedOffice = input.storedCurrentOffice?.trim() || null;
  const incomingOffice = input.profile.current_office?.trim() || null;
  const effectiveOffice = input.clearFields?.has("current_office")
    ? null
    : input.overwriteFields?.has("current_office") && incomingOffice !== null
      ? incomingOffice
      : storedOffice ?? incomingOffice;

  if (effectiveHasHeld === false && effectiveOffice !== null) {
    throw new Error(
      `Profile merge would leave a contradictory candidate row: current_office would be "${effectiveOffice}" (stored: ${JSON.stringify(input.storedCurrentOffice)}, incoming: ${JSON.stringify(input.profile.current_office ?? null)}) while has_held_public_office would be false — a candidate holding a public office now HAS held public office. If the stored office is stale or holds an occupation, clear or replace it (--clear-profile-fields current_office / --replace-profile-fields current_office); if the stored false routing answer is stale, correct it with --replace-profile-fields has_held_public_office. Nothing was written.`
    );
  }

  // A null-retraction is refused while a current office would survive the
  // write: the office itself proves the answer is true, so retracting to
  // "unanswered" beside it either erases a provable fact or leaves a stale
  // office standing. Deliberately narrower than "reject office + non-true":
  // legacy rows hold current_office with a NULL answer wholesale, and a
  // routine pass that answers null from office-history-silent sources must
  // still be able to fill their OTHER empty fields — only the write that
  // actively retracts is refused, not one that merely fails to repair
  // pre-existing state.
  if (
    input.overwriteFields?.has("has_held_public_office") &&
    incomingHasHeld === null &&
    effectiveOffice !== null
  ) {
    throw new Error(
      `Profile merge refuses to retract has_held_public_office to null while current_office would remain "${effectiveOffice}" (stored: ${JSON.stringify(input.storedCurrentOffice)}) — a standing current office proves the answer is true. If the stored office is real, drop the retraction (or answer true); if it is stale or holds an occupation, clear or replace it in the same write (--clear-profile-fields current_office / --replace-profile-fields current_office). Nothing was written.`
    );
  }
}

async function mergeCandidateIdentifiersForExistingCandidate(
  client: PoolClient,
  candidateId: string,
  profile: CandidateProfilePayload,
  storedParty: string,
  overwriteFields?: ReadonlySet<OverwritableProfileField>,
  clearFields?: ReadonlySet<OverwritableProfileField>
): Promise<void> {
  const locked = await client.query<{
    fec_ids: unknown;
    state_filing_ids: unknown;
    current_office: string | null;
    has_held_public_office: boolean | null;
    party: string | null;
    profile_sources: unknown;
  }>(
    `
      SELECT fec_ids, state_filing_ids, current_office, has_held_public_office, party, profile_sources
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

  assertMergedOfficeRoutingConsistent({
    profile,
    storedCurrentOffice: current.current_office,
    storedHasHeldPublicOffice: current.has_held_public_office,
    overwriteFields,
    clearFields,
  });

  const existingFecIds = parseOptionalStringArray(current.fec_ids);
  const existingStateFilingIds = parseOptionalStringArray(current.state_filing_ids);

  const mergedFecIds = mergeIdentifierLists(existingFecIds, profile.fec_ids);
  const mergedStateFilingIds = mergeIdentifierLists(existingStateFilingIds, profile.state_filing_ids);

  // profile_sources follows the same additive logic as the fact columns it
  // vouches for: the UPDATE below keeps stored facts, so a narrow correction
  // payload must not wipe the sources supporting them. Only an explicit
  // --replace-profile-fields profile_sources swaps in the payload's list.
  const mergedProfileSources = overwriteFields?.has("profile_sources")
    ? [...profile.sources]
    : mergeProfileSourceLists(parseOptionalStringArray(current.profile_sources), profile.sources);

  // Scalar fields fill empty columns but never overwrite non-empty stored
  // values, unless the caller explicitly listed the field for replacement —
  // or listed it for clearing, which sets the column to NULL and wins over
  // every other branch. Identifier lists stay additive; profile_sources
  // unions with the stored list (replace only when explicitly listed).
  const scalarValue = (field: OverwritableProfileField, value: string | undefined | null) => ({
    value: value ?? null,
    overwrite: overwriteFields?.has(field) ?? false,
    clear: clearFields?.has(field) ?? false,
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
            WHEN $17::boolean THEN NULL
            WHEN $6::boolean AND $5::date IS NOT NULL THEN $5::date
            WHEN date_of_birth IS NULL THEN $5::date
            ELSE date_of_birth
          END,
          twitter_handle = CASE
            WHEN $18::boolean THEN NULL
            WHEN $8::boolean AND $7::text IS NOT NULL AND length(trim($7::text)) > 0 THEN $7::text
            WHEN twitter_handle IS NULL OR length(trim(twitter_handle)) = 0 THEN COALESCE($7::text, twitter_handle)
            ELSE twitter_handle
          END,
          linkedin_url = CASE
            WHEN $19::boolean THEN NULL
            WHEN $10::boolean AND $9::text IS NOT NULL AND length(trim($9::text)) > 0 THEN $9::text
            WHEN linkedin_url IS NULL OR length(trim(linkedin_url)) = 0 THEN COALESCE($9::text, linkedin_url)
            ELSE linkedin_url
          END,
          official_website_url = CASE
            WHEN $20::boolean THEN NULL
            WHEN $12::boolean AND $11::text IS NOT NULL AND length(trim($11::text)) > 0 THEN $11::text
            WHEN official_website_url IS NULL OR length(trim(official_website_url)) = 0 THEN COALESCE($11::text, official_website_url)
            ELSE official_website_url
          END,
          summary = CASE
            WHEN $21::boolean THEN NULL
            WHEN $14::boolean AND $13::text IS NOT NULL AND length(trim($13::text)) > 0 THEN $13::text
            WHEN summary IS NULL OR length(trim(summary)) = 0 THEN COALESCE($13::text, summary)
            ELSE summary
          END,
          current_office = CASE
            WHEN $22::boolean THEN NULL
            WHEN $16::boolean AND $15::text IS NOT NULL AND length(trim($15::text)) > 0 THEN $15::text
            WHEN current_office IS NULL OR length(trim(current_office)) = 0 THEN COALESCE($15::text, current_office)
            ELSE current_office
          END,
          has_held_public_office = CASE
            -- Overwrite takes the payload value even when it is NULL: a
            -- stored false manufactured from office-history-silent sources
            -- is repaired by an explicit payload null under
            -- --replace-profile-fields, returning the row to "unanswered".
            WHEN $24::boolean THEN $23::boolean
            WHEN has_held_public_office IS NULL THEN $23::boolean
            ELSE has_held_public_office
          END,
          party = CASE
            WHEN $26::boolean AND length(trim($25::text)) > 0 THEN $25::text
            -- party has been NOT NULL since migration 001. Repair blank
            -- values, but never turn schema-drift NULLs into "Unknown"
            -- without the explicit overwrite branch above.
            WHEN party IS NOT NULL AND length(trim(party)) = 0 THEN $25::text
            ELSE party
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
      JSON.stringify(mergedProfileSources),
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
      scalars.date_of_birth.clear,
      scalars.twitter_handle.clear,
      scalars.linkedin_url.clear,
      scalars.official_website_url.clear,
      scalars.summary.clear,
      scalars.current_office.clear,
      // Tri-state routing fact: fill-if-NULL, replace only when explicitly
      // listed (including replace with an explicit payload null). Never
      // cleared via --clear-profile-fields — the contract requires an
      // answer, so a retraction travels as a payload null instead.
      profile.has_held_public_office ?? null,
      overwriteFields?.has("has_held_public_office") ?? false,
      storedParty,
      overwriteFields?.has("party") ?? false,
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
  assertCandidatePartyWillNotBeDiscarded({
    includeParty: input.includeParty,
    partyLabels: [input.rosterParty, input.profile.party],
  });
  const storedParty = resolveStoredCandidateParty({
    includeParty: input.includeParty,
    rosterParty: input.rosterParty,
    profileParty: input.profile.party,
  });
  // Serialize identity resolution per person: the read below and the insert
  // at the bottom are otherwise an unlocked read-then-insert, and the
  // candidates table has no uniqueness constraint — two workers processing
  // the same person concurrently would both see no match and insert
  // duplicate rows. The transaction-scoped advisory lock releases on
  // commit/rollback. Keyed by name only (not state) so the cross-state
  // presidential path contends with the per-state path for the same person.
  const lockName = normalizeCandidateName(`${input.profile.first_name} ${input.profile.last_name}`);
  await input.client.query(
    `SELECT pg_advisory_xact_lock(hashtextextended('candidate_identity:' || $1, 0))`,
    [lockName]
  );

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
        storedParty,
        input.overwriteProfileFields,
        input.clearProfileFields
      );
      return { candidateId: matchedCandidate.id, matchedExisting: true };
    }
    if (matched.length > 1) {
      // Falling through would insert a third row for an already-duplicated
      // person. Surface instead so an operator merges the duplicates.
      throw new AmbiguousCandidateIdentityError(
        `Multiple existing candidates named "${input.profile.display_name}" match this profile's hard identifiers ` +
          `(${matched.map((row) => row.id).join(", ")}); merge the duplicate rows before re-writing this profile.`
      );
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
        storedParty,
        input.overwriteProfileFields,
        input.clearProfileFields
      );
      return { candidateId: linkedCandidateId, matchedExisting: true };
    }
  }

  const candidateId = await insertCandidate(
    input.client,
    input.profile,
    input.state,
    storedParty
  );
  return { candidateId, matchedExisting: false };
}

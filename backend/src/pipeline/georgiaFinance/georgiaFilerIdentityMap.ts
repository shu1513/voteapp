import type { Pool, PoolClient } from "pg";

// Access module for ga_finance_filer_identity_map (georgia_plan.md D3).
// The map ties one canonical PeachFile entity to its per-host registrations —
// the SAME registration chain re-keyed across the two portals, never a
// legally separate committee. Sync legs (PR 4+) read the map to know which
// per-host registrations to pull and whether each row's money belongs in the
// candidate's totals.

type Queryable = Pick<Pool | PoolClient, "query">;

export type GeorgiaFilerSourceSystem = "peachfile" | "efile_archive";
export type GeorgiaFilerEntityRole = "candidate_committee" | "outside_spender";
export type GeorgiaFilerMapProvenance = "reconciled" | "manual";

export type GeorgiaFilerIdentityMapRow = {
  canonicalCommitteeId: string;
  canonicalCommitteeName: string;
  entityRole: GeorgiaFilerEntityRole;
  sourceSystem: GeorgiaFilerSourceSystem;
  sourceFilerEntityId: string;
  sourceRegistrationGuid: string;
  sourceFilerName: string;
  sourceCommitteeName: string | null;
  sourceFilingCycleName: string | null;
  includeInCandidateTotals: boolean;
  mapProvenance: GeorgiaFilerMapProvenance;
  notes: string | null;
  lastVerifiedAt: Date;
};

const SOURCE_SYSTEMS: readonly GeorgiaFilerSourceSystem[] = ["peachfile", "efile_archive"];
const ENTITY_ROLES: readonly GeorgiaFilerEntityRole[] = ["candidate_committee", "outside_spender"];
const PROVENANCES: readonly GeorgiaFilerMapProvenance[] = ["reconciled", "manual"];
const GUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function requireGeorgiaFilerEntityId(value: string, fieldName: string): string {
  const trimmed = value.trim();
  // Both hosts key filers by a positive integer filerEntityId (F3/A6).
  if (!/^[1-9]\d*$/.test(trimmed)) {
    throw new Error(`Invalid Georgia ${fieldName}: ${JSON.stringify(value)}`);
  }
  return trimmed;
}

export function requireGeorgiaRegistrationGuid(value: string): string {
  const trimmed = value.trim().toLowerCase();
  if (!GUID_PATTERN.test(trimmed)) {
    throw new Error(`Invalid Georgia registration guid: ${JSON.stringify(value)}`);
  }
  return trimmed;
}

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`Georgia filer identity map ${fieldName} is required`);
  }
  return trimmed;
}

function normalizeOptional(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function requireMember<T extends string>(value: T, allowed: readonly T[], fieldName: string): T {
  if (!allowed.includes(value)) {
    throw new Error(`Invalid Georgia filer identity map ${fieldName}: ${JSON.stringify(value)}`);
  }
  return value;
}

function validateGeorgiaFilerIdentityMapRow(row: GeorgiaFilerIdentityMapRow): GeorgiaFilerIdentityMapRow {
  const entityRole = requireMember(row.entityRole, ENTITY_ROLES, "entity role");
  if (entityRole === "outside_spender" && row.includeInCandidateTotals) {
    // Mirrors the ga_ffim_outside_spender_totals_check constraint so bad
    // input fails with a readable message before it reaches the DB.
    throw new Error("Georgia outside-spender map rows can never be included in candidate totals");
  }
  return {
    canonicalCommitteeId: requireGeorgiaFilerEntityId(row.canonicalCommitteeId, "canonical committee id"),
    canonicalCommitteeName: requireNonEmpty(row.canonicalCommitteeName, "canonical committee name"),
    entityRole,
    sourceSystem: requireMember(row.sourceSystem, SOURCE_SYSTEMS, "source system"),
    sourceFilerEntityId: requireGeorgiaFilerEntityId(row.sourceFilerEntityId, "source filer entity id"),
    sourceRegistrationGuid: requireGeorgiaRegistrationGuid(row.sourceRegistrationGuid),
    sourceFilerName: requireNonEmpty(row.sourceFilerName, "source filer name"),
    sourceCommitteeName: normalizeOptional(row.sourceCommitteeName),
    sourceFilingCycleName: normalizeOptional(row.sourceFilingCycleName),
    includeInCandidateTotals: row.includeInCandidateTotals,
    mapProvenance: requireMember(row.mapProvenance, PROVENANCES, "provenance"),
    notes: normalizeOptional(row.notes),
    lastVerifiedAt: row.lastVerifiedAt,
  };
}

type MapQueryRow = {
  canonical_committee_id: string;
  canonical_committee_name: string;
  entity_role: GeorgiaFilerEntityRole;
  source_system: GeorgiaFilerSourceSystem;
  source_filer_entity_id: string;
  source_registration_guid: string;
  source_filer_name: string;
  source_committee_name: string | null;
  source_filing_cycle_name: string | null;
  include_in_candidate_totals: boolean;
  map_provenance: GeorgiaFilerMapProvenance;
  notes: string | null;
  last_verified_at: Date;
};

function mapQueryRow(row: MapQueryRow): GeorgiaFilerIdentityMapRow {
  return {
    canonicalCommitteeId: row.canonical_committee_id,
    canonicalCommitteeName: row.canonical_committee_name,
    entityRole: row.entity_role,
    sourceSystem: row.source_system,
    sourceFilerEntityId: row.source_filer_entity_id,
    sourceRegistrationGuid: row.source_registration_guid,
    sourceFilerName: row.source_filer_name,
    sourceCommitteeName: row.source_committee_name,
    sourceFilingCycleName: row.source_filing_cycle_name,
    includeInCandidateTotals: row.include_in_candidate_totals,
    mapProvenance: row.map_provenance,
    notes: row.notes,
    lastVerifiedAt: row.last_verified_at,
  };
}

const SELECT_COLUMNS = `
  canonical_committee_id,
  canonical_committee_name,
  entity_role,
  source_system,
  source_filer_entity_id,
  source_registration_guid::text AS source_registration_guid,
  source_filer_name,
  source_committee_name,
  source_filing_cycle_name,
  include_in_candidate_totals,
  map_provenance,
  notes,
  last_verified_at
`;

export async function listGeorgiaFilerIdentityMapRowsByCanonicalCommittee(
  db: Queryable,
  canonicalCommitteeId: string
): Promise<GeorgiaFilerIdentityMapRow[]> {
  const result = await db.query<MapQueryRow>(
    `
      SELECT ${SELECT_COLUMNS}
      FROM public.ga_finance_filer_identity_map
      WHERE canonical_committee_id = $1
      ORDER BY source_system ASC, source_registration_guid ASC
    `,
    [requireGeorgiaFilerEntityId(canonicalCommitteeId, "canonical committee id")]
  );
  return result.rows.map(mapQueryRow);
}

export async function upsertGeorgiaFilerIdentityMapRow(input: {
  db: Queryable;
  row: GeorgiaFilerIdentityMapRow;
}): Promise<{ id: string }> {
  const row = validateGeorgiaFilerIdentityMapRow(input.row);
  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.ga_finance_filer_identity_map (
        canonical_committee_id,
        canonical_committee_name,
        entity_role,
        source_system,
        source_filer_entity_id,
        source_registration_guid,
        source_filer_name,
        source_committee_name,
        source_filing_cycle_name,
        include_in_candidate_totals,
        map_provenance,
        notes,
        last_verified_at
      )
      VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $8, $9, $10, $11, $12, $13)
      ON CONFLICT (source_system, source_registration_guid)
      DO UPDATE SET
        canonical_committee_id = EXCLUDED.canonical_committee_id,
        canonical_committee_name = EXCLUDED.canonical_committee_name,
        entity_role = EXCLUDED.entity_role,
        source_filer_entity_id = EXCLUDED.source_filer_entity_id,
        source_filer_name = EXCLUDED.source_filer_name,
        source_committee_name = EXCLUDED.source_committee_name,
        source_filing_cycle_name = EXCLUDED.source_filing_cycle_name,
        include_in_candidate_totals = EXCLUDED.include_in_candidate_totals,
        map_provenance = EXCLUDED.map_provenance,
        notes = EXCLUDED.notes,
        last_verified_at = EXCLUDED.last_verified_at
      RETURNING id::text AS id
    `,
    [
      row.canonicalCommitteeId,
      row.canonicalCommitteeName,
      row.entityRole,
      row.sourceSystem,
      row.sourceFilerEntityId,
      row.sourceRegistrationGuid,
      row.sourceFilerName,
      row.sourceCommitteeName,
      row.sourceFilingCycleName,
      row.includeInCandidateTotals,
      row.mapProvenance,
      row.notes,
      row.lastVerifiedAt,
    ]
  );
  const id = result.rows[0]?.id;
  if (!id) {
    throw new Error("Georgia filer identity map upsert returned no id");
  }
  return { id };
}

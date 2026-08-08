import { describe, expect, it, vi } from "vitest";

import {
  listGeorgiaFilerIdentityMapRowsByCanonicalCommittee,
  requireGeorgiaFilerEntityId,
  requireGeorgiaRegistrationGuid,
  upsertGeorgiaFilerIdentityMapRow,
  type GeorgiaFilerIdentityMapRow,
} from "../../../src/pipeline/georgiaFinance/georgiaFilerIdentityMap.js";

const NOW = new Date("2026-08-07T00:00:00.000Z");

// The Carr chain from spike result A6: canonical PeachFile 100035, archive
// re-key 757274 — the same registration chain across systems.
function archiveChainRow(overrides: Partial<GeorgiaFilerIdentityMapRow> = {}): GeorgiaFilerIdentityMapRow {
  return {
    canonicalCommitteeId: "100035",
    canonicalCommitteeName: "Carr for Georgia, Inc.",
    entityRole: "candidate_committee",
    sourceSystem: "efile_archive",
    sourceFilerEntityId: "757274",
    sourceRegistrationGuid: "b31a4752-7fc6-45fb-b9b6-ffb2293d7f9e",
    sourceFilerName: "Carr, Christopher Michael",
    sourceCommitteeName: "Carr for Georgia, Inc.",
    sourceFilingCycleName: "2026 State/Statewide Election Cycle for Candidates (January and June)",
    includeInCandidateTotals: true,
    mapProvenance: "reconciled",
    notes: null,
    lastVerifiedAt: NOW,
    ...overrides,
  };
}

describe("validation", () => {
  it("requires positive-integer filer entity ids and well-formed guids", () => {
    expect(requireGeorgiaFilerEntityId(" 100035 ", "id")).toBe("100035");
    expect(() => requireGeorgiaFilerEntityId("0", "id")).toThrow(/Invalid Georgia id/);
    expect(() => requireGeorgiaFilerEntityId("abc", "id")).toThrow(/Invalid Georgia id/);
    expect(requireGeorgiaRegistrationGuid("B31A4752-7FC6-45FB-B9B6-FFB2293D7F9E")).toBe(
      "b31a4752-7fc6-45fb-b9b6-ffb2293d7f9e"
    );
    expect(() => requireGeorgiaRegistrationGuid("not-a-guid")).toThrow(/Invalid Georgia registration guid/);
  });

  it("refuses outside-spender rows marked as inside candidate totals", async () => {
    const db = { query: vi.fn() };
    await expect(
      upsertGeorgiaFilerIdentityMapRow({
        db,
        row: archiveChainRow({ entityRole: "outside_spender", includeInCandidateTotals: true }),
      })
    ).rejects.toThrow(/never be included in candidate totals/);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects unknown source systems and provenances", async () => {
    const db = { query: vi.fn() };
    await expect(
      upsertGeorgiaFilerIdentityMapRow({
        db,
        row: archiveChainRow({ sourceSystem: "efile" as never }),
      })
    ).rejects.toThrow(/source system/);
    await expect(
      upsertGeorgiaFilerIdentityMapRow({
        db,
        row: archiveChainRow({ mapProvenance: "guessed" as never }),
      })
    ).rejects.toThrow(/provenance/);
  });
});

describe("upsertGeorgiaFilerIdentityMapRow", () => {
  it("inserts with conflict target (source_system, source_registration_guid) and returns the id", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: "map-row-1" }] }) };
    const result = await upsertGeorgiaFilerIdentityMapRow({ db, row: archiveChainRow() });
    expect(result).toEqual({ id: "map-row-1" });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("INSERT INTO public.ga_finance_filer_identity_map");
    expect(sql).toContain("ON CONFLICT (source_system, source_registration_guid)");
    expect(params).toEqual([
      "100035",
      "Carr for Georgia, Inc.",
      "candidate_committee",
      "efile_archive",
      "757274",
      "b31a4752-7fc6-45fb-b9b6-ffb2293d7f9e",
      "Carr, Christopher Michael",
      "Carr for Georgia, Inc.",
      "2026 State/Statewide Election Cycle for Candidates (January and June)",
      true,
      "reconciled",
      null,
      NOW,
    ]);
  });
});

describe("listGeorgiaFilerIdentityMapRowsByCanonicalCommittee", () => {
  it("maps snake_case rows and validates the canonical id", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            canonical_committee_id: "100035",
            canonical_committee_name: "Carr for Georgia, Inc.",
            entity_role: "candidate_committee",
            source_system: "peachfile",
            source_filer_entity_id: "100035",
            source_registration_guid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
            source_filer_name: "Carr for Georgia, Inc.",
            source_committee_name: "Carr for Georgia, Inc.",
            source_filing_cycle_name: "2026 Candidate/Committee Filing Cycle",
            include_in_candidate_totals: true,
            map_provenance: "reconciled",
            notes: null,
            last_verified_at: NOW,
          },
        ],
      }),
    };
    const rows = await listGeorgiaFilerIdentityMapRowsByCanonicalCommittee(db, "100035");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      canonicalCommitteeId: "100035",
      sourceSystem: "peachfile",
      sourceRegistrationGuid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
      includeInCandidateTotals: true,
    });
    await expect(listGeorgiaFilerIdentityMapRowsByCanonicalCommittee(db, "nope")).rejects.toThrow(
      /Invalid Georgia canonical committee id/
    );
  });
});

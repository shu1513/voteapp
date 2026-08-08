import { describe, expect, it, vi } from "vitest";
import {
  flagSanFranciscoFinanceLinksMissingFromManifest,
  replaceSanFranciscoOutsideCommitteeLinks,
  upsertSanFranciscoFinanceLink,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoFinanceWriter.js";

const LINK = {
  candidateId: "cand-1",
  electionId: "elec-1",
  electionYear: 2026,
  candidateNameNormalized: "ALAN WONG",
  contestCode: "bos04",
  fppcId: "1489126",
  filerNid: "216198377",
  committeeName: "ALAN WONG FOR SUPERVISOR 2026 GENERAL",
  linkSource: "sfec_dashboard" as const,
};

describe("upsertSanFranciscoFinanceLink", () => {
  it("reuses a protected manual link with the same committee", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [{ id: "manual-1", fppc_id: "1489126" }],
        }),
    };
    const result = await upsertSanFranciscoFinanceLink({ db, link: LINK });
    expect(result.linkId).toBe("manual-1");
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("refuses to override a manual link with a different committee", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [{ id: "manual-1", fppc_id: "9999999" }],
        }),
    };
    await expect(
      upsertSanFranciscoFinanceLink({ db, link: LINK }),
    ).rejects.toThrow(/protected manual link/);
  });

  it("deactivates other automatic links, then upserts", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [] }) // manual-link probe
        .mockResolvedValueOnce({ rows: [] }) // deactivation
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }] }),
    };
    const result = await upsertSanFranciscoFinanceLink({ db, link: LINK });
    expect(result.linkId).toBe("link-1");
    const [deactivateSql] = db.query.mock.calls[1]!;
    expect(deactivateSql).toContain("link_status='inactive'");
    expect(deactivateSql).toContain("link_source<>'manual'");
    const [insertSql, insertParams] = db.query.mock.calls[2]!;
    expect(insertSql).toContain("ON CONFLICT (candidate_id,election_id,fppc_id)");
    expect(insertParams).toEqual([
      "cand-1",
      "elec-1",
      2026,
      "ALAN WONG",
      "bos04",
      "1489126",
      "216198377",
      "ALAN WONG FOR SUPERVISOR 2026 GENERAL",
      "active",
      "sfec_dashboard",
      null,
      null,
    ]);
  });

  it("writes a needs_review link without touching active links", async () => {
    const db = {
      query: vi.fn().mockResolvedValueOnce({ rows: [{ id: "link-2" }] }),
    };
    const result = await upsertSanFranciscoFinanceLink({
      db,
      link: { ...LINK, linkStatus: "needs_review" },
    });
    expect(result.linkId).toBe("link-2");
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(db.query.mock.calls[0]![0]).toContain("INSERT INTO");
  });
});

describe("flagSanFranciscoFinanceLinksMissingFromManifest", () => {
  it("flags active automatic links whose committee left the manifest", async () => {
    const db = {
      query: vi.fn().mockResolvedValueOnce({ rows: [{ id: "stale-1" }] }),
    };
    const flagged = await flagSanFranciscoFinanceLinksMissingFromManifest({
      db,
      electionId: "elec-1",
      presentFppcIds: ["1489126", "1491969"],
    });
    expect(flagged).toEqual(["stale-1"]);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("link_status='needs_review'");
    expect(sql).toContain("link_source='sfec_dashboard'");
    expect(params).toEqual(["elec-1", ["1489126", "1491969"]]);
  });
});

describe("replaceSanFranciscoOutsideCommitteeLinks", () => {
  it("deletes then inserts the manifest's relation set", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await replaceSanFranciscoOutsideCommitteeLinks({
      db,
      candidateId: "cand-1",
      electionId: "elec-1",
      electionYear: 2026,
      relations: [
        {
          spenderFppcId: "1488188",
          spenderName: "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
          supportOppose: "support",
          sourceUrl: "https://example.test/bos04",
        },
        {
          spenderFppcId: "name:AFFORDABLE SF NOW",
          spenderName: "AFFORDABLE SF NOW",
          supportOppose: "oppose",
        },
      ],
      lastVerifiedAt: new Date("2026-08-07T00:00:00Z"),
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(db.query.mock.calls[0]![0]).toContain("DELETE FROM");
    const [insertSql, insertParams] = db.query.mock.calls[1]!;
    expect(insertSql).toContain("ON CONFLICT");
    expect(insertParams).toEqual([
      "cand-1",
      "elec-1",
      2026,
      "1488188",
      "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
      "support",
      "https://example.test/bos04",
      "2026-08-07T00:00:00.000Z",
    ]);
  });

  it("clears every relation when the manifest has none", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await replaceSanFranciscoOutsideCommitteeLinks({
      db,
      candidateId: "cand-1",
      electionId: "elec-1",
      electionYear: 2026,
      relations: [],
      lastVerifiedAt: new Date(),
    });
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(db.query.mock.calls[0]![0]).toContain("DELETE FROM");
  });
});

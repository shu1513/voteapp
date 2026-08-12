import { describe, expect, it, vi } from "vitest";
import { autoLinkMissingSanFranciscoCandidateFinanceLinks } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceAutoLink.js";

// Compact real-shape bos04 general manifest: Wong (on ballot, id-linked),
// Gee (committee former, not on the November ballot), Greco (on ballot, no
// roster id — name path); GrowSF supports Wong and opposes Gee.
const BOS04_MANIFEST = `---
layout: contest_candidate
election: '2026-11-03'
title: Board of Supervisors D04
candidates:
- filer_nid: '216198377'
  filer_id: '1489126'
  committee_name: ALAN WONG FOR SUPERVISOR 2026 GENERAL
  candidate_name: ALAN WONG
  funds: 33459.98
  expenses: 7653.53
- filer_nid: '216135683'
  filer_id: '1490199'
  committee_name: GEE FOR SUPERVISOR 2026
  candidate_name: NATALIE GEE
  funds: 7210.12
  expenses: 5561.03
- filer_nid: '216781160'
  filer_id: '1491969'
  committee_name: GRECO FOR SUPERVISOR 2026
  candidate_name: JEREMY GRECO
  funds: 1100.0
  expenses: 617.68
ie_candidates:
- candidate_name: ALAN WONG
  filer_id: '1489126'
  committees:
  - position: SUPPORT
    filer_id: '1488188'
    committee_name: GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026
    expenses: 1000.0
- candidate_name: NATALIE GEE
  filer_id: '1490199'
  committees:
  - position: OPPOSE
    filer_id: '1488188'
    committee_name: GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026
    expenses: 500.0
---
`;

const APP_CANDIDATE_ROWS = [
  {
    candidate_id: "cand-wong",
    candidate_name: "Alan Wong",
    state_filing_ids: ["1489126"],
  },
  {
    candidate_id: "cand-chow",
    candidate_name: "Albert Chow",
    state_filing_ids: ["1492163", "1485609"],
  },
  {
    candidate_id: "cand-greco",
    candidate_name: "Jeremy Julian Greco",
    state_filing_ids: null,
  },
];

const INPUT_CANDIDATES = APP_CANDIDATE_ROWS.map((row) => ({
  candidateId: row.candidate_id,
  electionId: "elec-bos04",
  candidateName: row.candidate_name,
  electionDate: "2026-11-03",
  electionYear: 2026,
  contestCode: "bos04",
}));

// Routes by SQL shape; records calls for assertions. connect() hands back a
// client sharing the same query fn, so transactional statements are visible
// in the same call log.
function fakeDb(overrides?: {
  onSql?: (sql: string) => { rows: unknown[] } | undefined;
}) {
  const calls: Array<{ sql: string; params: unknown[] }> = [];
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, params: params ?? [] });
    const overridden = overrides?.onSql?.(sql);
    if (overridden) return overridden;
    if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK")
      return { rows: [] };
    if (sql.includes("FROM public.candidate_elections"))
      return { rows: APP_CANDIDATE_ROWS };
    if (sql.includes("link_source='manual'") && sql.startsWith("SELECT"))
      return { rows: [] };
    if (sql.includes("SET link_status='inactive'")) return { rows: [] };
    if (sql.includes("INSERT INTO public.sfc_candidate_finance_links"))
      return { rows: [{ id: `link-${params?.[0]}` }] };
    if (sql.includes("SET link_status='needs_review'")) return { rows: [] };
    if (
      sql.includes("public.sfc_candidate_finance_outside_committee_links")
    )
      return { rows: [] };
    throw new Error(`unexpected sql: ${sql}`);
  });
  const db = {
    query,
    connect: async () => ({ query, release: vi.fn() }) as never,
  };
  return { db, calls, query };
}

const manifestFetch = (async (url: RequestInfo | URL) => {
  expect(String(url)).toContain("elections/2026-11-03/contests/bos04.md");
  return new Response(BOS04_MANIFEST, { status: 200 });
}) as typeof fetch;

const filerRegistryFetch = (filerType: string) =>
  (async (url: RequestInfo | URL) => {
    const fppcId = /fppc_id%3D%27(\d+)%27|fppc_id='(\d+)'/.exec(
      decodeURIComponent(String(url)),
    );
    return new Response(
      JSON.stringify([
        {
          filer_nid: "n-1",
          fppc_id: fppcId?.[1] ?? fppcId?.[2] ?? "0000",
          filer_name: "COMMITTEE",
          filer_type: filerType,
          candidate_name: "X, Y",
          status: "ACTIVE",
          is_terminated: false,
        },
      ]),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;

describe("autoLinkMissingSanFranciscoCandidateFinanceLinks", () => {
  it("links by id and name, reports committee-less candidates, resolves outside targets", async () => {
    const { db, calls } = fakeDb();
    const { results, diagnostics } =
      await autoLinkMissingSanFranciscoCandidateFinanceLinks({
        db,
        now: new Date("2026-08-07T00:00:00Z"),
        candidates: INPUT_CANDIDATES,
        manifestClientOptions: { fetchImpl: manifestFetch, retryCount: 0 },
        openDataClientOptions: {
          fetchImpl: filerRegistryFetch("Candidate or Officeholder"),
          retryCount: 0,
        },
      });

    const byCandidate = new Map(results.map((r) => [r.candidateId, r]));
    expect(byCandidate.get("cand-wong")).toMatchObject({ status: "linked" });
    expect(byCandidate.get("cand-greco")).toMatchObject({ status: "linked" });
    expect(byCandidate.get("cand-chow")).toMatchObject({
      status: "no_committee",
    });

    // Gee's committee matched nobody on the ballot — diagnostic, not error.
    expect(diagnostics.unmatchedManifestCandidates).toEqual([
      expect.objectContaining({ candidateName: "NATALIE GEE" }),
    ]);
    // GrowSF's oppose money targets Gee, who is not a ballot candidate.
    expect(diagnostics.unresolvedOutsideTargets).toEqual([
      expect.objectContaining({
        candidateName: "NATALIE GEE",
        spenderName: "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
      }),
    ]);

    // Wong's link upsert carried the manifest committee identity.
    const linkInsert = calls.find(
      (call) =>
        call.sql.includes("INSERT INTO public.sfc_candidate_finance_links") &&
        call.params[0] === "cand-wong",
    );
    expect(linkInsert?.params).toContain("1489126");
    expect(linkInsert?.params).toContain("216198377");
    expect(linkInsert?.params).toContain("bos04");
    expect(linkInsert?.params).toContain("active");

    // Wong got his support relation; every ballot candidate's relation set
    // was replaced (3 deletes), so stale rows cannot survive a refresh.
    const relationInserts = calls.filter((call) =>
      call.sql.includes(
        "INSERT INTO public.sfc_candidate_finance_outside_committee_links",
      ),
    );
    expect(relationInserts).toHaveLength(1);
    expect(relationInserts[0]!.params).toEqual([
      "cand-wong",
      "elec-bos04",
      2026,
      "1488188",
      "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
      "support",
      expect.stringContaining("bos04"),
      "2026-08-07T00:00:00.000Z",
    ]);
    const relationDeletes = calls.filter((call) =>
      call.sql.includes(
        "DELETE FROM public.sfc_candidate_finance_outside_committee_links",
      ),
    );
    expect(relationDeletes).toHaveLength(3);

    // Disappearance flagging ran with the manifest's committee ids.
    const flagCall = calls.find((call) =>
      call.sql.includes("SET link_status='needs_review'"),
    );
    expect(flagCall?.params?.[1]).toEqual(["1489126", "1490199", "1491969"]);
  });

  it("writes needs_review links when the filer registry contradicts the manifest", async () => {
    const { db } = fakeDb();
    const { results } = await autoLinkMissingSanFranciscoCandidateFinanceLinks({
      db,
      now: new Date("2026-08-07T00:00:00Z"),
      candidates: INPUT_CANDIDATES,
      manifestClientOptions: { fetchImpl: manifestFetch, retryCount: 0 },
      openDataClientOptions: {
        fetchImpl: filerRegistryFetch("General Purpose"),
        retryCount: 0,
      },
    });
    const wong = results.find((r) => r.candidateId === "cand-wong");
    expect(wong).toMatchObject({ status: "needs_review" });
    expect(wong?.reason).toMatch(/General Purpose/);
  });

  it("never resurrects an operator-disabled manual link with the same filer id", async () => {
    // The disabled manual row would be the planned upsert's ON CONFLICT
    // target — the plan must be vetoed per-candidate, before the
    // transaction, so the rest of the election still commits.
    for (const linkStatus of ["inactive", "needs_review"]) {
      const { db, calls } = fakeDb({
        onSql: (sql) =>
          sql.startsWith("SELECT candidate_id::text")
            ? {
                rows: [
                  {
                    candidate_id: "cand-wong",
                    fppc_id: "1489126",
                    link_status: linkStatus,
                  },
                ],
              }
            : undefined,
      });
      const { results } =
        await autoLinkMissingSanFranciscoCandidateFinanceLinks({
          db,
          now: new Date("2026-08-07T00:00:00Z"),
          candidates: INPUT_CANDIDATES,
          manifestClientOptions: { fetchImpl: manifestFetch, retryCount: 0 },
          openDataClientOptions: {
            fetchImpl: filerRegistryFetch("Candidate or Officeholder"),
            retryCount: 0,
          },
        });
      const byCandidate = new Map(results.map((r) => [r.candidateId, r]));
      expect(byCandidate.get("cand-wong")).toMatchObject({
        status: "error",
        reason: expect.stringContaining("operator-disabled manual link"),
      });
      expect(byCandidate.get("cand-greco")).toMatchObject({
        status: "linked",
      });
      expect(
        calls.some(
          (call) =>
            call.sql.includes(
              "INSERT INTO public.sfc_candidate_finance_links",
            ) && call.params[0] === "cand-wong",
        ),
      ).toBe(false);
      expect(calls.map((call) => call.sql)).toContain("COMMIT");
    }
  });

  it("allows a new automatic identity past a disabled manual link with a different fppc id", async () => {
    // The operator disabled that association, not the candidate.
    const { db, calls } = fakeDb({
      onSql: (sql) =>
        sql.startsWith("SELECT candidate_id::text")
          ? {
              rows: [
                {
                  candidate_id: "cand-wong",
                  fppc_id: "7777777",
                  link_status: "inactive",
                },
              ],
            }
          : undefined,
    });
    const { results } = await autoLinkMissingSanFranciscoCandidateFinanceLinks({
      db,
      now: new Date("2026-08-07T00:00:00Z"),
      candidates: INPUT_CANDIDATES,
      manifestClientOptions: { fetchImpl: manifestFetch, retryCount: 0 },
      openDataClientOptions: {
        fetchImpl: filerRegistryFetch("Candidate or Officeholder"),
        retryCount: 0,
      },
    });
    expect(
      results.find((r) => r.candidateId === "cand-wong"),
    ).toMatchObject({ status: "linked" });
    expect(
      calls.some(
        (call) =>
          call.sql.includes("INSERT INTO public.sfc_candidate_finance_links") &&
          call.params[0] === "cand-wong",
      ),
    ).toBe(true);
  });

  it("reports every input candidate as errored when the manifest fetch fails", async () => {
    const { db } = fakeDb();
    const failingFetch = (async () =>
      new Response("not found", { status: 404 })) as typeof fetch;
    const { results } = await autoLinkMissingSanFranciscoCandidateFinanceLinks({
      db,
      now: new Date("2026-08-07T00:00:00Z"),
      candidates: INPUT_CANDIDATES,
      manifestClientOptions: { fetchImpl: failingFetch, retryCount: 0 },
      openDataClientOptions: { fetchImpl: filerRegistryFetch("x"), retryCount: 0 },
    });
    expect(results).toHaveLength(3);
    for (const result of results)
      expect(result).toMatchObject({ status: "error" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rolls the whole election back when a write inside the transaction fails", async () => {
    const { db, calls } = fakeDb({
      onSql: (sql) => {
        if (
          sql.includes(
            "DELETE FROM public.sfc_candidate_finance_outside_committee_links",
          )
        )
          throw new Error("disk full");
        return undefined;
      },
    });
    const { results, diagnostics } =
      await autoLinkMissingSanFranciscoCandidateFinanceLinks({
        db,
        now: new Date("2026-08-07T00:00:00Z"),
        candidates: INPUT_CANDIDATES,
        manifestClientOptions: { fetchImpl: manifestFetch, retryCount: 0 },
        openDataClientOptions: {
          fetchImpl: filerRegistryFetch("Candidate or Officeholder"),
          retryCount: 0,
        },
      });
    expect(calls.map((call) => call.sql)).toContain("ROLLBACK");
    expect(results).toHaveLength(3);
    for (const result of results)
      expect(result).toMatchObject({
        status: "error",
        reason: expect.stringContaining("rolled back"),
      });
    expect(diagnostics.electionErrors).toHaveLength(1);
    expect(diagnostics.flaggedLinkIds).toEqual([]);
  });
});

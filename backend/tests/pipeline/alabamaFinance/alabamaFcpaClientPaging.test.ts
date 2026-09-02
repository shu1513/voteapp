import { describe, expect, it, vi } from "vitest";

import { getAlabamaCommitteeFilings } from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";

function envelope(totalRecords: number, ids: number[]): Response {
  return new Response(
    JSON.stringify({
      success: true,
      data: { totalRecords, list: ids.map((ID) => ({ ID, DESCRIPTION: "Annual Report" })) },
    }),
    { status: 200, headers: { "content-type": "application/json" } }
  );
}

function pageNumberOf(url: string): number {
  return Number(new URL(url).searchParams.get("pageNumber"));
}

describe("Alabama FCPA list paging", () => {
  it("accepts a list the portal exhausts below its advertised count", async () => {
    // Committee 7460 live 2026-09-01: totalRecords 17, 16 rows, then an
    // empty page — the count is wrong, the rows are complete.
    const pages: Record<number, number[]> = { 1: Array.from({ length: 50 }, (_, i) => i + 1).slice(0, 16), 2: [] };
    const fetchImpl = vi.fn(async (url: string) => envelope(17, pages[pageNumberOf(url)] ?? []));
    const rows = await getAlabamaCommitteeFilings(7460, { fetchImpl: fetchImpl as never });
    expect(rows).toHaveLength(16);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("keeps paging past a short page and still rejects a count that moves", async () => {
    // A short non-empty page proves nothing (only an empty page proves
    // exhaustion), so paging continues; a changed totalRecords stays fatal.
    const fetchImpl = vi.fn(async (url: string) => {
      const page = pageNumberOf(url);
      if (page === 1) return envelope(60, Array.from({ length: 50 }, (_, i) => i + 1));
      if (page === 2) return envelope(60, [51, 52, 53, 54, 55, 56, 57, 58, 59]);
      return envelope(61, [60]);
    });
    await expect(getAlabamaCommitteeFilings(1, { fetchImpl: fetchImpl as never })).rejects.toThrow(
      "totalRecords changed mid-pagination"
    );
  });
});

import { describe, expect, it, vi } from "vitest";
import {
  ContentReportQueueError,
  listOpenContentReports,
  resolveContentReport,
  summarizeOpenContentReports,
  terminalStatusForResolution,
} from "../../../src/pipeline/reports/contentReportQueue.js";

const REPORT_ID = "11111111-1111-4111-8111-111111111111";

describe("terminalStatusForResolution", () => {
  it("mirrors the migration 155 constraint", () => {
    expect(terminalStatusForResolution("fixed")).toBe("resolved");
    expect(terminalStatusForResolution("no_change_needed")).toBe("resolved");
    expect(terminalStatusForResolution("duplicate")).toBe("resolved");
    expect(terminalStatusForResolution("unverifiable")).toBe("dismissed");
    expect(terminalStatusForResolution("spam")).toBe("dismissed");
  });
});

describe("listOpenContentReports", () => {
  it("defaults to both open statuses, oldest first, with a limit", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ id: REPORT_ID }] });
    const rows = await listOpenContentReports({ query });
    expect(rows).toEqual([{ id: REPORT_ID }]);
    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("ORDER BY created_at ASC");
    expect(params).toEqual([["new", "investigating"], 50]);
  });

  it("narrows to one status when asked", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await listOpenContentReports({ query }, { status: "investigating", limit: 5 });
    expect(query.mock.calls[0][1]).toEqual([["investigating"], 5]);
  });
});

describe("resolveContentReport", () => {
  it("closes an open row with the status the resolution maps to", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: REPORT_ID, status: "dismissed", resolution: "spam" }] });
    const result = await resolveContentReport({ query }, { id: REPORT_ID, resolution: "spam", summary: "  junk  text " });
    expect(result).toEqual({ id: REPORT_ID, status: "dismissed", resolution: "spam" });
    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("UPDATE public.content_reports");
    expect(sql).toContain("status = ANY($5::text[])");
    expect(params).toEqual([REPORT_ID, "dismissed", "spam", "junk text", ["new", "investigating"]]);
  });

  it("leaves an already-closed row untouched and says so", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ status: "resolved", resolution: "fixed" }] });
    await expect(
      resolveContentReport({ query }, { id: REPORT_ID, resolution: "duplicate", summary: "dup" })
    ).rejects.toMatchObject({ code: "report_not_open", message: expect.stringContaining("already resolved") });
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("reports a missing row separately", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] }).mockResolvedValueOnce({ rows: [] });
    await expect(
      resolveContentReport({ query }, { id: REPORT_ID, resolution: "fixed", summary: "done" })
    ).rejects.toMatchObject({ code: "report_not_open", message: expect.stringContaining("not found") });
  });

  it("rejects bad input before touching the database", async () => {
    const query = vi.fn();
    await expect(
      resolveContentReport({ query }, { id: "nope", resolution: "fixed", summary: "x" })
    ).rejects.toBeInstanceOf(ContentReportQueueError);
    await expect(
      resolveContentReport({ query }, { id: REPORT_ID, resolution: "fixed", summary: "   " })
    ).rejects.toMatchObject({ code: "summary_required" });
    await expect(
      resolveContentReport({ query }, { id: REPORT_ID, resolution: "closed" as never, summary: "x" })
    ).rejects.toMatchObject({ code: "invalid_resolution" });
    expect(query).not.toHaveBeenCalled();
  });
});

describe("summarizeOpenContentReports", () => {
  it("aggregates counts and ages without reading reporter text", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        { entity_type: "candidate_record", entity_id: "a", report_count: "2", oldest_age_days: 49 },
        { entity_type: "election", entity_id: "b", report_count: "1", oldest_age_days: 3 },
      ],
    });
    const summary = await summarizeOpenContentReports({ query });
    expect(summary).toEqual({
      open_count: 3,
      oldest_age_days: 49,
      by_entity: [
        { entity_type: "candidate_record", entity_id: "a", report_count: 2, oldest_age_days: 49 },
        { entity_type: "election", entity_id: "b", report_count: 1, oldest_age_days: 3 },
      ],
    });
    const [sql] = query.mock.calls[0];
    expect(sql).not.toContain("message");
    expect(sql).not.toContain("suggested_source_url");
  });

  it("is empty when nothing is open", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    expect(await summarizeOpenContentReports({ query })).toEqual({ open_count: 0, oldest_age_days: 0, by_entity: [] });
  });
});

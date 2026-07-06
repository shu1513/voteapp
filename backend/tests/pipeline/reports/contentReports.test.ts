import { describe, expect, it } from "vitest";

import { ContentReportError, createContentReport, getContentReportStats } from "../../../src/pipeline/reports/contentReports.js";

function makeDb(options: { labelRows?: Array<{ label: string | null }>; userRows?: Array<{ id: string }>; insertId?: string } = {}) {
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  return {
    calls,
    db: {
      query: async (sql: string, params?: unknown[]) => {
        calls.push({ sql, params });
        if (sql.includes("FROM public.users")) {
          return { rows: options.userRows ?? [] };
        }
        if (sql.includes("INSERT INTO public.content_reports")) {
          return { rows: [{ id: options.insertId ?? "99999999-9999-4999-8999-999999999999" }] };
        }
        if (sql.includes("GROUP BY status")) {
          return { rows: [{ status: "new", count: "2" }] };
        }
        if (sql.includes("GROUP BY entity_type")) {
          return {
            rows: [
              {
                entity_type: "candidate_record",
                entity_id: "22222222-2222-4222-8222-222222222222",
                open_report_count: "2",
              },
            ],
          };
        }
        return { rows: options.labelRows ?? [{ label: "Jane Candidate" }] };
      },
    },
  };
}

describe("createContentReport", () => {
  it("captures server-side entity label and active user id", async () => {
    const { db, calls } = makeDb({
      labelRows: [{ label: "  Jane   Candidate  " }],
      userRows: [{ id: "11111111-1111-4111-8111-111111111111" }],
    });

    await expect(
      createContentReport(db, {
        entityType: "candidate",
        entityId: "22222222-2222-4222-8222-222222222222",
        message: "Election date looks wrong",
        suggestedSourceUrl: "https://example.org/source",
        reporterEmail: "reader@example.com",
        userId: "11111111-1111-4111-8111-111111111111",
      })
    ).resolves.toEqual({ id: "99999999-9999-4999-8999-999999999999" });

    const insert = calls.find((call) => call.sql.includes("INSERT INTO public.content_reports"));
    expect(insert?.params).toEqual([
      "candidate",
      "22222222-2222-4222-8222-222222222222",
      "Jane Candidate",
      "Election date looks wrong",
      "https://example.org/source",
      "reader@example.com",
      "11111111-1111-4111-8111-111111111111",
    ]);
  });

  it("does not attach a stale optional user id", async () => {
    const { db, calls } = makeDb({ labelRows: [{ label: "Jane Candidate" }], userRows: [] });

    await createContentReport(db, {
      entityType: "candidate",
      entityId: "22222222-2222-4222-8222-222222222222",
      message: "Typo",
      userId: "11111111-1111-4111-8111-111111111111",
    });

    const insert = calls.find((call) => call.sql.includes("INSERT INTO public.content_reports"));
    expect(insert?.params?.[6]).toBeNull();
  });

  it("rejects unknown content ids", async () => {
    const { db } = makeDb({ labelRows: [] });

    await expect(
      createContentReport(db, {
        entityType: "candidate_record",
        entityId: "22222222-2222-4222-8222-222222222222",
        message: "This seems wrong",
      })
    ).rejects.toMatchObject(new ContentReportError("entity_not_found", "Reported content was not found"));
  });
});

describe("getContentReportStats", () => {
  it("returns numeric counts", async () => {
    const { db } = makeDb();

    await expect(getContentReportStats(db)).resolves.toEqual({
      by_status: [{ status: "new", count: 2 }],
      open_entities: [
        {
          entity_type: "candidate_record",
          entity_id: "22222222-2222-4222-8222-222222222222",
          open_report_count: 2,
        },
      ],
    });
  });
});

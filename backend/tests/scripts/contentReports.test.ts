import { describe, expect, it, vi } from "vitest";
import {
  describeDatabaseTarget,
  parseContentReportsArgs,
  runContentReportsCommand,
} from "../../src/scripts/contentReports.js";

const REPORT_ID = "11111111-1111-4111-8111-111111111111";

describe("parseContentReportsArgs", () => {
  it("parses list with optional filters", () => {
    expect(parseContentReportsArgs(["list"])).toEqual({ command: "list" });
    expect(parseContentReportsArgs(["list", "--status", "new", "--limit", "5"])).toEqual({
      command: "list",
      status: "new",
      limit: 5,
    });
    expect(() => parseContentReportsArgs(["list", "--status", "resolved"])).toThrow(/--status/);
    expect(() => parseContentReportsArgs(["list", "--limit", "0"])).toThrow(/--limit/);
  });

  it("requires id, resolution, and summary for resolve", () => {
    expect(
      parseContentReportsArgs(["resolve", "--id", REPORT_ID, "--resolution", "fixed", "--summary", "corrected the date"])
    ).toEqual({ command: "resolve", id: REPORT_ID, resolution: "fixed", summary: "corrected the date" });
    expect(() => parseContentReportsArgs(["resolve", "--id", REPORT_ID, "--resolution", "fixed"])).toThrow(/--summary/);
    expect(() => parseContentReportsArgs(["resolve", "--id", REPORT_ID, "--resolution", "closed", "--summary", "x"])).toThrow(
      /--resolution/
    );
  });

  it("parses summary and rejects unknown commands", () => {
    expect(parseContentReportsArgs(["summary"])).toEqual({ command: "summary" });
    expect(parseContentReportsArgs(["summary", "--to", "ops@example.com"])).toEqual({ command: "summary", to: "ops@example.com" });
    expect(() => parseContentReportsArgs(["summary", "--to"])).toThrow(/--to/);
    expect(() => parseContentReportsArgs(["claim"])).toThrow(/Unknown command/);
    expect(() => parseContentReportsArgs([])).toThrow(/Missing command/);
  });
});

describe("describeDatabaseTarget", () => {
  it("prints host and database, never credentials", () => {
    expect(describeDatabaseTarget("postgresql://user:secret@db.example.com:5432/voteapp")).toBe("db.example.com/voteapp");
    expect(describeDatabaseTarget("postgresql://localhost:5432/voteapp")).toBe("localhost/voteapp");
  });
});

describe("runContentReportsCommand summary", () => {
  it("sends nothing when no reports are open", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    const sendSummaryEmail = vi.fn();
    const print = vi.fn();
    await runContentReportsCommand({
      db,
      parsed: { command: "summary" },
      buildMailer: () => ({ sendSummaryEmail }),
      summaryRecipient: "contact@example.com",
      print,
    });
    expect(sendSummaryEmail).not.toHaveBeenCalled();
    expect(print).toHaveBeenCalledWith({ sent: false, reason: "no open reports" });
  });

  it("emails the configured recipient when reports are open", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [{ entity_type: "election", entity_id: "e1", report_count: "2", oldest_age_days: 10 }],
      }),
    };
    const sendSummaryEmail = vi.fn().mockResolvedValue(undefined);
    const print = vi.fn();
    await runContentReportsCommand({
      db,
      parsed: { command: "summary" },
      buildMailer: () => ({ sendSummaryEmail }),
      summaryRecipient: "contact@example.com",
      print,
    });
    expect(sendSummaryEmail).toHaveBeenCalledWith({
      toEmailAddress: "contact@example.com",
      summary: { open_count: 2, oldest_age_days: 10, by_entity: [{ entity_type: "election", entity_id: "e1", report_count: 2, oldest_age_days: 10 }] },
    });
    expect(print).toHaveBeenCalledWith({ sent: true, to: "contact@example.com", openCount: 2, entities: 1 });
  });

  it("fails clearly when no recipient is configured", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [{ entity_type: "election", entity_id: "e1", report_count: "1", oldest_age_days: 1 }],
      }),
    };
    await expect(
      runContentReportsCommand({ db, parsed: { command: "summary" }, buildMailer: () => ({ sendSummaryEmail: vi.fn() }), print: vi.fn() })
    ).rejects.toThrow(/recipient/);
  });
});

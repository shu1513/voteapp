import { SendEmailCommand } from "@aws-sdk/client-sesv2";
import { describe, expect, it, vi } from "vitest";
import {
  buildSummarySubject,
  buildSummaryTextBody,
  createConsoleContentReportSummaryMailer,
  createSesContentReportSummaryMailer,
} from "../../../src/pipeline/reports/contentReportSummaryMailer.js";
import type { OpenContentReportSummary } from "../../../src/pipeline/reports/contentReportQueue.js";

const SUMMARY: OpenContentReportSummary = {
  open_count: 3,
  oldest_age_days: 49,
  by_entity: [
    { entity_type: "candidate_record", entity_id: "22222222-2222-4222-8222-222222222222", report_count: 2, oldest_age_days: 49 },
    { entity_type: "election", entity_id: "33333333-3333-4333-8333-333333333333", report_count: 1, oldest_age_days: 3 },
  ],
};

describe("content report summary builders", () => {
  it("lists counts, ages, and entity ids only", () => {
    const body = buildSummaryTextBody(undefined, SUMMARY);
    expect(body).toContain("3 open content reports across 2 entities; the oldest is 49 days old.");
    expect(body).toContain("candidate_record 22222222-2222-4222-8222-222222222222");
    expect(body).toContain("npm run content-reports -- list");
    expect(buildSummarySubject("Test App", SUMMARY)).toBe("[Test App] 3 open content reports");
    expect(buildSummarySubject(undefined, { ...SUMMARY, open_count: 1 })).toMatch(/1 open content report$/);
  });
});

describe("createSesContentReportSummaryMailer", () => {
  it("sends a text-only SES email to the operator", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesContentReportSummaryMailer({
      sesClient: { send },
      fromEmailAddress: "noreply@example.com",
    });
    await mailer.sendSummaryEmail({ toEmailAddress: "contact@example.com", summary: SUMMARY });
    expect(send).toHaveBeenCalledTimes(1);
    const command = send.mock.calls[0][0] as SendEmailCommand;
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input.Destination?.ToAddresses).toEqual(["contact@example.com"]);
    expect(command.input.Content?.Simple?.Body?.Html).toBeUndefined();
    expect(command.input.Content?.Simple?.Body?.Text?.Data).toContain("3 open content reports");
  });

  it("refuses an empty recipient", async () => {
    const mailer = createSesContentReportSummaryMailer({ sesClient: { send: vi.fn() }, fromEmailAddress: "a@b.c" });
    await expect(mailer.sendSummaryEmail({ toEmailAddress: "  ", summary: SUMMARY })).rejects.toThrow(/recipient/);
  });
});

describe("createConsoleContentReportSummaryMailer", () => {
  it("prints instead of sending", async () => {
    const log = vi.fn();
    await createConsoleContentReportSummaryMailer({ log }).sendSummaryEmail({ toEmailAddress: "x@y.z", summary: SUMMARY });
    expect(log).toHaveBeenCalledTimes(1);
    expect(log.mock.calls[0][0]).toContain("to=x@y.z");
  });
});

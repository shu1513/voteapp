import { SendEmailCommand } from "@aws-sdk/client-sesv2";
import { describe, expect, it, vi } from "vitest";

import {
  buildAlertSubject,
  buildAlertTextBody,
  createConsoleNewElectionAlertMailer,
  createSesNewElectionAlertMailer,
  type NewElectionAlertEmailInput,
} from "../../../src/pipeline/users/newElectionAlertMailer.js";

const baseInput: NewElectionAlertEmailInput = {
  email: "voter@example.com",
  firstName: "Sam",
  totalEventCount: 3,
  items: [
    {
      districtName: "Los Angeles County",
      electionTitle: "County Assessor",
      electionDate: "2026-11-03",
    },
    {
      districtName: "Los Angeles County",
      electionTitle: "District Attorney",
      electionDate: "2026-11-03",
    },
    {
      districtName: "Texas Senate District 19",
      electionTitle: "State Senator, District 19",
      electionDate: "2026-11-03",
    },
  ],
};

describe("alert message builders", () => {
  it("builds a subject with singular/plural counts", () => {
    expect(buildAlertSubject(undefined, 1)).toBe("[Elections Simplified] 1 new election in your districts");
    expect(buildAlertSubject("MyApp", 3)).toBe("[MyApp] 3 new elections in your districts");
  });

  it("groups body lines by district and includes election dates", () => {
    const body = buildAlertTextBody(undefined, baseInput);
    expect(body).toContain("Hi Sam,");
    expect(body).toContain("Los Angeles County\n- County Assessor (2026-11-03)\n- District Attorney (2026-11-03)");
    expect(body).toContain("Texas Senate District 19\n- State Senator, District 19 (2026-11-03)");
    expect(body).not.toContain("more election");
  });

  it("notes the remainder when rendered items are capped below the total", () => {
    const body = buildAlertTextBody(undefined, { ...baseInput, totalEventCount: 10 });
    expect(body).toContain("…and 7 more elections.");
  });

  it("includes the unsubscribe link only when provided", () => {
    const withLink = buildAlertTextBody(undefined, {
      ...baseInput,
      unsubscribeUrl: "https://api.example.com/api/email/unsubscribe?token=t&pref=new_election_alerts",
    });
    expect(withLink).toContain("Unsubscribe from these alerts: https://api.example.com");
    expect(buildAlertTextBody(undefined, baseInput)).not.toContain("Unsubscribe");
  });
});

describe("createSesNewElectionAlertMailer", () => {
  it("sends a composed SES message with one-click unsubscribe headers", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesNewElectionAlertMailer({
      fromEmailAddress: "alerts@example.com",
      sesClient: { send },
    });

    await mailer.sendAlertEmail({
      ...baseInput,
      unsubscribeUrl: "https://api.example.com/api/email/unsubscribe?token=t&pref=new_election_alerts",
    });

    expect(send).toHaveBeenCalledTimes(1);
    const command = send.mock.calls[0][0];
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input.Destination?.ToAddresses).toEqual(["voter@example.com"]);
    expect(command.input.Content?.Simple?.Subject?.Data).toBe("[Elections Simplified] 3 new elections in your districts");
    expect(command.input.Content?.Simple?.Headers).toEqual([
      {
        Name: "List-Unsubscribe",
        Value: "<https://api.example.com/api/email/unsubscribe?token=t&pref=new_election_alerts>",
      },
      { Name: "List-Unsubscribe-Post", Value: "List-Unsubscribe=One-Click" },
    ]);
  });

  it("omits unsubscribe headers when no link is provided and rejects empty item lists", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesNewElectionAlertMailer({
      fromEmailAddress: "alerts@example.com",
      sesClient: { send },
    });

    await mailer.sendAlertEmail(baseInput);
    expect(send.mock.calls[0][0].input.Content?.Simple?.Headers).toBeUndefined();

    await expect(mailer.sendAlertEmail({ ...baseInput, items: [] })).rejects.toThrow(
      "Alert email requires at least one item"
    );
  });
});

describe("createConsoleNewElectionAlertMailer", () => {
  it("logs the composed alert instead of sending", async () => {
    const log = vi.fn();
    const mailer = createConsoleNewElectionAlertMailer({ log });

    await mailer.sendAlertEmail(baseInput);

    expect(log).toHaveBeenCalledTimes(1);
    const message = log.mock.calls[0][0] as string;
    expect(message).toContain("to=voter@example.com");
    expect(message).toContain("3 new elections in your districts");
    expect(message).toContain("Los Angeles County");
  });
});

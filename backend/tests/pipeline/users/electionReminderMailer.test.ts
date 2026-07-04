import { SendEmailCommand } from "@aws-sdk/client-sesv2";
import { describe, expect, it, vi } from "vitest";

import {
  buildReminderSubject,
  buildReminderTextBody,
  createConsoleElectionReminderMailer,
  createSesElectionReminderMailer,
  formatElectionDateLabel,
  type ElectionReminderEmailInput,
} from "../../../src/pipeline/users/electionReminderMailer.js";

const baseInput: ElectionReminderEmailInput = {
  email: "voter@example.com",
  firstName: "Sam",
  electionDateLabel: "Tuesday, November 3, 2026",
  totalElectionCount: 3,
  items: [
    {
      districtName: "Los Angeles County",
      electionTitle: "County Assessor",
    },
    {
      districtName: "Los Angeles County",
      electionTitle: "District Attorney",
    },
    {
      districtName: "Texas Senate District 19",
      electionTitle: "State Senator, District 19",
    },
  ],
};

describe("formatElectionDateLabel", () => {
  it("formats an ISO date as a full US date without timezone drift", () => {
    expect(formatElectionDateLabel("2026-11-03")).toBe("Tuesday, November 3, 2026");
    expect(formatElectionDateLabel("2026-01-01")).toBe("Thursday, January 1, 2026");
  });

  it("passes non-ISO input through untouched", () => {
    expect(formatElectionDateLabel("soon")).toBe("soon");
  });
});

describe("reminder message builders", () => {
  it("builds a subject with singular/plural counts", () => {
    expect(buildReminderSubject(undefined, 1)).toBe("[VoteApp] Reminder: 1 election tomorrow");
    expect(buildReminderSubject("MyApp", 3)).toBe("[MyApp] Reminder: 3 elections tomorrow");
  });

  it("names the date, groups lines by district, and matches verb to count", () => {
    const body = buildReminderTextBody(undefined, baseInput);
    expect(body).toContain("Hi Sam,");
    expect(body).toContain("Tomorrow (Tuesday, November 3, 2026) there are 3 elections in your districts on VoteApp:");
    expect(body).toContain("Los Angeles County\n- County Assessor\n- District Attorney");
    expect(body).toContain("Texas Senate District 19\n- State Senator, District 19");
    expect(body).not.toContain("more election");

    const singular = buildReminderTextBody(undefined, {
      ...baseInput,
      totalElectionCount: 1,
      items: baseInput.items.slice(0, 1),
    });
    expect(singular).toContain("there is 1 election in your districts");
  });

  it("notes the remainder when rendered items are capped below the total", () => {
    const body = buildReminderTextBody(undefined, { ...baseInput, totalElectionCount: 10 });
    expect(body).toContain("…and 7 more elections.");
  });

  it("includes the unsubscribe link only when provided", () => {
    const withLink = buildReminderTextBody(undefined, {
      ...baseInput,
      unsubscribeUrl: "https://api.example.com/api/email/unsubscribe?token=t&pref=election_reminders",
    });
    expect(withLink).toContain("Unsubscribe from these reminders: https://api.example.com");
    expect(buildReminderTextBody(undefined, baseInput)).not.toContain("Unsubscribe");
  });
});

describe("createSesElectionReminderMailer", () => {
  it("sends a composed SES message with one-click unsubscribe headers", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesElectionReminderMailer({
      fromEmailAddress: "reminders@example.com",
      sesClient: { send },
    });

    await mailer.sendReminderEmail({
      ...baseInput,
      unsubscribeUrl: "https://api.example.com/api/email/unsubscribe?token=t&pref=election_reminders",
    });

    expect(send).toHaveBeenCalledTimes(1);
    const command = send.mock.calls[0][0];
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input.Destination?.ToAddresses).toEqual(["voter@example.com"]);
    expect(command.input.FromEmailAddress).toBe("reminders@example.com");
    const headers = command.input.Content?.Simple?.Headers;
    expect(headers).toEqual([
      {
        Name: "List-Unsubscribe",
        Value: "<https://api.example.com/api/email/unsubscribe?token=t&pref=election_reminders>",
      },
      { Name: "List-Unsubscribe-Post", Value: "List-Unsubscribe=One-Click" },
    ]);
    expect(command.input.Content?.Simple?.Subject?.Data).toBe("[VoteApp] Reminder: 3 elections tomorrow");
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain("<li>County Assessor</li>");
  });

  it("omits unsubscribe headers when no URL is provided", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesElectionReminderMailer({
      fromEmailAddress: "reminders@example.com",
      sesClient: { send },
    });

    await mailer.sendReminderEmail(baseInput);

    expect(send.mock.calls[0][0].input.Content?.Simple?.Headers).toBeUndefined();
  });

  it("escapes HTML-special characters in user-controlled fields", async () => {
    const send = vi.fn().mockResolvedValue({});
    const mailer = createSesElectionReminderMailer({
      fromEmailAddress: "reminders@example.com",
      sesClient: { send },
    });

    await mailer.sendReminderEmail({
      ...baseInput,
      firstName: "<script>alert(1)</script>",
      items: [
        {
          districtName: "Tom & Jerry's \"District\" <1>",
          electionTitle: "Sheriff <img src=x onerror=alert(1)>",
        },
      ],
      totalElectionCount: 1,
    });

    const html = send.mock.calls[0][0].input.Content?.Simple?.Body?.Html?.Data as string;
    expect(html).not.toContain("<script>");
    expect(html).not.toContain("<img");
    expect(html).toContain("&lt;script&gt;alert(1)&lt;/script&gt;");
    expect(html).toContain("Tom &amp; Jerry&#39;s &quot;District&quot; &lt;1&gt;");
    expect(html).toContain("Sheriff &lt;img src=x onerror=alert(1)&gt;");
  });

  it("rejects an empty item list", async () => {
    const send = vi.fn();
    const mailer = createSesElectionReminderMailer({
      fromEmailAddress: "reminders@example.com",
      sesClient: { send },
    });

    await expect(mailer.sendReminderEmail({ ...baseInput, items: [] })).rejects.toThrow(
      "at least one item"
    );
    expect(send).not.toHaveBeenCalled();
  });
});

describe("createConsoleElectionReminderMailer", () => {
  it("logs the recipient, subject, and text body instead of sending", async () => {
    const lines: string[] = [];
    const mailer = createConsoleElectionReminderMailer({ log: (message) => lines.push(message) });

    await mailer.sendReminderEmail(baseInput);

    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("to=voter@example.com");
    expect(lines[0]).toContain("subject=[VoteApp] Reminder: 3 elections tomorrow");
    expect(lines[0]).toContain("County Assessor");
  });
});

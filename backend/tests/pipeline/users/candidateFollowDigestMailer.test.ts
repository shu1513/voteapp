import { SendEmailCommand } from "@aws-sdk/client-sesv2";
import { describe, expect, it, vi } from "vitest";

import {
  buildDigestSubject,
  buildDigestTextBody,
  createConsoleCandidateFollowDigestMailer,
  createSesCandidateFollowDigestMailer,
  type CandidateFollowDigestEmailInput,
} from "../../../src/pipeline/users/candidateFollowDigestMailer.js";

const baseInput: CandidateFollowDigestEmailInput = {
  email: "voter@example.com",
  firstName: "Sam",
  totalEventCount: 3,
  items: [
    {
      candidateDisplayName: "Roland Gutierrez",
      eventType: "candidate_record_update",
      recordDescription: "Voted against Senate Bill 2, the 2025 school voucher law.",
    },
    {
      candidateDisplayName: "Roland Gutierrez",
      eventType: "candidate_future_election",
      electionTitle: "State Senator, District 19",
      electionDate: "2026-11-03",
    },
    {
      candidateDisplayName: "Marcus Cardenas",
      eventType: "candidate_record_update",
      recordDescription: "Received Governor Greg Abbott's endorsement.",
    },
  ],
};

describe("digest message builders", () => {
  it("builds a subject with singular/plural counts", () => {
    expect(buildDigestSubject(undefined, 1)).toBe("[VoteApp] 1 update on candidates you follow");
    expect(buildDigestSubject("MyApp", 3)).toBe("[MyApp] 3 updates on candidates you follow");
  });

  it("groups body lines by candidate and includes election dates", () => {
    const body = buildDigestTextBody(undefined, baseInput);
    expect(body).toContain("Hi Sam,");
    expect(body).toContain("Roland Gutierrez\n- New record: Voted against Senate Bill 2");
    expect(body).toContain("- On the ballot: State Senator, District 19 (2026-11-03)");
    expect(body).toContain("Marcus Cardenas\n- New record: Received Governor Greg Abbott's endorsement.");
    expect(body).not.toContain("more update");
  });

  it("notes the remainder when rendered items are capped below the total", () => {
    const body = buildDigestTextBody(undefined, { ...baseInput, totalEventCount: 10 });
    expect(body).toContain("…and 7 more updates.");
  });

  it("truncates long record descriptions", () => {
    const body = buildDigestTextBody(undefined, {
      ...baseInput,
      items: [
        {
          candidateDisplayName: "X",
          eventType: "candidate_record_update",
          recordDescription: "a".repeat(500),
        },
      ],
      totalEventCount: 1,
    });
    expect(body).toContain("…");
    expect(body).not.toContain("a".repeat(300));
  });
});

describe("createSesCandidateFollowDigestMailer", () => {
  it("sends a composed digest through SES", async () => {
    const sesClient = { send: vi.fn().mockResolvedValue({}) };
    const mailer = createSesCandidateFollowDigestMailer({
      appName: "VoteApp",
      fromEmailAddress: "noreply@example.com",
      replyToEmailAddress: "support@example.com",
      sesClient,
    });

    await mailer.sendDigestEmail(baseInput);

    expect(sesClient.send).toHaveBeenCalledTimes(1);
    const command = sesClient.send.mock.calls[0][0];
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input).toMatchObject({
      FromEmailAddress: "noreply@example.com",
      Destination: { ToAddresses: ["voter@example.com"] },
      ReplyToAddresses: ["support@example.com"],
    });
    expect(command.input.Content?.Simple?.Subject?.Data).toBe(
      "[VoteApp] 3 updates on candidates you follow"
    );
    expect(command.input.Content?.Simple?.Body?.Text?.Data).toContain("Roland Gutierrez");
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain("<h3>Roland Gutierrez</h3>");
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain('<meta charset="UTF-8">');
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain(
      "Received Governor Greg Abbott&#39;s endorsement."
    );
  });

  it("rejects an empty digest instead of sending a blank email", async () => {
    const sesClient = { send: vi.fn() };
    const mailer = createSesCandidateFollowDigestMailer({
      fromEmailAddress: "noreply@example.com",
      sesClient,
    });

    await expect(
      mailer.sendDigestEmail({ ...baseInput, items: [], totalEventCount: 0 })
    ).rejects.toThrow("Digest email requires at least one item");
    expect(sesClient.send).not.toHaveBeenCalled();
  });
});

describe("createConsoleCandidateFollowDigestMailer", () => {
  it("logs the digest without any SES dependency", async () => {
    const log = vi.fn();
    const mailer = createConsoleCandidateFollowDigestMailer({ log });

    await mailer.sendDigestEmail(baseInput);

    expect(log).toHaveBeenCalledTimes(1);
    const message = log.mock.calls[0][0] as string;
    expect(message).toContain("to=voter@example.com");
    expect(message).toContain("[VoteApp] 3 updates on candidates you follow");
    expect(message).toContain("Marcus Cardenas");
  });
});

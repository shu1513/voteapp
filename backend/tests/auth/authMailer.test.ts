import { SendEmailCommand } from "@aws-sdk/client-sesv2";
import { describe, expect, it, vi } from "vitest";

import { createConsoleAuthMailer, createSesAuthMailer } from "../../src/auth/authMailer.js";

const fromEmailAddress = "noreply@example.com";
const replyToEmailAddress = "support@example.com";
const recipientEmail = "user@example.com";
const verificationLinkUrl = "https://example.com/auth/verify?token=abc";
const resetLinkUrl = "https://example.com/auth/reset?token=def";

function createSesClientMock() {
  return {
    send: vi.fn().mockResolvedValue({}),
  };
}

describe("authMailer", () => {
  it("sends verification emails through SES with a composed message", async () => {
    const sesClient = createSesClientMock();
    const mailer = createSesAuthMailer({
      appName: "VoteApp",
      fromEmailAddress,
      replyToEmailAddress,
      sesClient,
    });

    await mailer.sendVerificationEmail({
      email: recipientEmail,
      linkUrl: verificationLinkUrl,
    });

    expect(sesClient.send).toHaveBeenCalledTimes(1);
    const command = sesClient.send.mock.calls[0][0];
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input).toMatchObject({
      FromEmailAddress: fromEmailAddress,
      Destination: {
        ToAddresses: [recipientEmail],
      },
      ReplyToAddresses: [replyToEmailAddress],
    });
    expect(command.input.Content?.Simple?.Subject?.Data).toBe("[VoteApp] Verify your email");
    expect(command.input.Content?.Simple?.Body?.Text?.Data).toContain(verificationLinkUrl);
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain(verificationLinkUrl);
  });

  it("sends password reset emails through SES with a composed message", async () => {
    const sesClient = createSesClientMock();
    const mailer = createSesAuthMailer({
      fromEmailAddress,
      sesClient,
    });

    await mailer.sendPasswordResetEmail({
      email: recipientEmail,
      linkUrl: resetLinkUrl,
    });

    expect(sesClient.send).toHaveBeenCalledTimes(1);
    const command = sesClient.send.mock.calls[0][0];
    expect(command).toBeInstanceOf(SendEmailCommand);
    expect(command.input).toMatchObject({
      FromEmailAddress: fromEmailAddress,
      Destination: {
        ToAddresses: [recipientEmail],
      },
    });
    expect(command.input.Content?.Simple?.Subject?.Data).toBe("[VoteApp] Reset your password");
    expect(command.input.Content?.Simple?.Body?.Text?.Data).toContain(resetLinkUrl);
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain(resetLinkUrl);
  });

  it("sends email change confirmations through SES with a composed message", async () => {
    const sesClient = createSesClientMock();
    const mailer = createSesAuthMailer({
      fromEmailAddress,
      sesClient,
    });
    const changeLinkUrl = "https://example.com/verify-email-change?token=tok";

    await mailer.sendEmailChangeEmail({
      email: recipientEmail,
      linkUrl: changeLinkUrl,
    });

    expect(sesClient.send).toHaveBeenCalledTimes(1);
    const command = sesClient.send.mock.calls[0][0];
    expect(command.input.Content?.Simple?.Subject?.Data).toBe("[VoteApp] Confirm your new email address");
    expect(command.input.Content?.Simple?.Body?.Text?.Data).toContain(changeLinkUrl);
    expect(command.input.Content?.Simple?.Body?.Html?.Data).toContain(changeLinkUrl);
  });

  it("rejects malformed link URLs before sending", async () => {
    const sesClient = createSesClientMock();
    const mailer = createSesAuthMailer({
      fromEmailAddress,
      sesClient,
    });

    await expect(
      mailer.sendVerificationEmail({
        email: recipientEmail,
        linkUrl: "not-a-url",
      })
    ).rejects.toThrow("Invalid URL");
    expect(sesClient.send).not.toHaveBeenCalled();
  });

  it("console mailer logs links instead of sending and validates inputs", async () => {
    const lines: string[] = [];
    const mailer = createConsoleAuthMailer({ log: (message) => lines.push(message) });

    await mailer.sendVerificationEmail({
      email: recipientEmail,
      linkUrl: "https://app.example.org/verify-email?token=abc",
    });
    await mailer.sendPasswordResetEmail({
      email: recipientEmail,
      linkUrl: "https://app.example.org/reset-password?token=def",
    });

    expect(lines).toHaveLength(2);
    expect(lines[0]).toContain("verification email");
    expect(lines[0]).toContain("https://app.example.org/verify-email?token=abc");
    expect(lines[1]).toContain("password reset email");
    expect(lines[1]).toContain("https://app.example.org/reset-password?token=def");

    await expect(
      mailer.sendVerificationEmail({ email: recipientEmail, linkUrl: "not-a-url" })
    ).rejects.toThrow("Invalid URL");
  });
});

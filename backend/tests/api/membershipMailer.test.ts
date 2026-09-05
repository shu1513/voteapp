import { describe, expect, it, vi } from "vitest";

import {
  createConsoleMembershipChangedSender,
  createSesMembershipChangedSender,
} from "../../src/api/membership/membershipMailer.js";

// Cancel / resume confirmations (docs/plans/membership-manage-page.md).

function createSesCapture() {
  const send = vi.fn(async () => ({}));
  return {
    send,
    options: {
      sesClient: { send },
      fromEmailAddress: "no-reply@site.test",
      replyToEmailAddress: "contact@site.test",
      manageMembershipUrl: "https://site.test/me/settings",
      // An ampersand, so the HTML escaping is exercised.
      termsUrl: "https://site.test/terms?v=1&x=2",
    },
  };
}

function sentContent(send: ReturnType<typeof vi.fn>) {
  const command = send.mock.calls[0]?.[0] as { input: Record<string, unknown> } | undefined;
  const input = command?.input as {
    FromEmailAddress: string;
    ReplyToAddresses?: string[];
    Destination: { ToAddresses: string[] };
    Content: { Simple: { Subject: { Data: string }; Body: { Text: { Data: string }; Html: { Data: string } } } };
  };
  return {
    from: input.FromEmailAddress,
    replyTo: input.ReplyToAddresses,
    to: input.Destination.ToAddresses,
    subject: input.Content.Simple.Subject.Data,
    text: input.Content.Simple.Body.Text.Data,
    html: input.Content.Simple.Body.Html.Data,
  };
}

// 2026-10-04 21:46:36 UTC is still October 4 in America/Los_Angeles.
const ENDS_AT = new Date("2026-10-04T21:46:36Z");

describe("membership changed email (SES)", () => {
  it("cancel: names the end date, promises no further renewal, links the manage page", async () => {
    const { send, options } = createSesCapture();
    await createSesMembershipChangedSender(options)({ kind: "canceled", email: "user@example.com", endsAt: ENDS_AT });

    const sent = sentContent(send);
    expect(sent.to).toEqual(["user@example.com"]);
    expect(sent.from).toBe("no-reply@site.test");
    expect(sent.replyTo).toEqual(["contact@site.test"]);
    expect(sent.subject).toBe("[Elections Simplified] Your membership will end on October 4, 2026");
    expect(sent.text).toContain("will not renew after October 4, 2026");
    expect(sent.text).toContain("https://site.test/me/settings");
    expect(sent.html).toContain("<strong>October 4, 2026</strong>");
    expect(sent.html).toContain('href="https://site.test/me/settings"');
  });

  it("cancel without a known end date still reads correctly", async () => {
    const { send, options } = createSesCapture();
    await createSesMembershipChangedSender(options)({ kind: "canceled", email: "user@example.com", endsAt: null });

    const sent = sentContent(send);
    expect(sent.subject).toBe("[Elections Simplified] Your membership will not renew");
    expect(sent.text).toContain("Your monthly membership will not renew. ");
  });

  it("resume: states the amount, next charge date, and cancel path; escapes the terms link", async () => {
    const { send, options } = createSesCapture();
    await createSesMembershipChangedSender(options)({
      kind: "resumed",
      email: "user@example.com",
      monthlyAmountCents: 1000,
      renewsAt: ENDS_AT,
    });

    const sent = sentContent(send);
    expect(sent.subject).toBe("[Elections Simplified] Your membership continues");
    expect(sent.text).toContain("$10.00 will be charged to your payment method on October 4, 2026 and each month until you cancel");
    expect(sent.text).toContain("Terms of Use: https://site.test/terms?v=1&x=2");
    expect(sent.html).toContain("<strong>$10.00</strong>");
    expect(sent.html).toContain('href="https://site.test/terms?v=1&amp;x=2"');
    expect(sent.html).not.toContain("&x=2\"");
  });
});

describe("membership changed email (console)", () => {
  it("logs the kind, recipient, and subject instead of sending", async () => {
    const log = vi.fn();
    await createConsoleMembershipChangedSender({
      manageMembershipUrl: "https://site.test/me/settings",
      termsUrl: "https://site.test/terms",
      log,
    })({ kind: "resumed", email: "user@example.com", monthlyAmountCents: 500, renewsAt: null });

    expect(log).toHaveBeenCalledWith(
      "[membership-mailer:console] resumed confirmation for user@example.com: [Elections Simplified] Your membership continues"
    );
  });
});

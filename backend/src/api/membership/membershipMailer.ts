import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";
import { APP_NAME } from "../../constants/brand.js";
import type { MembershipChangedEmailInput, MembershipStartedEmailInput } from "./membershipService.js";

// The §17602 post-purchase acknowledgment (docs/plans/membership-contributions.md):
// a retainable notice, sent when a membership starts, that states what renews,
// for how much, and how to cancel. A Stripe receipt alone doesn't carry the
// cancellation policy, which is why this exists.

export type SendMembershipStartedEmail = (input: MembershipStartedEmailInput) => Promise<void>;

export type SesMembershipMailerOptions = {
  sesClient: Pick<SESv2Client, "send">;
  fromEmailAddress: string;
  replyToEmailAddress?: string;
  /** Absolute URL of the settings page holding "Manage membership". */
  manageMembershipUrl: string;
  /** Absolute URL of the Terms of Use. */
  termsUrl: string;
};

function formatUsd(amountCents: number): string {
  return `$${(amountCents / 100).toFixed(2)}`;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function buildTextBody(input: MembershipStartedEmailInput, manageMembershipUrl: string, termsUrl: string): string {
  const amount = formatUsd(input.monthlyAmountCents);
  return (
    `Thank you for supporting ${APP_NAME}.\n\n` +
    `Your monthly membership is active: ${amount} will be charged to your payment method each month until you cancel. ` +
    `Your support funds the operation of the service; it is not a contribution to any candidate, campaign, committee, party, or charity, and it is not tax-deductible.\n\n` +
    `Cancel anytime: open account settings and choose Manage membership.\n${manageMembershipUrl}\n\n` +
    `Terms of Use: ${termsUrl}\n\n` +
    `Questions? Just reply to this email.`
  );
}

function buildHtmlBody(input: MembershipStartedEmailInput, manageMembershipUrl: string, termsUrl: string): string {
  const amount = escapeHtml(formatUsd(input.monthlyAmountCents));
  const manageUrl = escapeHtml(manageMembershipUrl);
  const terms = escapeHtml(termsUrl);
  return `<!doctype html>
<html lang="en">
  <body>
    <p>Thank you for supporting ${escapeHtml(APP_NAME)}.</p>
    <p>Your monthly membership is active: <strong>${amount}</strong> will be charged to your payment method each month until you cancel. Your support funds the operation of the service; it is not a contribution to any candidate, campaign, committee, party, or charity, and it is not tax-deductible.</p>
    <p>Cancel anytime: open <a href="${manageUrl}">account settings</a> and choose Manage membership.</p>
    <p><a href="${terms}">Terms of Use</a></p>
    <p>Questions? Just reply to this email.</p>
  </body>
</html>`;
}

export function createSesMembershipStartedSender(options: SesMembershipMailerOptions): SendMembershipStartedEmail {
  return async (input) => {
    await options.sesClient.send(
      new SendEmailCommand({
        FromEmailAddress: options.fromEmailAddress,
        Destination: { ToAddresses: [input.email] },
        ReplyToAddresses: options.replyToEmailAddress ? [options.replyToEmailAddress] : undefined,
        Content: {
          Simple: {
            Subject: {
              Data: `[${APP_NAME}] Your monthly membership is active`,
              Charset: "UTF-8",
            },
            Body: {
              Text: {
                Data: buildTextBody(input, options.manageMembershipUrl, options.termsUrl),
                Charset: "UTF-8",
              },
              Html: {
                Data: buildHtmlBody(input, options.manageMembershipUrl, options.termsUrl),
                Charset: "UTF-8",
              },
            },
          },
        },
      })
    );
  };
}

export type ConsoleMembershipMailerOptions = {
  manageMembershipUrl: string;
  termsUrl: string;
  log?: (message: string) => void;
};

/** Local-development sender: prints instead of emailing (AUTH_MAILER=console). */
export function createConsoleMembershipStartedSender(
  options: ConsoleMembershipMailerOptions
): SendMembershipStartedEmail {
  const log = options.log ?? ((message: string) => console.log(message));
  return async (input) => {
    log(
      `[membership-mailer:console] membership-started acknowledgment for ${input.email}: ` +
        `${formatUsd(input.monthlyAmountCents)}/month, manage at ${options.manageMembershipUrl}`
    );
  };
}

// Membership changes (docs/plans/membership-manage-page.md): cancel / resume
// are courtesy confirmations for changes the member made in an authenticated
// session; the amount notice is the CA BPC §17602(g)(2) advance notice of a
// fee change, sent 7–30 days before the first charge at the new amount, so
// like the start acknowledgment it states the amount, the date, and how to
// cancel.

export type SendMembershipChangedEmail = (input: MembershipChangedEmailInput) => Promise<void>;

// Renewal instants are shown as a calendar date in the operator's time zone
// (the Stripe dashboard's default for this account); the member's own zone is
// unknowable server-side.
function formatDate(value: Date): string {
  return value.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "America/Los_Angeles",
  });
}

function changedSubject(input: MembershipChangedEmailInput): string {
  if (input.kind === "canceled") {
    return input.endsAt
      ? `[${APP_NAME}] Your membership will end on ${formatDate(input.endsAt)}`
      : `[${APP_NAME}] Your membership will not renew`;
  }
  if (input.kind === "amount_notice") {
    return `[${APP_NAME}] Your membership amount changes on ${formatDate(input.startsAt)}`;
  }
  return `[${APP_NAME}] Your membership continues`;
}

function changedTextBody(input: MembershipChangedEmailInput, manageMembershipUrl: string, termsUrl: string): string {
  if (input.kind === "amount_notice") {
    const amount = formatUsd(input.newAmountCents);
    return (
      `You asked to change your monthly membership amount. Starting on ${formatDate(input.startsAt)}, ${amount} will be charged to your payment method each month until you cancel. ` +
      `Nothing is charged today. ` +
      `Your support funds the operation of the service; it is not a contribution to any candidate, campaign, committee, party, or charity, and it is not tax-deductible.\n\n` +
      `Cancel anytime: open account settings and choose Manage membership.\n${manageMembershipUrl}\n\n` +
      `Terms of Use: ${termsUrl}\n\n` +
      `Questions? Just reply to this email.`
    );
  }
  if (input.kind === "canceled") {
    const when = input.endsAt ? ` after ${formatDate(input.endsAt)}` : "";
    return (
      `Your monthly membership will not renew${when}. You will not be charged for another month after that.\n\n` +
      `Changed your mind? Open account settings and choose Manage membership to keep your membership.\n${manageMembershipUrl}\n\n` +
      `Thank you for having supported ${APP_NAME}.\n\n` +
      `Questions? Just reply to this email.`
    );
  }
  const amount = formatUsd(input.monthlyAmountCents);
  const next = input.renewsAt ? ` on ${formatDate(input.renewsAt)} and` : "";
  return (
    `Welcome back. Your monthly membership continues: ${amount} will be charged to your payment method${next} each month until you cancel.\n\n` +
    `Cancel anytime: open account settings and choose Manage membership.\n${manageMembershipUrl}\n\n` +
    `Terms of Use: ${termsUrl}\n\n` +
    `Questions? Just reply to this email.`
  );
}

function changedHtmlBody(input: MembershipChangedEmailInput, manageMembershipUrl: string, termsUrl: string): string {
  const manageUrl = escapeHtml(manageMembershipUrl);
  const body =
    input.kind === "amount_notice"
      ? `<p>You asked to change your monthly membership amount. Starting on <strong>${escapeHtml(formatDate(input.startsAt))}</strong>, <strong>${escapeHtml(formatUsd(input.newAmountCents))}</strong> will be charged to your payment method each month until you cancel. Nothing is charged today. Your support funds the operation of the service; it is not a contribution to any candidate, campaign, committee, party, or charity, and it is not tax-deductible.</p>
    <p>Cancel anytime: open <a href="${manageUrl}">account settings</a> and choose Manage membership.</p>
    <p><a href="${escapeHtml(termsUrl)}">Terms of Use</a></p>`
      : input.kind === "canceled"
        ? `<p>Your monthly membership will not renew${input.endsAt ? ` after <strong>${escapeHtml(formatDate(input.endsAt))}</strong>` : ""}. You will not be charged for another month after that.</p>
    <p>Changed your mind? Open <a href="${manageUrl}">account settings</a> and choose Manage membership to keep your membership.</p>
    <p>Thank you for having supported ${escapeHtml(APP_NAME)}.</p>`
        : `<p>Welcome back. Your monthly membership continues: <strong>${escapeHtml(formatUsd(input.monthlyAmountCents))}</strong> will be charged to your payment method${input.renewsAt ? ` on ${escapeHtml(formatDate(input.renewsAt))} and` : ""} each month until you cancel.</p>
    <p>Cancel anytime: open <a href="${manageUrl}">account settings</a> and choose Manage membership.</p>
    <p><a href="${escapeHtml(termsUrl)}">Terms of Use</a></p>`;
  return `<!doctype html>
<html lang="en">
  <body>
    ${body}
    <p>Questions? Just reply to this email.</p>
  </body>
</html>`;
}

export function createSesMembershipChangedSender(options: SesMembershipMailerOptions): SendMembershipChangedEmail {
  return async (input) => {
    await options.sesClient.send(
      new SendEmailCommand({
        FromEmailAddress: options.fromEmailAddress,
        Destination: { ToAddresses: [input.email] },
        ReplyToAddresses: options.replyToEmailAddress ? [options.replyToEmailAddress] : undefined,
        Content: {
          Simple: {
            Subject: { Data: changedSubject(input), Charset: "UTF-8" },
            Body: {
              Text: { Data: changedTextBody(input, options.manageMembershipUrl, options.termsUrl), Charset: "UTF-8" },
              Html: { Data: changedHtmlBody(input, options.manageMembershipUrl, options.termsUrl), Charset: "UTF-8" },
            },
          },
        },
      })
    );
  };
}

export function createConsoleMembershipChangedSender(
  options: ConsoleMembershipMailerOptions
): SendMembershipChangedEmail {
  const log = options.log ?? ((message: string) => console.log(message));
  return async (input) => {
    log(`[membership-mailer:console] ${input.kind} email for ${input.email}: ${changedSubject(input)}`);
  };
}

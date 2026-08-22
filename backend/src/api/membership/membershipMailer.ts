import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";
import { APP_NAME } from "../../constants/brand.js";
import type { MembershipStartedEmailInput } from "./membershipService.js";

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

import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";

export type IssueBroadcastEmailInput = {
  email: string;
  firstName: string;
  subject: string;
  /** Operator-written plain-text body; rendered verbatim in the text part. */
  body: string;
  /** Names of the saved areas that made this user a recipient. */
  matchedAreaNames: readonly string[];
  /**
   * Signed per-user unsubscribe link scoped to issue updates. When present it
   * is rendered in the footer and emitted as List-Unsubscribe /
   * List-Unsubscribe-Post headers (RFC 8058 one-click).
   */
  unsubscribeUrl?: string;
};

export type IssueBroadcastMailer = {
  sendBroadcastEmail(input: IssueBroadcastEmailInput): Promise<void>;
};

export type SesIssueBroadcastMailerOptions = {
  appName?: string;
  fromEmailAddress: string;
  replyToEmailAddress?: string;
  sesClient: Pick<SESv2Client, "send">;
};

function resolveBrandName(appName: string | undefined): string {
  const normalized = appName?.trim();
  return normalized && normalized.length > 0 ? normalized : "VoteApp";
}

function normalizeEmailAddress(email: string): string {
  if (typeof email !== "string") {
    throw new TypeError("Email address must be a string");
  }
  const normalized = email.trim();
  if (normalized.length === 0) {
    throw new TypeError("Email address must be a non-empty string");
  }
  return normalized;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function assertBroadcastInput(input: IssueBroadcastEmailInput): void {
  if (input.subject.trim().length === 0) {
    throw new Error("Broadcast email requires a subject");
  }
  if (input.body.trim().length === 0) {
    throw new Error("Broadcast email requires a body");
  }
}

function describeMatchedAreas(matchedAreaNames: readonly string[]): string {
  return matchedAreaNames.length > 0 ? matchedAreaNames.join(", ") : "your saved issues";
}

export function buildBroadcastTextBody(appName: string | undefined, input: IssueBroadcastEmailInput): string {
  const brand = resolveBrandName(appName);
  const unsubscribeLine = input.unsubscribeUrl
    ? `\nUnsubscribe from these updates: ${input.unsubscribeUrl}`
    : "";
  return (
    `Hi ${input.firstName.trim() || "there"},\n\n` +
    `${input.body.trim()}\n\n` +
    `You are receiving this because you saved ${describeMatchedAreas(input.matchedAreaNames)} ` +
    `as issues you care about on ${brand}. You can change this in your account settings.` +
    unsubscribeLine
  );
}

export function buildBroadcastHtmlBody(appName: string | undefined, input: IssueBroadcastEmailInput): string {
  const brand = escapeHtml(resolveBrandName(appName));
  // Operator body is plain text: escape it, then blank lines become
  // paragraphs and single newlines become line breaks.
  const paragraphs = input.body
    .trim()
    .split(/\n{2,}/)
    .map((paragraph) => `    <p>${escapeHtml(paragraph).replaceAll("\n", "<br>")}</p>`)
    .join("\n");
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
  </head>
  <body>
    <p>Hi ${escapeHtml(input.firstName.trim() || "there")},</p>
${paragraphs}
    <p>You are receiving this because you saved ${escapeHtml(describeMatchedAreas(input.matchedAreaNames))} as issues you care about on ${brand}. You can change this in your account settings.</p>
${
    input.unsubscribeUrl
      ? `    <p><a href="${escapeHtml(input.unsubscribeUrl)}">Unsubscribe from these updates</a></p>\n`
      : ""
  }  </body>
</html>`;
}

export function createSesIssueBroadcastMailer(options: SesIssueBroadcastMailerOptions): IssueBroadcastMailer {
  return {
    async sendBroadcastEmail(input) {
      assertBroadcastInput(input);
      await options.sesClient.send(
        new SendEmailCommand({
          FromEmailAddress: normalizeEmailAddress(options.fromEmailAddress),
          Destination: {
            ToAddresses: [normalizeEmailAddress(input.email)],
          },
          ReplyToAddresses: options.replyToEmailAddress
            ? [normalizeEmailAddress(options.replyToEmailAddress)]
            : undefined,
          Content: {
            Simple: {
              ...(input.unsubscribeUrl
                ? {
                    Headers: [
                      { Name: "List-Unsubscribe", Value: `<${input.unsubscribeUrl}>` },
                      { Name: "List-Unsubscribe-Post", Value: "List-Unsubscribe=One-Click" },
                    ],
                  }
                : {}),
              Subject: {
                Data: input.subject.trim(),
                Charset: "UTF-8",
              },
              Body: {
                Text: {
                  Data: buildBroadcastTextBody(options.appName, input),
                  Charset: "UTF-8",
                },
                Html: {
                  Data: buildBroadcastHtmlBody(options.appName, input),
                  Charset: "UTF-8",
                },
              },
            },
          },
        })
      );
    },
  };
}

export type ConsoleIssueBroadcastMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

// Local-development mailer: prints the broadcast instead of sending email, so
// the full send loop is testable without an email provider.
export function createConsoleIssueBroadcastMailer(
  options: ConsoleIssueBroadcastMailerOptions = {}
): IssueBroadcastMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendBroadcastEmail(input) {
      assertBroadcastInput(input);
      const body = buildBroadcastTextBody(options.appName, input);
      log(`[issue-broadcast-mailer:console] to=${normalizeEmailAddress(input.email)} subject=${input.subject.trim()}\n${body}`);
    },
  };
}

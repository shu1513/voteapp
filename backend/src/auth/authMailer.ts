import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";
import { APP_NAME } from "../constants/brand.js";

export type AuthMailer = {
  sendVerificationEmail(input: AuthMailerEmailInput): Promise<void>;
  sendPasswordResetEmail(input: AuthMailerEmailInput): Promise<void>;
  /** Sent to the requested NEW address; the link proves control of it. */
  sendEmailChangeEmail(input: AuthMailerEmailInput): Promise<void>;
};

export type AuthMailerEmailInput = {
  email: string;
  linkUrl: string;
};

export type SesAuthMailerOptions = {
  appName?: string;
  fromEmailAddress: string;
  replyToEmailAddress?: string;
  sesClient: Pick<SESv2Client, "send">;
};

type EmailMessageKind = "verification" | "password_reset" | "email_change";

function normalizeEmailAddress(email: string): string {
  if (typeof email !== "string") {
    throw new Error("Email address must be a string");
  }
  const normalized = email.trim();
  if (normalized.length === 0) {
    throw new Error("Email address must be a non-empty string");
  }
  return normalized;
}

function normalizeAbsoluteLinkUrl(linkUrl: string): string {
  if (typeof linkUrl !== "string") {
    throw new Error("Link URL must be a string");
  }
  const normalized = linkUrl.trim();
  if (normalized.length === 0) {
    throw new Error("Link URL must be a non-empty string");
  }

  const parsed = new URL(normalized);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("Link URL must use http or https");
  }
  return parsed.toString();
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function resolveMailerBrandName(appName: string | undefined): string {
  const normalized = appName?.trim();
  return normalized && normalized.length > 0 ? normalized : APP_NAME;
}

function buildSubject(kind: EmailMessageKind, appName: string): string {
  if (kind === "verification") {
    return `[${appName}] Verify your email`;
  }
  if (kind === "email_change") {
    return `[${appName}] Confirm your new email address`;
  }
  return `[${appName}] Reset your password`;
}

function buildTextBody(kind: EmailMessageKind, appName: string, linkUrl: string): string {
  const intro =
    kind === "verification"
      ? `Verify your email for ${appName}.`
      : kind === "email_change"
        ? `Confirm your new email address for ${appName}.`
        : `Reset your password for ${appName}.`;
  return `${intro}\n\nOpen this link to continue:\n${linkUrl}\n\nIf you did not request this email, you can ignore it.`;
}

function buildHtmlBody(kind: EmailMessageKind, appName: string, linkUrl: string): string {
  const title =
    kind === "verification"
      ? "Verify your email"
      : kind === "email_change"
        ? "Confirm your new email address"
        : "Reset your password";
  const intro =
    kind === "verification"
      ? `Verify your email for ${escapeHtml(appName)}.`
      : kind === "email_change"
        ? `Confirm your new email address for ${escapeHtml(appName)}.`
        : `Reset your password for ${escapeHtml(appName)}.`;
  const escapedLink = escapeHtml(linkUrl);
  return `<!doctype html>
<html lang="en">
  <body>
    <p>${intro}</p>
    <p><a href="${escapedLink}">${title}</a></p>
    <p>If you did not request this email, you can ignore it.</p>
  </body>
</html>`;
}

function buildEmailCommandInput(
  options: SesAuthMailerOptions,
  kind: EmailMessageKind,
  input: AuthMailerEmailInput
) {
  const appName = resolveMailerBrandName(options.appName);
  const toEmailAddress = normalizeEmailAddress(input.email);
  const linkUrl = normalizeAbsoluteLinkUrl(input.linkUrl);

  return {
    FromEmailAddress: normalizeEmailAddress(options.fromEmailAddress),
    Destination: {
      ToAddresses: [toEmailAddress],
    },
    ReplyToAddresses: options.replyToEmailAddress
      ? [normalizeEmailAddress(options.replyToEmailAddress)]
      : undefined,
    Content: {
      Simple: {
        Subject: {
          Data: buildSubject(kind, appName),
          Charset: "UTF-8",
        },
        Body: {
          Text: {
            Data: buildTextBody(kind, appName, linkUrl),
            Charset: "UTF-8",
          },
          Html: {
            Data: buildHtmlBody(kind, appName, linkUrl),
            Charset: "UTF-8",
          },
        },
      },
    },
  };
}

async function sendAuthEmail(
  options: SesAuthMailerOptions,
  kind: EmailMessageKind,
  input: AuthMailerEmailInput
): Promise<void> {
  const commandInput = buildEmailCommandInput(options, kind, input);
  await options.sesClient.send(new SendEmailCommand(commandInput));
}

export function createSesAuthMailer(options: SesAuthMailerOptions): AuthMailer {
  return {
    async sendVerificationEmail(input) {
      await sendAuthEmail(options, "verification", input);
    },
    async sendPasswordResetEmail(input) {
      await sendAuthEmail(options, "password_reset", input);
    },
    async sendEmailChangeEmail(input) {
      await sendAuthEmail(options, "email_change", input);
    },
  };
}

export type ConsoleAuthMailerOptions = {
  log?: (message: string) => void;
};

// Local-development mailer: prints the link instead of sending email, so the
// signup/verify/reset flows are fully testable without an email provider.
// Never use in production — the printed link grants account access.
export function createConsoleAuthMailer(options: ConsoleAuthMailerOptions = {}): AuthMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendVerificationEmail(input) {
      const linkUrl = normalizeAbsoluteLinkUrl(input.linkUrl);
      log(`[auth-mailer:console] verification email for ${normalizeEmailAddress(input.email)}: ${linkUrl}`);
    },
    async sendPasswordResetEmail(input) {
      const linkUrl = normalizeAbsoluteLinkUrl(input.linkUrl);
      log(`[auth-mailer:console] password reset email for ${normalizeEmailAddress(input.email)}: ${linkUrl}`);
    },
    async sendEmailChangeEmail(input) {
      const linkUrl = normalizeAbsoluteLinkUrl(input.linkUrl);
      log(`[auth-mailer:console] email change email for ${normalizeEmailAddress(input.email)}: ${linkUrl}`);
    },
  };
}

import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";
import { APP_NAME } from "../../constants/brand.js";

export type NewElectionAlertItem = {
  electionTitle: string;
  electionDate: string;
  districtName: string;
};

export type NewElectionAlertEmailInput = {
  email: string;
  firstName: string;
  /**
   * Items to render, already sorted by district then date. May be fewer than
   * totalEventCount when the caller caps rendering; the body then notes the
   * remainder.
   */
  items: readonly NewElectionAlertItem[];
  totalEventCount: number;
  /**
   * Signed per-user unsubscribe link scoped to new-election alerts. When
   * present it is rendered in the footer and emitted as List-Unsubscribe /
   * List-Unsubscribe-Post headers (RFC 8058 one-click).
   */
  unsubscribeUrl?: string;
};

export type NewElectionAlertMailer = {
  sendAlertEmail(input: NewElectionAlertEmailInput): Promise<void>;
};

export type SesNewElectionAlertMailerOptions = {
  appName?: string;
  fromEmailAddress: string;
  replyToEmailAddress?: string;
  sesClient: Pick<SESv2Client, "send">;
};

function resolveBrandName(appName: string | undefined): string {
  const normalized = appName?.trim();
  return normalized && normalized.length > 0 ? normalized : APP_NAME;
}

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

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function describeItem(item: NewElectionAlertItem): string {
  const title = item.electionTitle.trim() || "New election";
  const date = item.electionDate.trim();
  return date ? `${title} (${date})` : title;
}

function groupItemsByDistrict(
  items: readonly NewElectionAlertItem[]
): Array<{ districtName: string; lines: string[] }> {
  const groups: Array<{ districtName: string; lines: string[] }> = [];
  for (const item of items) {
    const last = groups[groups.length - 1];
    if (last && last.districtName === item.districtName) {
      last.lines.push(describeItem(item));
    } else {
      groups.push({ districtName: item.districtName, lines: [describeItem(item)] });
    }
  }
  return groups;
}

export function buildAlertSubject(appName: string | undefined, totalEventCount: number): string {
  const brand = resolveBrandName(appName);
  const noun = totalEventCount === 1 ? "election" : "elections";
  return `[${brand}] ${totalEventCount} new ${noun} in your districts`;
}

export function buildAlertTextBody(appName: string | undefined, input: NewElectionAlertEmailInput): string {
  const brand = resolveBrandName(appName);
  const groups = groupItemsByDistrict(input.items);
  const sections = groups.map(
    (group) => `${group.districtName}\n${group.lines.map((line) => `- ${line}`).join("\n")}`
  );
  const remainder = input.totalEventCount - input.items.length;
  const remainderLine = remainder > 0 ? `\n…and ${remainder} more election${remainder === 1 ? "" : "s"}.\n` : "";
  const unsubscribeLine = input.unsubscribeUrl
    ? `\nUnsubscribe from these alerts: ${input.unsubscribeUrl}`
    : "";
  return (
    `Hi ${input.firstName.trim() || "there"},\n\n` +
    `New elections are coming up in your districts on ${brand}:\n\n` +
    `${sections.join("\n\n")}\n` +
    remainderLine +
    `\nYou are receiving this because you have new-election alerts enabled ` +
    `for your districts on ${brand}. You can change this in your account settings.` +
    unsubscribeLine
  );
}

export function buildAlertHtmlBody(appName: string | undefined, input: NewElectionAlertEmailInput): string {
  const brand = escapeHtml(resolveBrandName(appName));
  const groups = groupItemsByDistrict(input.items);
  const sections = groups
    .map(
      (group) =>
        `    <h3>${escapeHtml(group.districtName)}</h3>\n` +
        `    <ul>\n${group.lines.map((line) => `      <li>${escapeHtml(line)}</li>`).join("\n")}\n    </ul>`
    )
    .join("\n");
  const remainder = input.totalEventCount - input.items.length;
  const remainderHtml =
    remainder > 0 ? `    <p>…and ${remainder} more election${remainder === 1 ? "" : "s"}.</p>\n` : "";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
  </head>
  <body>
    <p>Hi ${escapeHtml(input.firstName.trim() || "there")},</p>
    <p>New elections are coming up in your districts on ${brand}:</p>
${sections}
${remainderHtml}    <p>You are receiving this because you have new-election alerts enabled for your districts on ${brand}. You can change this in your account settings.</p>
${
    input.unsubscribeUrl
      ? `    <p><a href="${escapeHtml(input.unsubscribeUrl)}">Unsubscribe from these alerts</a></p>\n`
      : ""
  }  </body>
</html>`;
}

export function createSesNewElectionAlertMailer(
  options: SesNewElectionAlertMailerOptions
): NewElectionAlertMailer {
  return {
    async sendAlertEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Alert email requires at least one item");
      }
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
                Data: buildAlertSubject(options.appName, input.totalEventCount),
                Charset: "UTF-8",
              },
              Body: {
                Text: {
                  Data: buildAlertTextBody(options.appName, input),
                  Charset: "UTF-8",
                },
                Html: {
                  Data: buildAlertHtmlBody(options.appName, input),
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

export type ConsoleNewElectionAlertMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

// Local-development mailer: prints the alert instead of sending email, so the
// full send loop is testable without an email provider.
export function createConsoleNewElectionAlertMailer(
  options: ConsoleNewElectionAlertMailerOptions = {}
): NewElectionAlertMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendAlertEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Alert email requires at least one item");
      }
      const subject = buildAlertSubject(options.appName, input.totalEventCount);
      const body = buildAlertTextBody(options.appName, input);
      log(`[new-election-alert-mailer:console] to=${normalizeEmailAddress(input.email)} subject=${subject}\n${body}`);
    },
  };
}

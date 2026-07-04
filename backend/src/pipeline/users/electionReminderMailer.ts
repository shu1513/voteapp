import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";

export type ElectionReminderItem = {
  electionTitle: string;
  districtName: string;
};

export type ElectionReminderEmailInput = {
  email: string;
  firstName: string;
  /**
   * Human-readable label for the shared election date (all items in one
   * reminder are on the same day, tomorrow), e.g. "Tuesday, November 3, 2026".
   */
  electionDateLabel: string;
  /**
   * Items to render, already sorted by district then title. May be fewer than
   * totalElectionCount when the caller caps rendering; the body then notes
   * the remainder.
   */
  items: readonly ElectionReminderItem[];
  totalElectionCount: number;
  /**
   * Signed per-user unsubscribe link scoped to election reminders. When
   * present it is rendered in the footer and emitted as List-Unsubscribe /
   * List-Unsubscribe-Post headers (RFC 8058 one-click).
   */
  unsubscribeUrl?: string;
};

export type ElectionReminderMailer = {
  sendReminderEmail(input: ElectionReminderEmailInput): Promise<void>;
};

export type SesElectionReminderMailerOptions = {
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

/**
 * "2026-11-03" -> "Tuesday, November 3, 2026". Formats in UTC so the plain
 * date never shifts a day; anything not shaped like an ISO date passes
 * through untouched.
 */
export function formatElectionDateLabel(isoDate: string): string {
  const normalized = isoDate.trim();
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(normalized);
  if (!match) {
    return normalized;
  }
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  return new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(date);
}

function groupItemsByDistrict(
  items: readonly ElectionReminderItem[]
): Array<{ districtName: string; lines: string[] }> {
  const groups: Array<{ districtName: string; lines: string[] }> = [];
  for (const item of items) {
    const line = item.electionTitle.trim() || "Election";
    const last = groups[groups.length - 1];
    if (last && last.districtName === item.districtName) {
      last.lines.push(line);
    } else {
      groups.push({ districtName: item.districtName, lines: [line] });
    }
  }
  return groups;
}

export function buildReminderSubject(appName: string | undefined, totalElectionCount: number): string {
  const brand = resolveBrandName(appName);
  const noun = totalElectionCount === 1 ? "election" : "elections";
  return `[${brand}] Reminder: ${totalElectionCount} ${noun} tomorrow`;
}

function buildIntroLine(brand: string, input: ElectionReminderEmailInput): string {
  const verb = input.totalElectionCount === 1 ? "is" : "are";
  const noun = input.totalElectionCount === 1 ? "election" : "elections";
  return (
    `Tomorrow (${input.electionDateLabel}) there ${verb} ${input.totalElectionCount} ${noun} ` +
    `in your districts on ${brand}:`
  );
}

export function buildReminderTextBody(appName: string | undefined, input: ElectionReminderEmailInput): string {
  const brand = resolveBrandName(appName);
  const groups = groupItemsByDistrict(input.items);
  const sections = groups.map(
    (group) => `${group.districtName}\n${group.lines.map((line) => `- ${line}`).join("\n")}`
  );
  const remainder = input.totalElectionCount - input.items.length;
  const remainderLine = remainder > 0 ? `\n…and ${remainder} more election${remainder === 1 ? "" : "s"}.\n` : "";
  const unsubscribeLine = input.unsubscribeUrl
    ? `\nUnsubscribe from these reminders: ${input.unsubscribeUrl}`
    : "";
  return (
    `Hi ${input.firstName.trim() || "there"},\n\n` +
    `${buildIntroLine(brand, input)}\n\n` +
    `${sections.join("\n\n")}\n` +
    remainderLine +
    `\nYou are receiving this because you have day-before election reminders ` +
    `enabled on ${brand}. You can change this in your account settings.` +
    unsubscribeLine
  );
}

export function buildReminderHtmlBody(appName: string | undefined, input: ElectionReminderEmailInput): string {
  const brand = escapeHtml(resolveBrandName(appName));
  const groups = groupItemsByDistrict(input.items);
  const sections = groups
    .map(
      (group) =>
        `    <h3>${escapeHtml(group.districtName)}</h3>\n` +
        `    <ul>\n${group.lines.map((line) => `      <li>${escapeHtml(line)}</li>`).join("\n")}\n    </ul>`
    )
    .join("\n");
  const remainder = input.totalElectionCount - input.items.length;
  const remainderHtml =
    remainder > 0 ? `    <p>…and ${remainder} more election${remainder === 1 ? "" : "s"}.</p>\n` : "";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
  </head>
  <body>
    <p>Hi ${escapeHtml(input.firstName.trim() || "there")},</p>
    <p>${escapeHtml(buildIntroLine(resolveBrandName(appName), input))}</p>
${sections}
${remainderHtml}    <p>You are receiving this because you have day-before election reminders enabled on ${brand}. You can change this in your account settings.</p>
${
    input.unsubscribeUrl
      ? `    <p><a href="${escapeHtml(input.unsubscribeUrl)}">Unsubscribe from these reminders</a></p>\n`
      : ""
  }  </body>
</html>`;
}

export function createSesElectionReminderMailer(
  options: SesElectionReminderMailerOptions
): ElectionReminderMailer {
  return {
    async sendReminderEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Reminder email requires at least one item");
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
                Data: buildReminderSubject(options.appName, input.totalElectionCount),
                Charset: "UTF-8",
              },
              Body: {
                Text: {
                  Data: buildReminderTextBody(options.appName, input),
                  Charset: "UTF-8",
                },
                Html: {
                  Data: buildReminderHtmlBody(options.appName, input),
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

export type ConsoleElectionReminderMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

// Local-development mailer: prints the reminder instead of sending email, so
// the full send loop is testable without an email provider.
export function createConsoleElectionReminderMailer(
  options: ConsoleElectionReminderMailerOptions = {}
): ElectionReminderMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendReminderEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Reminder email requires at least one item");
      }
      const subject = buildReminderSubject(options.appName, input.totalElectionCount);
      const body = buildReminderTextBody(options.appName, input);
      log(`[election-reminder-mailer:console] to=${normalizeEmailAddress(input.email)} subject=${subject}\n${body}`);
    },
  };
}

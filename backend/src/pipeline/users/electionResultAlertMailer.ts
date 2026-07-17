import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";

export type ElectionResultAlertItem = {
  electionTitle: string;
  electionDate: string;
  districtName: string;
  /** Decisive outcome from the result row: won/advanced/runoff/passed/failed. */
  outcome: string;
  /** Winner display names (with party when known); empty for ballot measures. */
  winnerNames: readonly string[];
};

export type ElectionResultAlertEmailInput = {
  email: string;
  firstName: string;
  /**
   * Items to render, already sorted by district then date. May be fewer than
   * totalEventCount when the caller caps rendering; the body then notes the
   * remainder.
   */
  items: readonly ElectionResultAlertItem[];
  totalEventCount: number;
  /**
   * Signed per-user unsubscribe link scoped to the email digest (result
   * alerts ride the digest opt-in). When present it is rendered in the footer
   * and emitted as List-Unsubscribe / List-Unsubscribe-Post headers (RFC 8058
   * one-click).
   */
  unsubscribeUrl?: string;
};

export type ElectionResultAlertMailer = {
  sendResultAlertEmail(input: ElectionResultAlertEmailInput): Promise<void>;
};

export type SesElectionResultAlertMailerOptions = {
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

function describeOutcome(item: ElectionResultAlertItem): string {
  const winners = item.winnerNames.map((name) => name.trim()).filter((name) => name.length > 0);
  switch (item.outcome) {
    case "won":
      return winners.length > 0 ? `Winner${winners.length === 1 ? "" : "s"}: ${winners.join(", ")}` : "Decided";
    case "advanced":
      return winners.length > 0 ? `Advancing: ${winners.join(", ")}` : "Advanced to the next round";
    case "runoff":
      return winners.length > 0 ? `Headed to a runoff: ${winners.join(", ")}` : "Headed to a runoff";
    case "passed":
      return "Measure passed";
    case "failed":
      return "Measure failed";
    default:
      return "Results available";
  }
}

function describeItem(item: ElectionResultAlertItem): string {
  const title = item.electionTitle.trim() || "Election";
  const date = item.electionDate.trim();
  const heading = date ? `${title} (${date})` : title;
  return `${heading} — ${describeOutcome(item)}`;
}

function groupItemsByDistrict(
  items: readonly ElectionResultAlertItem[]
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

export function buildResultAlertSubject(appName: string | undefined, totalEventCount: number): string {
  const brand = resolveBrandName(appName);
  const noun = totalEventCount === 1 ? "election" : "elections";
  return `[${brand}] Results are in for ${totalEventCount} ${noun} in your districts`;
}

export function buildResultAlertTextBody(
  appName: string | undefined,
  input: ElectionResultAlertEmailInput
): string {
  const brand = resolveBrandName(appName);
  const groups = groupItemsByDistrict(input.items);
  const sections = groups.map(
    (group) => `${group.districtName}\n${group.lines.map((line) => `- ${line}`).join("\n")}`
  );
  const remainder = input.totalEventCount - input.items.length;
  const remainderLine = remainder > 0 ? `\n…and results for ${remainder} more election${remainder === 1 ? "" : "s"}.\n` : "";
  const unsubscribeLine = input.unsubscribeUrl
    ? `\nUnsubscribe from these emails: ${input.unsubscribeUrl}`
    : "";
  return (
    `Hi ${input.firstName.trim() || "there"},\n\n` +
    `Election results are in for your districts on ${brand}:\n\n` +
    `${sections.join("\n\n")}\n` +
    remainderLine +
    `\nYou are receiving this because you have the email digest enabled ` +
    `on ${brand}. You can change this in your account settings.` +
    unsubscribeLine
  );
}

export function buildResultAlertHtmlBody(
  appName: string | undefined,
  input: ElectionResultAlertEmailInput
): string {
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
    remainder > 0
      ? `    <p>…and results for ${remainder} more election${remainder === 1 ? "" : "s"}.</p>\n`
      : "";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
  </head>
  <body>
    <p>Hi ${escapeHtml(input.firstName.trim() || "there")},</p>
    <p>Election results are in for your districts on ${brand}:</p>
${sections}
${remainderHtml}    <p>You are receiving this because you have the email digest enabled on ${brand}. You can change this in your account settings.</p>
${
    input.unsubscribeUrl
      ? `    <p><a href="${escapeHtml(input.unsubscribeUrl)}">Unsubscribe from these emails</a></p>\n`
      : ""
  }  </body>
</html>`;
}

export function createSesElectionResultAlertMailer(
  options: SesElectionResultAlertMailerOptions
): ElectionResultAlertMailer {
  return {
    async sendResultAlertEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Result alert email requires at least one item");
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
                Data: buildResultAlertSubject(options.appName, input.totalEventCount),
                Charset: "UTF-8",
              },
              Body: {
                Text: {
                  Data: buildResultAlertTextBody(options.appName, input),
                  Charset: "UTF-8",
                },
                Html: {
                  Data: buildResultAlertHtmlBody(options.appName, input),
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

export type ConsoleElectionResultAlertMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

// Local-development mailer: prints the alert instead of sending email, so the
// full send loop is testable without an email provider.
export function createConsoleElectionResultAlertMailer(
  options: ConsoleElectionResultAlertMailerOptions = {}
): ElectionResultAlertMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendResultAlertEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Result alert email requires at least one item");
      }
      const subject = buildResultAlertSubject(options.appName, input.totalEventCount);
      const body = buildResultAlertTextBody(options.appName, input);
      log(
        `[election-result-alert-mailer:console] to=${normalizeEmailAddress(input.email)} subject=${subject}\n${body}`
      );
    },
  };
}

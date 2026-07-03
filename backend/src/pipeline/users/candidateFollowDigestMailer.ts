import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";

export type CandidateFollowDigestItem = {
  candidateDisplayName: string;
  eventType: "candidate_record_update" | "candidate_future_election";
  recordDescription?: string | null;
  electionTitle?: string | null;
  electionDate?: string | null;
};

export type CandidateFollowDigestEmailInput = {
  email: string;
  firstName: string;
  /**
   * Items to render, already sorted by candidate. May be fewer than
   * totalEventCount when the caller caps rendering; the body then notes the
   * remainder.
   */
  items: readonly CandidateFollowDigestItem[];
  totalEventCount: number;
  /**
   * Signed per-user unsubscribe link. When present it is rendered in the
   * footer and emitted as List-Unsubscribe / List-Unsubscribe-Post headers
   * (RFC 8058 one-click, expected by major mailbox providers at volume).
   */
  unsubscribeUrl?: string;
};

export type CandidateFollowDigestMailer = {
  sendDigestEmail(input: CandidateFollowDigestEmailInput): Promise<void>;
};

export type SesCandidateFollowDigestMailerOptions = {
  appName?: string;
  fromEmailAddress: string;
  replyToEmailAddress?: string;
  sesClient: Pick<SESv2Client, "send">;
};

const MAX_RECORD_DESCRIPTION_LENGTH = 240;

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

function truncate(value: string, maxLength: number): string {
  const normalized = value.trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength - 1).trimEnd()}…`;
}

function describeItem(item: CandidateFollowDigestItem): string {
  if (item.eventType === "candidate_future_election") {
    const title = item.electionTitle?.trim() || "an upcoming election";
    const date = item.electionDate?.trim();
    return date ? `On the ballot: ${title} (${date})` : `On the ballot: ${title}`;
  }
  const description = item.recordDescription?.trim();
  return description
    ? `New record: ${truncate(description, MAX_RECORD_DESCRIPTION_LENGTH)}`
    : "New record added";
}

function groupItemsByCandidate(
  items: readonly CandidateFollowDigestItem[]
): Array<{ candidateDisplayName: string; lines: string[] }> {
  const groups: Array<{ candidateDisplayName: string; lines: string[] }> = [];
  for (const item of items) {
    const last = groups[groups.length - 1];
    if (last && last.candidateDisplayName === item.candidateDisplayName) {
      last.lines.push(describeItem(item));
    } else {
      groups.push({ candidateDisplayName: item.candidateDisplayName, lines: [describeItem(item)] });
    }
  }
  return groups;
}

export function buildDigestSubject(appName: string | undefined, totalEventCount: number): string {
  const brand = resolveBrandName(appName);
  const noun = totalEventCount === 1 ? "update" : "updates";
  return `[${brand}] ${totalEventCount} ${noun} on candidates you follow`;
}

export function buildDigestTextBody(appName: string | undefined, input: CandidateFollowDigestEmailInput): string {
  const brand = resolveBrandName(appName);
  const groups = groupItemsByCandidate(input.items);
  const sections = groups.map(
    (group) => `${group.candidateDisplayName}\n${group.lines.map((line) => `- ${line}`).join("\n")}`
  );
  const remainder = input.totalEventCount - input.items.length;
  const remainderLine = remainder > 0 ? `\n…and ${remainder} more update${remainder === 1 ? "" : "s"}.\n` : "";
  const unsubscribeLine = input.unsubscribeUrl
    ? `\nUnsubscribe from these digests: ${input.unsubscribeUrl}`
    : "";
  return (
    `Hi ${input.firstName.trim() || "there"},\n\n` +
    `Updates on candidates you follow on ${brand}:\n\n` +
    `${sections.join("\n\n")}\n` +
    remainderLine +
    `\nYou are receiving this because you follow these candidates on ${brand} ` +
    `and have digest emails enabled. You can change this in your account settings.` +
    unsubscribeLine
  );
}

export function buildDigestHtmlBody(appName: string | undefined, input: CandidateFollowDigestEmailInput): string {
  const brand = escapeHtml(resolveBrandName(appName));
  const groups = groupItemsByCandidate(input.items);
  const sections = groups
    .map(
      (group) =>
        `    <h3>${escapeHtml(group.candidateDisplayName)}</h3>\n` +
        `    <ul>\n${group.lines.map((line) => `      <li>${escapeHtml(line)}</li>`).join("\n")}\n    </ul>`
    )
    .join("\n");
  const remainder = input.totalEventCount - input.items.length;
  const remainderHtml =
    remainder > 0 ? `    <p>…and ${remainder} more update${remainder === 1 ? "" : "s"}.</p>\n` : "";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
  </head>
  <body>
    <p>Hi ${escapeHtml(input.firstName.trim() || "there")},</p>
    <p>Updates on candidates you follow on ${brand}:</p>
${sections}
${remainderHtml}    <p>You are receiving this because you follow these candidates on ${brand} and have digest emails enabled. You can change this in your account settings.</p>
${
    input.unsubscribeUrl
      ? `    <p><a href="${escapeHtml(input.unsubscribeUrl)}">Unsubscribe from these digests</a></p>\n`
      : ""
  }  </body>
</html>`;
}

export function createSesCandidateFollowDigestMailer(
  options: SesCandidateFollowDigestMailerOptions
): CandidateFollowDigestMailer {
  return {
    async sendDigestEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Digest email requires at least one item");
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
                Data: buildDigestSubject(options.appName, input.totalEventCount),
                Charset: "UTF-8",
              },
              Body: {
                Text: {
                  Data: buildDigestTextBody(options.appName, input),
                  Charset: "UTF-8",
                },
                Html: {
                  Data: buildDigestHtmlBody(options.appName, input),
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

export type ConsoleCandidateFollowDigestMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

// Local-development mailer: prints the digest instead of sending email, so the
// full send loop is testable without an email provider.
export function createConsoleCandidateFollowDigestMailer(
  options: ConsoleCandidateFollowDigestMailerOptions = {}
): CandidateFollowDigestMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendDigestEmail(input) {
      if (input.items.length === 0) {
        throw new Error("Digest email requires at least one item");
      }
      const subject = buildDigestSubject(options.appName, input.totalEventCount);
      const body = buildDigestTextBody(options.appName, input);
      log(`[digest-mailer:console] to=${normalizeEmailAddress(input.email)} subject=${subject}\n${body}`);
    },
  };
}

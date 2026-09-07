import { SendEmailCommand, type SESv2Client } from "@aws-sdk/client-sesv2";
import { APP_NAME } from "../../constants/brand.js";
import type { OpenContentReportSummary } from "./contentReportQueue.js";

/**
 * One operator email listing what is open in content_reports. Plain text
 * only, and the body is built from ids/counts/ages alone: reporter-written
 * message and URL fields never appear (mail clients auto-link URLs, and the
 * text is attacker-controlled). Detail lives in `content-reports -- list`.
 */
export type ContentReportSummaryEmailInput = {
  toEmailAddress: string;
  summary: OpenContentReportSummary;
};

export type ContentReportSummaryMailer = {
  sendSummaryEmail(input: ContentReportSummaryEmailInput): Promise<void>;
};

function resolveBrandName(appName: string | undefined): string {
  const normalized = appName?.trim();
  return normalized && normalized.length > 0 ? normalized : APP_NAME;
}

function plural(count: number, singular: string, pluralForm = `${singular}s`): string {
  return `${count} ${count === 1 ? singular : pluralForm}`;
}

export function buildSummarySubject(appName: string | undefined, summary: OpenContentReportSummary): string {
  return `[${resolveBrandName(appName)}] ${plural(summary.open_count, "open content report")}`;
}

export function buildSummaryTextBody(appName: string | undefined, summary: OpenContentReportSummary): string {
  const brand = resolveBrandName(appName);
  const lines = [
    `${brand} has ${plural(summary.open_count, "open content report")} across ${plural(summary.by_entity.length, "entity", "entities")}; the oldest is ${plural(summary.oldest_age_days, "day")} old.`,
    "",
    "reports  oldest(days)  entity",
  ];
  for (const entity of summary.by_entity) {
    lines.push(
      `${String(entity.report_count).padStart(7)}  ${String(entity.oldest_age_days).padStart(12)}  ${entity.entity_type} ${entity.entity_id}`
    );
  }
  lines.push("", "Read and close them with: npm run content-reports -- list  /  npm run content-reports -- resolve --id <uuid> ...");
  return lines.join("\n");
}

export type SesContentReportSummaryMailerOptions = {
  appName?: string;
  fromEmailAddress: string;
  sesClient: Pick<SESv2Client, "send">;
};

export function createSesContentReportSummaryMailer(
  options: SesContentReportSummaryMailerOptions
): ContentReportSummaryMailer {
  return {
    async sendSummaryEmail(input) {
      const to = input.toEmailAddress.trim();
      if (to.length === 0) {
        throw new Error("Summary recipient must be a non-empty email address");
      }
      await options.sesClient.send(
        new SendEmailCommand({
          FromEmailAddress: options.fromEmailAddress.trim(),
          Destination: { ToAddresses: [to] },
          Content: {
            Simple: {
              Subject: { Data: buildSummarySubject(options.appName, input.summary), Charset: "UTF-8" },
              Body: { Text: { Data: buildSummaryTextBody(options.appName, input.summary), Charset: "UTF-8" } },
            },
          },
        })
      );
    },
  };
}

export type ConsoleContentReportSummaryMailerOptions = {
  appName?: string;
  log?: (message: string) => void;
};

/** Local-development mailer: prints the summary instead of sending it. */
export function createConsoleContentReportSummaryMailer(
  options: ConsoleContentReportSummaryMailerOptions = {}
): ContentReportSummaryMailer {
  const log = options.log ?? ((message: string) => console.log(message));
  return {
    async sendSummaryEmail(input) {
      log(
        [
          `[console content-report summary] to=${input.toEmailAddress}`,
          `subject: ${buildSummarySubject(options.appName, input.summary)}`,
          "",
          buildSummaryTextBody(options.appName, input.summary),
        ].join("\n")
      );
    },
  };
}

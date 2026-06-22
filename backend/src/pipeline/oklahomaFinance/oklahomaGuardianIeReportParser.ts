import { inflateSync } from "node:zlib";

import { normalizeOklahomaCandidateNameKeys } from "./oklahomaCandidateCommitteeResolver.js";
import type { OklahomaGuardianIeReportPdfArtifact } from "./oklahomaGuardianIeReportClient.js";

export type OklahomaGuardianIeSupportOppose = "support" | "oppose";

export type OklahomaGuardianIeReportCandidateStance = {
  candidateName: string;
  officeName: string | null;
  supportOppose: OklahomaGuardianIeSupportOppose;
};

export type OklahomaGuardianIeReportParsedSummary = {
  spenderName: string | null;
  amended: boolean | null;
  reportDescription: string | null;
  reportingPeriodBegin: string | null;
  reportingPeriodEnd: string | null;
  totalExpenditures: number | null;
  candidateStances: OklahomaGuardianIeReportCandidateStance[];
  text: string;
};

export type OklahomaGuardianIeReportCandidateEvaluation =
  | {
      status: "matched";
      spenderName: string;
      candidateName: string;
      officeName: string | null;
      supportOppose: OklahomaGuardianIeSupportOppose;
      amount: number;
    }
  | {
      status: "skipped";
      reason:
        | "missing_spender"
        | "missing_amount"
        | "candidate_not_found"
        | "multiple_candidate_stances"
        | "ambiguous_candidate_match";
      matchingCandidateStances: OklahomaGuardianIeReportCandidateStance[];
      candidateStances: OklahomaGuardianIeReportCandidateStance[];
    };

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&#34;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&#(\d+);/g, (_match, code: string) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, code: string) => String.fromCodePoint(Number.parseInt(code, 16)));
}

function normalizeWhitespace(value: string): string {
  return decodeHtmlEntities(value).replace(/\s+/g, " ").trim();
}

function decodePdfStringLiteral(value: string): string {
  return value
    .replace(/\\([nrtbf()\\])/g, (_match, char: string) => {
      switch (char) {
        case "n":
          return "\n";
        case "r":
          return "\r";
        case "t":
          return "\t";
        case "b":
          return "\b";
        case "f":
          return "\f";
        default:
          return char;
      }
    })
    .replace(/\\([0-7]{1,3})/g, (_match, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));
}

function decodePdfHexText(value: string): string {
  let decoded = "";
  for (let index = 0; index + 3 < value.length; index += 4) {
    const code = Number.parseInt(value.slice(index, index + 4), 16);
    if (code === 0x0003) {
      decoded += " ";
    } else if (code === 0x0087) {
      decoded += "•";
    } else if (code >= 32 && code <= 126) {
      decoded += String.fromCharCode(code);
    } else {
      decoded += " ";
    }
  }
  return decoded;
}

function extractPdfTextFragmentsFromStream(text: string): string[] {
  const fragments: string[] = [];
  for (const match of text.matchAll(/\((?:\\.|[^\\()])*\)\s*Tj/g)) {
    const raw = match[0].slice(1, match[0].lastIndexOf(")"));
    const fragment = normalizeWhitespace(decodePdfStringLiteral(raw));
    if (fragment) {
      fragments.push(fragment);
    }
  }
  for (const match of text.matchAll(/<([0-9A-Fa-f]+)>\s*Tj/g)) {
    const fragment = normalizeWhitespace(decodePdfHexText(match[1]));
    if (fragment) {
      fragments.push(fragment);
    }
  }
  return fragments;
}

function pdfStreamPayload(input: Buffer, streamStart: number, streamEnd: number): Buffer {
  let start = streamStart;
  if (input[start] === 0x0d && input[start + 1] === 0x0a) {
    start += 2;
  } else if (input[start] === 0x0a) {
    start += 1;
  }

  let end = streamEnd;
  while (end > start && (input[end - 1] === 0x0a || input[end - 1] === 0x0d)) {
    end -= 1;
  }
  return input.subarray(start, end);
}

export function decodeOklahomaGuardianIeReportPdfArtifact(
  artifact: OklahomaGuardianIeReportPdfArtifact
): Buffer {
  const prefix = "data:application/pdf;base64,";
  if (!artifact.dataUrl.startsWith(prefix)) {
    throw new Error("Oklahoma Guardian IE report artifact is not a PDF data URL");
  }
  return Buffer.from(artifact.dataUrl.slice(prefix.length), "base64");
}

export function extractOklahomaGuardianIeReportPdfText(input: Buffer | OklahomaGuardianIeReportPdfArtifact): string {
  const pdf = Buffer.isBuffer(input) ? input : decodeOklahomaGuardianIeReportPdfArtifact(input);
  const pdfText = pdf.toString("latin1");
  const fragments: string[] = [];

  for (const match of pdfText.matchAll(/\d+\s+0\s+obj\s*<<([\s\S]*?)>>\s*stream/g)) {
    const dictionary = match[1];
    const streamStart = (match.index ?? 0) + match[0].length;
    const streamEnd = pdfText.indexOf("endstream", streamStart);
    if (streamEnd < 0) {
      continue;
    }
    const payload = pdfStreamPayload(pdf, streamStart, streamEnd);
    let content: Buffer;
    if (/\/FlateDecode\b/.test(dictionary)) {
      try {
        content = inflateSync(payload);
      } catch {
        continue;
      }
    } else {
      content = payload;
    }
    fragments.push(...extractPdfTextFragmentsFromStream(content.toString("latin1")));
  }

  return fragments.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function linesFromText(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((line) => normalizeWhitespace(line))
    .filter(Boolean);
}

function nextLineAfter(lines: readonly string[], label: string): string | null {
  const normalizedLabel = normalizeWhitespace(label).toUpperCase();
  const index = lines.findIndex((line) => line.toUpperCase() === normalizedLabel);
  if (index < 0) {
    return null;
  }
  return lines[index + 1] ?? null;
}

function lineIndex(lines: readonly string[], label: string): number {
  const normalizedLabel = normalizeWhitespace(label).toUpperCase();
  return lines.findIndex((line) => line.toUpperCase() === normalizedLabel);
}

function spenderNameFromLines(lines: readonly string[]): string | null {
  const index = lineIndex(lines, "Full Name of Committee or Person Making Expenditure");
  if (index < 0) {
    return null;
  }
  const next = lines[index + 1] ?? null;
  if (next?.toUpperCase() === "ACRONYM") {
    return lines[index + 2] ?? null;
  }
  return next;
}

function reportDescriptionAndPeriodFromLines(lines: readonly string[]): {
  reportDescription: string | null;
  reportingPeriodBegin: string | null;
  reportingPeriodEnd: string | null;
} {
  const index = lineIndex(lines, "Type of Report");
  if (index < 0) {
    return { reportDescription: null, reportingPeriodBegin: null, reportingPeriodEnd: null };
  }

  let reportDescription: string | null = null;
  let reportingPeriodBegin: string | null = null;
  let reportingPeriodEnd: string | null = null;

  for (const line of lines.slice(index + 1, index + 8)) {
    const upper = line.toUpperCase();
    const period = parseReportingPeriod(line);
    if (period.begin && period.end) {
      reportingPeriodBegin = period.begin;
      reportingPeriodEnd = period.end;
      continue;
    }
    if (upper === "REPORTING PERIOD:" || upper === "ETHICS NUMBER:") {
      continue;
    }
    if (!reportDescription) {
      reportDescription = line;
    }
  }

  return { reportDescription, reportingPeriodBegin, reportingPeriodEnd };
}

function parseBooleanLine(value: string | null): boolean | null {
  const normalized = value?.trim().toUpperCase();
  if (normalized === "YES") {
    return true;
  }
  if (normalized === "NO") {
    return false;
  }
  return null;
}

function parseMoney(value: string): number | null {
  const match = /\$?\s*([0-9][0-9,]*(?:\.\d{2})?)/.exec(value);
  if (!match) {
    return null;
  }
  const parsed = Number(match[1].replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseReportingPeriod(value: string | null): { begin: string | null; end: string | null } {
  const match = /^(\d{1,2}\/\d{1,2}\/\d{4})\s*-\s*(\d{1,2}\/\d{1,2}\/\d{4})$/.exec(value ?? "");
  return { begin: match?.[1] ?? null, end: match?.[2] ?? null };
}

function stripLeadingComma(value: string | undefined): string {
  return (value ?? "").replace(/^,+\s*/, "").trim();
}

function candidateAndOfficeBeforeStance(lines: readonly string[], stanceLineIndex: number): {
  candidateName: string | null;
  officeName: string | null;
} {
  const previous = lines[stanceLineIndex - 1];
  const secondPrevious = lines[stanceLineIndex - 2];
  const thirdPrevious = lines[stanceLineIndex - 3];

  if (previous && previous.startsWith(",")) {
    return {
      candidateName: stripLeadingComma(secondPrevious) || null,
      officeName: stripLeadingComma(previous) || null,
    };
  }

  if (secondPrevious && secondPrevious.startsWith(",")) {
    return {
      candidateName: stripLeadingComma(thirdPrevious) || null,
      officeName: [stripLeadingComma(secondPrevious), stripLeadingComma(previous)].filter(Boolean).join(" ") || null,
    };
  }

  return {
    candidateName: stripLeadingComma(secondPrevious) || null,
    officeName: stripLeadingComma(previous) || null,
  };
}

function parseCandidateStances(lines: readonly string[]): OklahomaGuardianIeReportCandidateStance[] {
  const stances: OklahomaGuardianIeReportCandidateStance[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < lines.length; index += 1) {
    const stanceMatch = /^\((SUPPORT|OPPOSE)\)$/.exec(lines[index].toUpperCase());
    if (!stanceMatch) {
      continue;
    }
    const { candidateName, officeName } = candidateAndOfficeBeforeStance(lines, index);
    if (!candidateName || /candidate\(s\)/i.test(candidateName)) {
      continue;
    }
    const stance = {
      candidateName,
      officeName,
      supportOppose: stanceMatch[1] === "SUPPORT" ? "support" : "oppose",
    } satisfies OklahomaGuardianIeReportCandidateStance;
    const key = [stance.candidateName, stance.officeName ?? "", stance.supportOppose]
      .map((value) => value.toUpperCase())
      .join("\u0000");
    if (!seen.has(key)) {
      seen.add(key);
      stances.push(stance);
    }
  }
  return stances;
}

export function parseOklahomaGuardianIeReportText(text: string): OklahomaGuardianIeReportParsedSummary {
  const lines = linesFromText(text);
  const report = reportDescriptionAndPeriodFromLines(lines);
  const totalLabelIndex = lines.findIndex((line) => line.toUpperCase() === "TOTAL EXPENDITURES:");
  const totalExpenditures = totalLabelIndex >= 0 ? parseMoney(lines[totalLabelIndex + 1] ?? "") : null;

  return {
    spenderName: spenderNameFromLines(lines),
    amended: parseBooleanLine(nextLineAfter(lines, "AMENDED:")),
    reportDescription: report.reportDescription,
    reportingPeriodBegin: report.reportingPeriodBegin,
    reportingPeriodEnd: report.reportingPeriodEnd,
    totalExpenditures,
    candidateStances: parseCandidateStances(lines),
    text: lines.join("\n"),
  };
}

function candidateNamesMatch(left: string, right: string): boolean {
  const leftKeys = normalizeOklahomaCandidateNameKeys(left);
  for (const key of normalizeOklahomaCandidateNameKeys(right)) {
    if (leftKeys.has(key)) {
      return true;
    }
  }
  return false;
}

export function evaluateOklahomaGuardianIeReportForCandidate(input: {
  parsed: OklahomaGuardianIeReportParsedSummary;
  candidateName: string;
}): OklahomaGuardianIeReportCandidateEvaluation {
  const matchingCandidateStances = input.parsed.candidateStances.filter((stance) =>
    candidateNamesMatch(stance.candidateName, input.candidateName)
  );

  if (!input.parsed.spenderName) {
    return {
      status: "skipped",
      reason: "missing_spender",
      matchingCandidateStances,
      candidateStances: input.parsed.candidateStances,
    };
  }
  if (input.parsed.totalExpenditures === null) {
    return {
      status: "skipped",
      reason: "missing_amount",
      matchingCandidateStances,
      candidateStances: input.parsed.candidateStances,
    };
  }
  if (matchingCandidateStances.length === 0) {
    return {
      status: "skipped",
      reason: "candidate_not_found",
      matchingCandidateStances,
      candidateStances: input.parsed.candidateStances,
    };
  }
  if (matchingCandidateStances.length > 1) {
    return {
      status: "skipped",
      reason: "ambiguous_candidate_match",
      matchingCandidateStances,
      candidateStances: input.parsed.candidateStances,
    };
  }
  if (input.parsed.candidateStances.length !== 1) {
    return {
      status: "skipped",
      reason: "multiple_candidate_stances",
      matchingCandidateStances,
      candidateStances: input.parsed.candidateStances,
    };
  }

  const stance = matchingCandidateStances[0];
  return {
    status: "matched",
    spenderName: input.parsed.spenderName,
    candidateName: stance.candidateName,
    officeName: stance.officeName,
    supportOppose: stance.supportOppose,
    amount: input.parsed.totalExpenditures,
  };
}

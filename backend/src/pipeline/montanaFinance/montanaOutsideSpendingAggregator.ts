// Montana outside spending: stance-aware candidateIssue parsing, two-stage
// resolution, and per-candidate aggregation (docs/plans/montana-finance.md,
// Phase 2b — re-scoped per Phase 0 Q4: resolved rows only, everything else
// quarantined and reported, never published).
//
// Stance rule (ARM 44.11.502(6)(b) + COPP CERS 101, plan "Stance rule"):
// the candidateIssue field names the candidate the expenditure was intended
// to BENEFIT, so a bare name means support — but filers also write the
// stance into the field itself ("Oppose George Nikolakakos" at scale), and
// mapping those to support would attribute attack money TO its target.
// Therefore: bare name -> support; leading Support -> support; leading
// Oppose -> oppose (filer-declared, published). Stance is never inferred
// beyond the leading verb.
//
// Resolution is exact-evidence only: full-name (or unique-last-name) match
// against the year's registration list, constrained by the parsed office
// token when one is present. Fuzzy matching is forbidden (Phase 0 observed
// LYN BENNET / LYN BENNETT); typo'd names and typo'd stance verbs both fail
// alignment and land in quarantine rather than mis-attributing. A parsed
// office token that CONTRADICTS the unique name match ("KATHY LOVE (SD-9)"
// filed against Love's SD-43 registration) is quarantined as a conflict —
// the token is treated as evidence, never decoration.
//
// Cycle scoping is by transaction date: the IE committee search matches
// committees for a year, but each committee's transaction list is its FULL
// history (verified live 2026-08-28 — the "2026" search surfaced $14.4M
// back to 2020, and row-level electionYear is always null). The window
// [Jan 1 of year-1, Jan 1 of year+1) matches Montana's two-year cycle; the
// UTC/Mountain boundary skew is immaterial at that width.

import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import {
  MONTANA_IE_RECOVERY_TOLERANCE_CENTS,
  montanaIeAttachmentRecoveryFor,
} from "./montanaOutsideAttachmentRecovery.js";
import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import {
  normalizeMontanaPersonNameForMatching,
  parseMontanaCersOfficeTitle,
} from "./montanaCandidateCersResolver.js";
import type {
  MontanaCersCandidateSearchRow,
  MontanaCersIeSweepArtifact,
  MontanaCersIeTransactionRow,
} from "./montanaCersParsers.js";
import type { MontanaFinanceOutsideGroupInput } from "./montanaFinanceWriter.js";

export type MontanaIeTargetStance = "support" | "oppose";

export type MontanaIeOfficeExpectation =
  | { kind: "legislative_upper" | "legislative_lower" | "psc"; districtNumber: number | null }
  | { kind: "supreme_court" };

export type MontanaIeParseQuarantineReason =
  | "blank_target"
  | "attachment_reference"
  | "ballot_issue"
  | "multi_candidate"
  | "unsupported_office";

export type MontanaIeQuarantineReason =
  | MontanaIeParseQuarantineReason
  | "unresolved_name"
  | "ambiguous_name"
  | "office_conflict";

export type MontanaCandidateIssueParse =
  | { kind: "target"; stance: MontanaIeTargetStance; name: string; office: MontanaIeOfficeExpectation | null }
  | { kind: "quarantine"; reason: MontanaIeParseQuarantineReason };

const ATTACHMENT_PATTERN =
  /\b(?:see|se)[\s-]+(?:attach\w*|adden\w*|list\b|quantity\b)|\bquantity\s+(?:box|field)\b/i;
const BALLOT_ISSUE_PATTERN = /\bCI[\s-]?\d+\b|\blev(?:y|ies)\b|\bballot\b|\binitiative\b/i;
// Target office families CERS's even-year registration list cannot resolve
// (municipal races are odd-year, federal races are FEC): a name-only match
// against the wrong person is worse than a quarantine, so these fail closed.
const UNSUPPORTED_OFFICE_PATTERN =
  /\b(?:mayor|city\s+council|ward|congress\w*|president\w*|u\.?\s?s\.?\s+(?:senate|house|senator|representative))\b/i;
const STANCE_PREFIX_PATTERN = /^\s*(support(?:ing)?|oppos(?:e|ing))\b[\s:,-]*/i;
// A leading district token ("SD 14; Russ Tempel") with its separator.
const LEADING_OFFICE_TOKEN_PATTERN = /^\s*\(?\s*(SD|HD)\s*[-\s]?\s*0*(\d+)\s*\)?\s*[;:,.-]?\s+/i;
const PAREN_OFFICE_TOKEN_PATTERN = /\(\s*(SD|HD)\s*[-\s]?\s*0*(\d+)\s*\)/i;
const CHAMBER_DISTRICT_PATTERN = /\b(Senate|House)\s+District\s+(?:No\.?\s*)?0*(\d+)\b/i;
const INLINE_OFFICE_TOKEN_PATTERN = /\b(SD|HD)\s*[-\s]?\s*0*(\d+)\b/i;
const PSC_PATTERN =
  /\b(?:(?:Montana|MT)\s+)?(?:PSC|Public\s+Service\s+Commission(?:er)?)\b(?:\s+District)?\s*(?:No\.?\s*)?0*(\d+)?/i;
const SUPREME_COURT_PATTERN =
  /\b(?:(?:Montana|MT)\s+)?Supreme\s+Court(?:\s+(?:Chief\s+)?(?:Justice|Judge))?\b/i;
const CHAMBER_ONLY_PATTERN = /\bfor\s+(?:the\s+)?(?:Montana\s+|MT\s+)?(?:State\s+)?(Senate|House)\b/i;

function chamberKind(token: string): "legislative_upper" | "legislative_lower" {
  return token.toUpperCase().startsWith("S") ? "legislative_upper" : "legislative_lower";
}

function stripExtracted(text: string, match: RegExpExecArray): string {
  return `${text.slice(0, match.index)} ${text.slice(match.index + match[0].length)}`;
}

/** Trailing/leading connectives left behind once the office phrase is removed. */
function trimConnectives(value: string): string {
  return value
    .replace(/\s+/g, " ")
    .replace(/(?:\b(?:candidate\s+for|in\s+support\s+(?:of|for)?|for|of|in)\s*|[\s,;:/.-])+$/i, "")
    .replace(/^(?:\s*(?:candidate|the)\b\s*|[\s,;:/.-])+/i, "")
    .replace(/(?:'s)?\s+races?\.?$/i, "")
    .replace(/\b(?:19|20)\d{2}\s*$/, "")
    .trim();
}

function extractOffice(text: string): { office: MontanaIeOfficeExpectation | null; rest: string } {
  const paren = PAREN_OFFICE_TOKEN_PATTERN.exec(text);
  if (paren !== null) {
    return {
      office: { kind: chamberKind(paren[1]!), districtNumber: Number.parseInt(paren[2]!, 10) },
      rest: stripExtracted(text, paren),
    };
  }
  const chamberDistrict = CHAMBER_DISTRICT_PATTERN.exec(text);
  if (chamberDistrict !== null) {
    return {
      office: { kind: chamberKind(chamberDistrict[1]!), districtNumber: Number.parseInt(chamberDistrict[2]!, 10) },
      rest: stripExtracted(text, chamberDistrict),
    };
  }
  const psc = PSC_PATTERN.exec(text);
  if (psc !== null) {
    const districtNumber = psc[1] === undefined ? null : Number.parseInt(psc[1], 10);
    return { office: { kind: "psc", districtNumber }, rest: stripExtracted(text, psc) };
  }
  const supremeCourt = SUPREME_COURT_PATTERN.exec(text);
  if (supremeCourt !== null) {
    return { office: { kind: "supreme_court" }, rest: stripExtracted(text, supremeCourt) };
  }
  const inline = INLINE_OFFICE_TOKEN_PATTERN.exec(text);
  if (inline !== null) {
    return {
      office: { kind: chamberKind(inline[1]!), districtNumber: Number.parseInt(inline[2]!, 10) },
      rest: stripExtracted(text, inline),
    };
  }
  const chamberOnly = CHAMBER_ONLY_PATTERN.exec(text);
  if (chamberOnly !== null) {
    return {
      office: { kind: chamberKind(chamberOnly[1]!), districtNumber: null },
      rest: stripExtracted(text, chamberOnly),
    };
  }
  return { office: null, rest: text };
}

/**
 * Parses one candidateIssue value into a single stance-tagged target or a
 * quarantine class. Grammar pinned against the full 2025-26 live corpus
 * (1,380 in-window rows, 312 distinct values, 2026-08-28). Deliberately
 * single-target: any separator that could join multiple candidates
 * (comma, semicolon, and/or/&, a slash not introducing an office token)
 * quarantines as multi_candidate — CERS gives one amount per row with no
 * allocation, so splitting would be invention.
 */
export function parseMontanaCandidateIssue(value: string | null): MontanaCandidateIssueParse {
  const raw = (value ?? "").replace(/\s+/g, " ").trim();
  if (raw === "" || /^none$/i.test(raw)) {
    return { kind: "quarantine", reason: "blank_target" };
  }
  if (ATTACHMENT_PATTERN.test(raw)) {
    return { kind: "quarantine", reason: "attachment_reference" };
  }
  if (BALLOT_ISSUE_PATTERN.test(raw)) {
    return { kind: "quarantine", reason: "ballot_issue" };
  }
  if (UNSUPPORTED_OFFICE_PATTERN.test(raw)) {
    return { kind: "quarantine", reason: "unsupported_office" };
  }
  // A slash that merely introduces an office token ("Shelley Vance/SD 34")
  // is punctuation, not a separator; normalize it away before multi checks.
  let text = raw.replace(
    /\s*\/\s*(?=\(?\s*(?:SD|HD|PSC|Public\s+Service|Senate\s+District|House\s+District|(?:Montana\s+|MT\s+)?Supreme\s+Court)\b)/gi,
    " "
  );
  const leadingToken = LEADING_OFFICE_TOKEN_PATTERN.exec(text);
  let office: MontanaIeOfficeExpectation | null = null;
  if (leadingToken !== null) {
    office = { kind: chamberKind(leadingToken[1]!), districtNumber: Number.parseInt(leadingToken[2]!, 10) };
    text = text.slice(leadingToken[0].length);
  }
  if (/[,;&]|\s(?:and|or)\s|\//i.test(text)) {
    return { kind: "quarantine", reason: "multi_candidate" };
  }
  const stanceMatch = STANCE_PREFIX_PATTERN.exec(text);
  const stance: MontanaIeTargetStance =
    stanceMatch !== null && stanceMatch[1]!.toLowerCase().startsWith("o") ? "oppose" : "support";
  if (stanceMatch !== null) {
    text = text.slice(stanceMatch[0].length);
  }
  if (office === null) {
    const extracted = extractOffice(text);
    office = extracted.office;
    text = extracted.rest;
  }
  const name = trimConnectives(text);
  if (name === "") {
    return { kind: "quarantine", reason: "blank_target" };
  }
  // A stance verb SURVIVING inside the residual name means a second target
  // joined by punctuation the separator check does not treat as a list
  // ("Support SD 43 Bedey. Support HD 55 Barker") — one amount, several
  // candidates. Benign connectives ("in support for") are already trimmed.
  if (/\b(?:support|oppos)/i.test(name)) {
    return { kind: "quarantine", reason: "multi_candidate" };
  }
  return { kind: "target", stance, name, office };
}

function ieOfficeMatches(expectation: MontanaIeOfficeExpectation, officeTitle: string | null): boolean {
  const parsed = parseMontanaCersOfficeTitle(officeTitle);
  if (parsed === null || parsed.kind !== expectation.kind) {
    return false;
  }
  if (parsed.kind === "supreme_court" || expectation.kind === "supreme_court") {
    return true;
  }
  return expectation.districtNumber === null || parsed.districtNumber === expectation.districtNumber;
}

function registrationFullName(row: MontanaCersCandidateSearchRow): string {
  return [row.firstName, row.middleInitial, row.lastName].filter(Boolean).join(" ");
}

export type MontanaIeResolution =
  | { status: "resolved"; cersCandidateId: number; cersCandidateName: string }
  | { status: "quarantined"; reason: "unresolved_name" | "ambiguous_name" | "office_conflict" | "blank_target" };

// One-sided nickname expansion (the shared alaska/connecticut/illinois/texas
// pattern): the free-text ISSUE side expands ("Ken" reaches KENNETH), the
// registration side is keyed literally. The live corpus files nicknames
// constantly ("Oppose Ken Walsh" against "Walsh, Kenneth M").
function issueFirstNameMatchesRegistration(issueFirst: string, rowFirst: string): boolean {
  return issueFirst === rowFirst || firstNameVariants(issueFirst).includes(rowFirst);
}

// A registration whose campaign is closed out. CERS mints a NEW candidateId
// per race, so a race-switcher carries several same-year registrations
// (live corpus: George Nikolakakos holds SD-11 Closed, HD-22 Closed, and
// SD-12 Reopened for 2026) — but "Closed" also just means a finished
// campaign (primary losers close their registration), so liveness is only a
// TIE-BREAKER among multiple name matches, never a pre-filter.
function isLiveRegistrationStatus(status: string | null): boolean {
  return status !== "Closed" && status !== "Withdrawn";
}

/**
 * Stage-1 resolution: a parsed target against the year's registration list.
 * A multi-token name must fully align (middle-evidence gate, nickname-aware
 * on the issue side); a single token is an exact last-name key. Either way
 * the match must be UNIQUE within the office-constrained pool — with one
 * escape: when every extra match is a closed-out registration of the same
 * matched name and exactly one live registration remains, the live one wins
 * (the same row auto-link would bind the roster to).
 */
export function resolveMontanaIeTarget(
  target: { name: string; office: MontanaIeOfficeExpectation | null },
  registrationRows: readonly MontanaCersCandidateSearchRow[]
): MontanaIeResolution {
  const tokens = normalizeMontanaPersonNameForMatching(target.name).split(" ").filter(Boolean);
  if (tokens.length === 0) {
    return { status: "quarantined", reason: "blank_target" };
  }
  const matchesIn = (pool: readonly MontanaCersCandidateSearchRow[]): MontanaCersCandidateSearchRow[] => {
    const matched =
      tokens.length === 1
        ? pool.filter((row) => normalizeMontanaPersonNameForMatching(row.lastName) === tokens[0])
        : pool.filter((row) =>
            personNamesMatchWithMiddleEvidence({
              candidateName: target.name,
              rowNames: [registrationFullName(row)],
              normalizePersonName: normalizeMontanaPersonNameForMatching,
              firstNamesEquivalent: issueFirstNameMatchesRegistration,
            })
          );
    return [...new Map(matched.map((row) => [row.candidateId, row])).values()];
  };
  const officePool =
    target.office === null
      ? registrationRows
      : registrationRows.filter((row) => ieOfficeMatches(target.office!, row.officeTitle));
  let matches = matchesIn(officePool);
  if (matches.length > 1) {
    const live = matches.filter((row) => isLiveRegistrationStatus(row.candidateStatusDescr));
    if (live.length === 1) {
      matches = live;
    }
  }
  if (matches.length === 1) {
    const row = matches[0]!;
    return {
      status: "resolved",
      cersCandidateId: row.candidateId,
      cersCandidateName: registrationFullName(row),
    };
  }
  if (matches.length > 1) {
    return { status: "quarantined", reason: "ambiguous_name" };
  }
  // The name resolves but the filed office token disagrees (live corpus:
  // "KATHY LOVE (SD-9)" against her SD-43 registration): conflicting
  // evidence, never publish on it.
  if (target.office !== null && matchesIn(registrationRows).length > 0) {
    return { status: "quarantined", reason: "office_conflict" };
  }
  return { status: "quarantined", reason: "unresolved_name" };
}

export type MontanaIeRowExclusionReason =
  | "non_ie_transaction"
  | "electioneering"
  | "out_of_cycle"
  | "duplicate_trans_id"
  | "non_positive_amount";

export type MontanaIeClassifiedRow = {
  committeeId: number;
  committeeName: string;
  row: MontanaCersIeTransactionRow;
  /**
   * Amount to attribute for THIS entry. Present only on rows recovered from
   * a filed attachment, where one transaction expands into several
   * per-candidate entries; every consumer must prefer it over the
   * transaction total so the lump is never counted once per entry.
   */
  recoveredAmountCents?: number;
  /** Attachment id a recovered entry came from (audit trail). */
  recoveredFromAttachmentId?: number;
  outcome:
    | { kind: "excluded"; reason: MontanaIeRowExclusionReason }
    | { kind: "quarantined"; reason: MontanaIeQuarantineReason }
    | { kind: "resolved"; stance: MontanaIeTargetStance; cersCandidateId: number; cersCandidateName: string };
};

/** The dollars a classified entry contributes (recovered split, else the row). */
export function montanaIeClassifiedAmountCents(entry: MontanaIeClassifiedRow): number {
  return entry.recoveredAmountCents ?? entry.row.totalAmtCents;
}

export function montanaIeCycleWindow(electionYear: number): { startMs: number; endMs: number } {
  return {
    startMs: Date.UTC(electionYear - 1, 0, 1),
    endMs: Date.UTC(electionYear + 1, 0, 1),
  };
}

/**
 * Classifies every sweep row once: exclusion (structural), quarantine
 * (target cannot be published), or resolution to a CERS candidate. The
 * per-candidate aggregation and the quarantine report both consume this.
 */
export function classifyMontanaOutsideSpendingRows(input: {
  sweep: MontanaCersIeSweepArtifact;
  registrationRows: readonly MontanaCersCandidateSearchRow[];
  electionYear: number;
}): MontanaIeClassifiedRow[] {
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2020 || input.electionYear > 2100) {
    throw new Error(`Invalid Montana IE election year: ${input.electionYear}`);
  }
  const window = montanaIeCycleWindow(input.electionYear);
  const registrationRows = input.registrationRows.filter((row) => row.electionYear === input.electionYear);
  const seenTransIds = new Set<number>();
  const resolutionByIssue = new Map<string, MontanaIeClassifiedRow["outcome"]>();
  const classifyIssueText = (issue: string | null): MontanaIeClassifiedRow["outcome"] => {
    const parse = parseMontanaCandidateIssue(issue);
    if (parse.kind === "quarantine") {
      return { kind: "quarantined", reason: parse.reason };
    }
    const resolution = resolveMontanaIeTarget(parse, registrationRows);
    return resolution.status === "resolved"
      ? {
          kind: "resolved",
          stance: parse.stance,
          cersCandidateId: resolution.cersCandidateId,
          cersCandidateName: resolution.cersCandidateName,
        }
      : { kind: "quarantined", reason: resolution.reason };
  };
  const classified: MontanaIeClassifiedRow[] = [];
  for (const committee of input.sweep.committees) {
    const rows = input.sweep.transactionsByCommitteeId.get(committee.committeeId) ?? [];
    for (const row of rows) {
      const base = { committeeId: committee.committeeId, committeeName: committee.committeeName, row };
      const exclude = (reason: MontanaIeRowExclusionReason) =>
        classified.push({ ...base, outcome: { kind: "excluded", reason } });
      if (row.transTypeDescr !== "Independent Expenditure") {
        exclude("non_ie_transaction");
        continue;
      }
      if (row.electioneeringInd === "Y") {
        exclude("electioneering");
        continue;
      }
      if (row.datePaid < window.startMs || row.datePaid >= window.endMs) {
        exclude("out_of_cycle");
        continue;
      }
      if (seenTransIds.has(row.transId)) {
        exclude("duplicate_trans_id");
        continue;
      }
      seenTransIds.add(row.transId);
      if (row.totalAmtCents <= 0) {
        exclude("non_positive_amount");
        continue;
      }
      // Attachment recovery: a filed PDF breakdown replaces the lump row,
      // but only when it reconciles to that row's amount. Each entry is a
      // canonical candidateIssue string, so it goes through the SAME parse
      // + stance + resolution path as a filer-typed target.
      const recovery = montanaIeAttachmentRecoveryFor(row.transId);
      if (recovery !== null) {
        const recoveredTotal = recovery.entries.reduce((sum, entry) => sum + entry.amountCents, 0);
        if (Math.abs(recoveredTotal - row.totalAmtCents) <= MONTANA_IE_RECOVERY_TOLERANCE_CENTS) {
          for (const entry of recovery.entries) {
            classified.push({
              ...base,
              recoveredAmountCents: entry.amountCents,
              recoveredFromAttachmentId: recovery.attachmentId,
              outcome: classifyIssueText(entry.issue),
            });
          }
          continue;
        }
      }
      const issueKey = (row.candidateIssue ?? "").replace(/\s+/g, " ").trim();
      let outcome = resolutionByIssue.get(issueKey);
      if (outcome === undefined) {
        outcome = classifyIssueText(row.candidateIssue);
        resolutionByIssue.set(issueKey, outcome);
      }
      classified.push({ ...base, outcome });
    }
  }
  return classified;
}

const DEFAULT_MAX_OUTSIDE_GROUPS = 50;

function centsToDollars(cents: number): number {
  return cents / 100;
}

export type MontanaOutsideSpendingAggregationResult = {
  outsideGroups: MontanaFinanceOutsideGroupInput[];
  /** Null when no resolved rows carry the stance — never a false zero. */
  supportTotal: number | null;
  opposeTotal: number | null;
  attributedRowCount: number;
  attributedAmount: number;
};

/**
 * Aggregates the rows resolved to ONE CERS candidate into stance totals and
 * per-committee outside groups. Totals are null (not zero) when nothing
 * resolved to the candidate for that stance: with ~64% of IE dollars
 * structurally unattributable (attachments, blanks, multi-candidate rows),
 * absence of resolved rows is absence of disclosure, not a measured zero.
 */
export function aggregateMontanaOutsideSpendingForCandidate(input: {
  classifiedRows: readonly MontanaIeClassifiedRow[];
  cersCandidateId: number;
  sourceUrl?: string | null;
  maxGroups?: number;
}): MontanaOutsideSpendingAggregationResult {
  const maxGroups = input.maxGroups ?? DEFAULT_MAX_OUTSIDE_GROUPS;
  if (!Number.isSafeInteger(maxGroups) || maxGroups <= 0) {
    throw new Error(`Invalid Montana outside-spending maxGroups: ${input.maxGroups}`);
  }
  let supportCents = 0;
  let opposeCents = 0;
  let sawSupport = false;
  let sawOppose = false;
  let attributedRowCount = 0;
  let attributedCents = 0;
  const groups = new Map<
    string,
    { committeeId: number; committeeName: string; supportOppose: MontanaIeTargetStance; amountCents: number }
  >();
  for (const entry of input.classifiedRows) {
    if (entry.outcome.kind !== "resolved" || entry.outcome.cersCandidateId !== input.cersCandidateId) {
      continue;
    }
    const cents = montanaIeClassifiedAmountCents(entry);
    attributedRowCount += 1;
    attributedCents += cents;
    if (entry.outcome.stance === "support") {
      sawSupport = true;
      supportCents += cents;
    } else {
      sawOppose = true;
      opposeCents += cents;
    }
    const key = `${entry.committeeId}\u0000${entry.outcome.stance}`;
    const group = groups.get(key);
    if (group) {
      group.amountCents += cents;
    } else {
      groups.set(key, {
        committeeId: entry.committeeId,
        committeeName: entry.committeeName,
        supportOppose: entry.outcome.stance,
        amountCents: cents,
      });
    }
  }
  const outsideGroups = [...groups.values()]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.committeeName.localeCompare(right.committeeName) ||
        left.supportOppose.localeCompare(right.supportOppose)
    )
    .slice(0, maxGroups)
    .map((group) => ({
      committeeId: String(group.committeeId),
      committeeName: group.committeeName.trim(),
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: input.sourceUrl ?? null,
    }));
  return {
    outsideGroups,
    supportTotal: sawSupport ? centsToDollars(supportCents) : null,
    opposeTotal: sawOppose ? centsToDollars(opposeCents) : null,
    attributedRowCount,
    attributedAmount: centsToDollars(attributedCents),
  };
}

export type MontanaOutsideSpendingCommitteeSummary = {
  committeeId: number;
  committeeName: string;
  rowCount: number;
  totalAmount: number;
  resolvedRowCount: number;
  resolvedAmount: number;
  excludedAmountByReason: Partial<Record<MontanaIeRowExclusionReason, number>>;
  quarantinedAmountByReason: Partial<Record<MontanaIeQuarantineReason, number>>;
};

/**
 * Per-committee quarantine report (the plan's Phase 2b reporting deliverable
 * and the input for the attachment-recovery campaign): where the
 * unattributable dollars sit, by committee and reason.
 */
export function summarizeMontanaOutsideSpendingByCommittee(
  classifiedRows: readonly MontanaIeClassifiedRow[]
): MontanaOutsideSpendingCommitteeSummary[] {
  const byCommittee = new Map<number, MontanaOutsideSpendingCommitteeSummary & {
    resolvedCents: number;
    totalCents: number;
    excludedCents: Map<string, number>;
    quarantinedCents: Map<string, number>;
  }>();
  for (const entry of classifiedRows) {
    let summary = byCommittee.get(entry.committeeId);
    if (summary === undefined) {
      summary = {
        committeeId: entry.committeeId,
        committeeName: entry.committeeName,
        rowCount: 0,
        totalAmount: 0,
        resolvedRowCount: 0,
        resolvedAmount: 0,
        excludedAmountByReason: {},
        quarantinedAmountByReason: {},
        resolvedCents: 0,
        totalCents: 0,
        excludedCents: new Map(),
        quarantinedCents: new Map(),
      };
      byCommittee.set(entry.committeeId, summary);
    }
    const cents = montanaIeClassifiedAmountCents(entry);
    summary.rowCount += 1;
    summary.totalCents += cents;
    if (entry.outcome.kind === "resolved") {
      summary.resolvedRowCount += 1;
      summary.resolvedCents += cents;
    } else if (entry.outcome.kind === "quarantined") {
      summary.quarantinedCents.set(entry.outcome.reason, (summary.quarantinedCents.get(entry.outcome.reason) ?? 0) + cents);
    } else {
      summary.excludedCents.set(entry.outcome.reason, (summary.excludedCents.get(entry.outcome.reason) ?? 0) + cents);
    }
  }
  return [...byCommittee.values()]
    .sort((left, right) => right.totalCents - left.totalCents || left.committeeName.localeCompare(right.committeeName))
    .map(({ resolvedCents, totalCents, excludedCents, quarantinedCents, ...summary }) => ({
      ...summary,
      totalAmount: centsToDollars(totalCents),
      resolvedAmount: centsToDollars(resolvedCents),
      excludedAmountByReason: Object.fromEntries(
        [...excludedCents.entries()].map(([reason, cents]) => [reason, centsToDollars(cents)])
      ) as Partial<Record<MontanaIeRowExclusionReason, number>>,
      quarantinedAmountByReason: Object.fromEntries(
        [...quarantinedCents.entries()].map(([reason, cents]) => [reason, centsToDollars(cents)])
      ) as Partial<Record<MontanaIeQuarantineReason, number>>,
    }));
}

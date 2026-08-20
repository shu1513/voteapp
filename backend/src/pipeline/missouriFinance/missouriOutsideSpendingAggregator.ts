import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import {
  normalizeMissouriMecPoliticalDistrict,
  normalizeMissouriMecText,
} from "./missouriFinanceEligibleOffices.js";
import type {
  MissouriMecOutsideSpenderIdentity,
  MissouriMecOutsideSpendingRow,
} from "./missouriMecParsers.js";
import type { MissouriFinanceOutsideGroupInput } from "./missouriFinanceWriter.js";

export type MissouriOutsideSpendingAggregationResult = {
  outsideGroups: MissouriFinanceOutsideGroupInput[];
  supportTotal: number;
  opposeTotal: number;
  sourceRowCount: number;
  candidateNameRowCount: number;
  candidateOfficeMismatchRowCount: number;
  candidateOfficeMismatchAmount: number;
  outOfCycleRowCount: number;
  outOfCycleAmount: number;
  unresolvedSpenderRowCount: number;
  unresolvedSpenderAmount: number;
  ambiguousLineageRowCount: number;
  ambiguousLineageAmount: number;
  ambiguousTimelyLineageRowCount: number;
  malformedAmountRowCount: number;
  malformedAmount: number;
  attributedRowCount: number;
  attributedAmount: number;
};

const DEFAULT_MAX_GROUPS = 50;

function centsToDollars(cents: number): number {
  return cents / 100;
}

function normalizeMaxGroups(value: number | undefined): number {
  const normalized = value ?? DEFAULT_MAX_GROUPS;
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Missouri outside-spending maxGroups: ${value}`);
  }
  return normalized;
}

function normalizeIsoDate(value: string, label: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw new Error(`Invalid Missouri outside-spending ${label}: ${value}`);
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== value) {
    throw new Error(`Invalid Missouri outside-spending ${label}: ${value}`);
  }
  return value;
}

function candidateNameMatches(candidateName: string, sourceNameAndAddress: string): boolean {
  const candidateTokens = normalizeMissouriMecText(candidateName).split(" ").filter(Boolean);
  const sourceTokens = normalizeMissouriMecText(sourceNameAndAddress).split(" ").filter(Boolean);
  if (candidateTokens.length < 2 || sourceTokens.length < candidateTokens.length) return false;

  // MEC concatenates candidate name and street address without a delimiter.
  // A numeric address token is the only source-proven boundary. Try the
  // bounded name prefixes before it; never scan the address for a fuzzy hit.
  const firstNumeric = sourceTokens.findIndex((token) => /\d/.test(token));
  const upperBound = Math.min(
    firstNumeric >= 2 ? firstNumeric : candidateTokens.length + 2,
    candidateTokens.length + 2,
    sourceTokens.length
  );
  const rowNames: string[] = [];
  for (let length = 2; length <= upperBound; length += 1) {
    rowNames.push(sourceTokens.slice(0, length).join(" "));
  }
  return personNamesMatchWithMiddleEvidence({
    candidateName,
    rowNames,
    normalizePersonName: normalizeMissouriMecText,
  });
}

function expectedOfficeTerms(officeName: string): readonly string[] {
  switch (officeName) {
    case "State Auditor": return ["STATE AUDITOR", "MISSOURI AUDITOR"];
    case "Collector of Revenue": return ["COLLECTOR OF REVENUE", "COUNTY COLLECTOR"];
    case "County Assessor": return ["ASSESSOR"];
    case "County Auditor": return ["AUDITOR"];
    case "County Clerk": return ["COUNTY CLERK"];
    case "County Clerk and Recorder": return ["CIRCUIT CLERK AND RECORDER OF DEEDS", "CIRCUIT CLERK"];
    case "County Commissioner": return ["COMMISSIONER", "COMMISIONER", "COMMISSIER"];
    case "County Executive": return ["COUNTY EXECUTIVE", "COUNTY EXEC"];
    case "County Level Judge": return ["CIRCUIT JUDGE"];
    case "County Recorder":
    case "Recorder of Deeds": return ["RECORDER OF DEEDS", "COUNTY RECORDER"];
    case "County Supervisor": return ["COUNTY COUNCIL", "COUNTY LEGISLATURE", "COUNTY LEGISLATOR"];
    case "County Treasurer": return ["TREASURER"];
    case "District Attorney": return ["PROSECUTING ATTORNEY", "PROSECUTOR"];
    case "License Collector": return ["LICENSE COLLECTOR"];
    case "Sheriff": return ["SHERIFF"];
    case "City Council Member": return ["COUNCIL", "ALDERMAN", "ALDERPERSON", "ALDERWOMAN"];
    case "City Treasurer": return ["TREASURER"];
    case "Mayor": return ["MAYOR"];
    case "Municipal Assessor": return ["ASSESSOR"];
    case "Place Level Judge": return ["MUNICIPAL JUDGE"];
    case "School Board Member": return ["SCHOOL BOARD", "BOARDMEMBER", "BOARD MEMBER", "SCHOOL DIRECTOR"];
    default: return [];
  }
}

function legislativeFamilyMatches(officeName: string, source: string): boolean {
  if (/\b(?:US|U S|CONGRESS|CONGRESSIONAL)\b/.test(source)) return false;
  if (officeName === "State Lower Chamber Legislator") {
    return /\bSTATE REP(?:RESENTATIVE)?\b|\bMISSOURI HOUSE\b|\bMO HOUSE\b|\bMO STATE (?:HOUSE|REP)\b|\bST REP\b|\bHD ?\d+\b|^HOUSE ?\d|^HOUSE (?:OF )?REP(?:RESENTATIVE|RESENTATIVES)?\b|^MO \d+ REPRESENTATIVE\b/.test(source);
  }
  if (officeName === "State Senator") {
    return /\bSTATE SENAT(?:E|OR)\b|\bMISSOURI SENATE\b|\bMO SENATE\b|\bST SENATE\b|\bSD ?\d+\b|^SENATE(?: |$)/.test(source);
  }
  return false;
}

function sourceLegislativeDistrict(officeName: string, source: string): string | null {
  const generic = normalizeMissouriMecPoliticalDistrict(source);
  if (generic) return generic;
  const patterns = officeName === "State Lower Chamber Legislator"
    ? [
        /\bHD ?0*(\d+)\b/,
        /\bHOUSE ?0*(\d+)\b/,
        /\bMO 0*(\d+) REPRESENTATIVE\b/,
        /\bREPRESENTATIVES? 0*(\d+)(?:ST|ND|RD|TH)?\b/,
      ]
    : [
        /\bSD ?0*(\d+)\b/,
        /\bSENATE 0*(\d+)\b/,
        /\bSENATOR 0*(\d+) DISTRICT\b/,
      ];
  for (const pattern of patterns) {
    const match = pattern.exec(source);
    if (match?.[1]) return `DISTRICT ${Number.parseInt(match[1], 10)}`;
  }
  return null;
}

function officeMatches(input: { officeName: string; district: string | null | undefined; sourceOffice: string }): boolean {
  const source = normalizeMissouriMecText(input.sourceOffice);
  const isLegislative = input.officeName === "State Lower Chamber Legislator" || input.officeName === "State Senator";
  if (isLegislative) {
    if (!legislativeFamilyMatches(input.officeName, source)) return false;
  } else {
    const terms = expectedOfficeTerms(input.officeName);
    if (terms.length === 0 || !terms.some((term) => source.includes(term))) return false;
  }
  const expectedDistrict = normalizeMissouriMecPoliticalDistrict(`District ${input.district ?? ""}`);
  const sourceDistrict = isLegislative
    ? sourceLegislativeDistrict(input.officeName, source)
    : normalizeMissouriMecPoliticalDistrict(source);
  return expectedDistrict === null || sourceDistrict === null || expectedDistrict === sourceDistrict;
}

function isTimelyReport(report: string): boolean {
  return /\b24[ -]?HOUR EXPENDITURE REPORT\b/i.test(report);
}

function collisionKey(row: MissouriMecOutsideSpendingRow, mecid: string): string {
  return [
    mecid,
    normalizeMissouriMecText(row.candidateNameAndAddress),
    row.expenditureDate,
    row.amountCents,
  ].join("\u0000");
}

export function aggregateMissouriOutsideSpending(input: {
  rows: readonly MissouriMecOutsideSpendingRow[];
  identities: readonly MissouriMecOutsideSpenderIdentity[];
  candidateName: string;
  officeName: string;
  district?: string | null;
  cycleStart: string;
  cycleEnd: string;
  sourceUrl?: string | null;
  maxGroups?: number;
}): MissouriOutsideSpendingAggregationResult {
  const maxGroups = normalizeMaxGroups(input.maxGroups);
  const cycleStart = normalizeIsoDate(input.cycleStart, "cycle start");
  const cycleEnd = normalizeIsoDate(input.cycleEnd, "cycle end");
  if (cycleStart > cycleEnd) throw new Error("Missouri outside-spending cycle start is after cycle end");
  const identityByName = new Map(input.identities.map((row) => [row.reportingCommittee, row.mecid]));
  if (identityByName.size !== input.identities.length) {
    throw new Error("Missouri outside-spending identities contain duplicate committee names");
  }

  let candidateNameRowCount = 0;
  let candidateOfficeMismatchRowCount = 0;
  let candidateOfficeMismatchCents = 0;
  let outOfCycleRowCount = 0;
  let outOfCycleCents = 0;
  let unresolvedSpenderRowCount = 0;
  let unresolvedSpenderCents = 0;
  let malformedAmountRowCount = 0;
  let malformedAmountCents = 0;
  const candidates: Array<{ row: MissouriMecOutsideSpendingRow; mecid: string }> = [];
  for (const row of input.rows) {
    if (!candidateNameMatches(input.candidateName, row.candidateNameAndAddress)) continue;
    candidateNameRowCount += 1;
    if (!officeMatches({ officeName: input.officeName, district: input.district, sourceOffice: row.officeSought })) {
      candidateOfficeMismatchRowCount += 1;
      candidateOfficeMismatchCents += row.amountCents;
      continue;
    }
    if (row.expenditureDate < cycleStart || row.expenditureDate > cycleEnd) {
      outOfCycleRowCount += 1;
      outOfCycleCents += row.amountCents;
      continue;
    }
    const mecid = identityByName.get(row.reportingCommittee);
    if (!mecid) {
      unresolvedSpenderRowCount += 1;
      unresolvedSpenderCents += row.amountCents;
      continue;
    }
    if (row.amountCents <= 0) {
      malformedAmountRowCount += 1;
      malformedAmountCents += row.amountCents;
      continue;
    }
    candidates.push({ row, mecid });
  }

  const collisions = new Map<string, Array<{ row: MissouriMecOutsideSpendingRow; mecid: string }>>();
  for (const candidate of candidates) {
    const key = collisionKey(candidate.row, candidate.mecid);
    const list = collisions.get(key) ?? [];
    list.push(candidate);
    collisions.set(key, list);
  }
  const ambiguousKeys = new Set<string>();
  let ambiguousLineageRowCount = 0;
  let ambiguousLineageCents = 0;
  let ambiguousTimelyLineageRowCount = 0;
  for (const [key, cluster] of collisions) {
    if (new Set(cluster.map(({ row }) => normalizeMissouriMecText(row.report))).size <= 1) continue;
    ambiguousKeys.add(key);
    ambiguousLineageRowCount += cluster.length;
    ambiguousLineageCents += cluster.reduce((sum, { row }) => sum + row.amountCents, 0);
    if (cluster.some(({ row }) => isTimelyReport(row.report))) ambiguousTimelyLineageRowCount += cluster.length;
  }

  const groups = new Map<string, { committeeId: string; committeeName: string; supportOppose: "support" | "oppose"; amountCents: number }>();
  let supportCents = 0;
  let opposeCents = 0;
  let attributedRowCount = 0;
  let attributedCents = 0;
  for (const candidate of candidates) {
    if (ambiguousKeys.has(collisionKey(candidate.row, candidate.mecid))) continue;
    const stance = candidate.row.supportOppose.toLowerCase() as "support" | "oppose";
    attributedRowCount += 1;
    attributedCents += candidate.row.amountCents;
    if (stance === "support") supportCents += candidate.row.amountCents;
    else opposeCents += candidate.row.amountCents;
    const key = `${candidate.mecid}\u0000${stance}`;
    const existing = groups.get(key);
    if (existing) existing.amountCents += candidate.row.amountCents;
    else groups.set(key, {
      committeeId: candidate.mecid,
      committeeName: candidate.row.reportingCommittee,
      supportOppose: stance,
      amountCents: candidate.row.amountCents,
    });
  }
  const outsideGroups = [...groups.values()]
    .sort((left, right) => right.amountCents - left.amountCents || left.committeeName.localeCompare(right.committeeName) || left.supportOppose.localeCompare(right.supportOppose))
    .slice(0, maxGroups)
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: input.sourceUrl ?? null,
    }));

  return {
    outsideGroups,
    supportTotal: centsToDollars(supportCents),
    opposeTotal: centsToDollars(opposeCents),
    sourceRowCount: input.rows.length,
    candidateNameRowCount,
    candidateOfficeMismatchRowCount,
    candidateOfficeMismatchAmount: centsToDollars(candidateOfficeMismatchCents),
    outOfCycleRowCount,
    outOfCycleAmount: centsToDollars(outOfCycleCents),
    unresolvedSpenderRowCount,
    unresolvedSpenderAmount: centsToDollars(unresolvedSpenderCents),
    ambiguousLineageRowCount,
    ambiguousLineageAmount: centsToDollars(ambiguousLineageCents),
    ambiguousTimelyLineageRowCount,
    malformedAmountRowCount,
    malformedAmount: centsToDollars(malformedAmountCents),
    attributedRowCount,
    attributedAmount: centsToDollars(attributedCents),
  };
}

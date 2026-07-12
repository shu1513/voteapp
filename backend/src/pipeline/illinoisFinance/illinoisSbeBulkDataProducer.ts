import { readFile } from "node:fs/promises";

import { mapIllinoisSbeOffice, normalizeIllinoisSbeLocalDistrictType } from "./illinoisFinanceEligibleOffices.js";
import {
  ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION,
  parseIllinoisSbeNormalizedArtifact,
  type IllinoisSbeCandidateCommitteeRelation,
  type IllinoisSbeD2ReportSummary,
  type IllinoisSbeNormalizedArtifact,
} from "./illinoisSbeNormalizedArtifact.js";

export const ILLINOIS_SBE_BULK_DOWNLOAD_URL =
  "https://www.elections.il.gov/CampaignDisclosure/DownloadCDDataFiles.aspx";
const ILLINOIS_SBE_TIME_ZONE = "America/Chicago";
const ILLINOIS_SBE_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: ILLINOIS_SBE_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const HEADERS = {
  candidates: ["ID", "LastName", "FirstName", "Address1", "Address2", "City", "State", "Zip", "Office", "DistrictType", "District", "ResidenceCounty", "PartyAffiliation", "RedactionRequested"],
  candidateElections: ["ID", "CandidateID", "ElectionType", "ElectionYear", "IncChallOpen", "WonLost", "FairCampaign", "LimitsOff", "LimitsOffReason"],
  committeeCandidateLinks: ["ID", "CommitteeID", "CandidateID"],
  committees: ["ID", "TypeOfCommittee", "StateCommittee", "StateID", "LocalCommittee", "LocalID", "ReferName", "Name", "Address1", "Address2", "Address3", "City", "State", "Zip", "Status", "StatusDate", "CreationDate", "CreationAmount", "DispFundsReturn", "DispFundsPolComm", "DispFundsCharity", "DispFunds95", "DispFundsDescrip", "CanSuppOpp", "PolicySuppOpp", "PartyAffiliation", "Purpose"],
  filedDocuments: ["ID", "CommitteeID", "FiledDocType", "DocName", "Amend", "Comment", "Pages", "ElectionType", "ElectionYear", "RptPdBegDate", "RptPdEndDate", "RcvdAt", "RcvdDateTime", "Source", "Provider", "SignerLastOnlyName", "SignerFirstName", "SbmttrLastOnlyName", "SbmttrFirstName", "SbmttrAddress1", "SbmttrAddress2", "SbmttrCity", "SbmttrState", "SbmttrZip", "B9SignerLastOnlyName", "B9SignerFirstName", "Archived", "Clarification", "RedactionRequested"],
  d2Totals: ["ID", "CommitteeID", "FiledDocID", "BegFundsAvail", "IndivContribI", "IndivContribNI", "XferInI", "XferInNI", "LoanRcvI", "LoanRcvNI", "OtherRctI", "OtherRctNI", "TotalReceipts", "InKindI", "InKindNI", "TotalInKind", "XferOutI", "XferOutNI", "LoanMadeI", "LoanMadeNI", "ExpendI", "ExpendNI", "IndependentExpI", "IndependentExpNI", "TotalExpend", "DebtsI", "DebtsNI", "TotalDebts", "TotalInvest", "EndFundsAvail", "Archived"],
} as const;

export type IllinoisSbeBulkDataPaths = {
  candidates: string;
  candidateElections: string;
  committeeCandidateLinks: string;
  committees: string;
  filedDocuments: string;
  d2Totals: string;
};

export type IllinoisSbeBulkProducerStats = {
  eligibleCandidates: number;
  rejectedCandidates: number;
  candidatesWithoutElection: number;
  candidatesWithoutCommittee: number;
  candidateCommitteeRelations: number;
  d2ReportSummaries: number;
  d2RowsWithoutUsableDocument: number;
};

type Candidate = {
  id: string;
  name: string;
  districtType: string;
  district: string;
  office: string;
  isAtLarge: boolean;
};

type Committee = {
  id: string;
  name: string;
  status: IllinoisSbeCandidateCommitteeRelation["committeeStatus"];
};

type FiledDocument = {
  committeeId: string;
  periodStart: string;
  periodEnd: string;
  filedAt: string;
};

function clean(value: string): string {
  return value.replace(/\0/g, "").trim().replace(/\s+/g, " ");
}

function parseTsv(text: string, label: string, expectedHeader: readonly string[], visit: (row: string[]) => void): void {
  const source = text.replace(/^\uFEFF/, "");
  if (!source.endsWith("\n")) {
    throw new Error(`Illinois SBE ${label} file is incomplete: final newline is missing`);
  }

  let row: string[] = [];
  let field = "";
  let quoted = false;
  let line = 1;
  let headerSeen = false;

  const finishRow = () => {
    row.push(field);
    field = "";
    if (!headerSeen) {
      if (row.length !== expectedHeader.length || row.some((value, index) => value !== expectedHeader[index])) {
        throw new Error(`Illinois SBE ${label} header does not match the published schema`);
      }
      headerSeen = true;
    } else if (row.some((value) => value.length > 0)) {
      if (row.length !== expectedHeader.length) {
        throw new Error(
          `Illinois SBE ${label} row ${line} has ${row.length} columns; expected ${expectedHeader.length}`
        );
      }
      visit(row);
    }
    row = [];
    line += 1;
  };

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index]!;
    if (quoted) {
      if (character === '"') {
        if (source[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          quoted = false;
        }
      } else {
        field += character;
      }
    } else if (character === '"' && field.length === 0) {
      quoted = true;
    } else if (character === "\t") {
      row.push(field);
      field = "";
    } else if (character === "\n") {
      if (field.endsWith("\r")) field = field.slice(0, -1);
      finishRow();
    } else {
      field += character;
    }
  }
  if (quoted) throw new Error(`Illinois SBE ${label} file is incomplete: unterminated quoted field`);
  if (!headerSeen) throw new Error(`Illinois SBE ${label} file has no header`);
}

async function readTsv(
  path: string,
  label: string,
  expectedHeader: readonly string[],
  visit: (row: string[]) => void
): Promise<void> {
  parseTsv(await readFile(path, "utf8"), label, expectedHeader, visit);
}

function parseElectionYear(value: string): number | null {
  const year = Number.parseInt(clean(value), 10);
  return Number.isInteger(year) && year >= 2000 && year <= 2100 ? year : null;
}

function parseAmount(value: string, field: string): number | null {
  const normalized = clean(value).replace(/[$,]/g, "");
  if (!normalized) return null;
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) throw new Error(`Illinois SBE D2Totals.txt has invalid ${field}: ${value}`);
  return Math.round(amount * 100) / 100;
}

function parseSbeDate(value: string): string | null {
  const match = clean(value).match(/^(\d{4}-\d{2}-\d{2})/);
  return match?.[1] ?? null;
}

function zonedDateTimeParts(timestamp: number): Record<string, number> {
  return Object.fromEntries(
    ILLINOIS_SBE_DATE_TIME_FORMATTER.formatToParts(new Date(timestamp))
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, Number.parseInt(part.value, 10)])
  );
}

function parseSbeTimestamp(value: string): string | null {
  const match = clean(value).match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})$/);
  if (!match) return null;
  const components = [...match[1].split("-"), ...match[2].split(":")].map((part) => Number.parseInt(part, 10));
  const [year, month, day, hour, minute, second] = components;
  if ([year, month, day, hour, minute, second].some((part) => !Number.isInteger(part))) return null;

  const localAsUtc = Date.UTC(year!, month! - 1, day!, hour!, minute!, second!);
  let timestamp = localAsUtc;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const parts = zonedDateTimeParts(timestamp);
    const representedLocalTime = Date.UTC(
      parts.year!,
      parts.month! - 1,
      parts.day!,
      parts.hour!,
      parts.minute!,
      parts.second!
    );
    const next = localAsUtc - (representedLocalTime - timestamp);
    if (next === timestamp) break;
    timestamp = next;
  }
  const resolved = zonedDateTimeParts(timestamp);
  if (
    resolved.year !== year ||
    resolved.month !== month ||
    resolved.day !== day ||
    resolved.hour !== hour ||
    resolved.minute !== minute ||
    resolved.second !== second
  ) {
    return null;
  }
  return new Date(timestamp).toISOString();
}

function committeeStatus(value: string): Committee["status"] {
  switch (clean(value).toUpperCase()) {
    case "A": return "active";
    case "F": return "final";
    case "I": return "inactive";
    default: return "unknown";
  }
}

export async function produceIllinoisSbeNormalizedArtifact(input: {
  paths: IllinoisSbeBulkDataPaths;
  acquiredAt: string;
  sourceUrl?: string;
}): Promise<{ artifact: IllinoisSbeNormalizedArtifact; stats: IllinoisSbeBulkProducerStats }> {
  const sourceUrl = input.sourceUrl ?? ILLINOIS_SBE_BULK_DOWNLOAD_URL;
  const candidates = new Map<string, Candidate>();
  let rejectedCandidates = 0;

  await readTsv(input.paths.candidates, "Candidates.txt", HEADERS.candidates, (row) => {
    const districtType = clean(row[9]!);
    const localDistrictType = normalizeIllinoisSbeLocalDistrictType(districtType);
    const office = clean(row[8]!);
    const isAtLarge = localDistrictType !== null;
    const mapping = mapIllinoisSbeOffice({ office, district: clean(row[10]!), districtType, isAtLarge });
    if (!mapping) {
      rejectedCandidates += 1;
      return;
    }
    const id = clean(row[0]!);
    const name = clean(`${row[2]} ${row[1]}`);
    if (!id || !name) throw new Error("Illinois SBE Candidates.txt contains an eligible candidate without an ID or name");
    candidates.set(id, {
      id,
      name,
      districtType: mapping.sbeDistrictType ?? mapping.officeScope,
      district: mapping.district ?? "Illinois",
      office: mapping.sbeOffice,
      isAtLarge: mapping.requiresAtLargeEvidence === true,
    });
  });

  const electionYears = new Map<string, Set<number>>();
  await readTsv(input.paths.candidateElections, "CanElections.txt", HEADERS.candidateElections, (row) => {
    const candidateId = clean(row[1]!);
    const year = parseElectionYear(row[3]!);
    if (!candidates.has(candidateId) || year === null) return;
    const years = electionYears.get(candidateId) ?? new Set<number>();
    years.add(year);
    electionYears.set(candidateId, years);
  });

  const committeeIdsByCandidate = new Map<string, Set<string>>();
  const referencedCommitteeIds = new Set<string>();
  await readTsv(input.paths.committeeCandidateLinks, "CmteCandidateLinks.txt", HEADERS.committeeCandidateLinks, (row) => {
    const candidateId = clean(row[2]!);
    const committeeId = clean(row[1]!);
    if (!candidates.has(candidateId) || !committeeId) return;
    const committeeIds = committeeIdsByCandidate.get(candidateId) ?? new Set<string>();
    committeeIds.add(committeeId);
    committeeIdsByCandidate.set(candidateId, committeeIds);
    referencedCommitteeIds.add(committeeId);
  });

  const committees = new Map<string, Committee>();
  await readTsv(input.paths.committees, "Committees.txt", HEADERS.committees, (row) => {
    const id = clean(row[0]!);
    if (!referencedCommitteeIds.has(id)) return;
    const name = clean(row[7]!);
    if (!name) throw new Error(`Illinois SBE committee ${id} has no name`);
    committees.set(id, { id, name, status: committeeStatus(row[14]!) });
  });
  for (const id of referencedCommitteeIds) {
    if (!committees.has(id)) throw new Error(`Illinois SBE committee link references missing committee ${id}`);
  }

  const filedDocuments = new Map<string, FiledDocument>();
  await readTsv(input.paths.filedDocuments, "FiledDocs.txt", HEADERS.filedDocuments, (row) => {
    const id = clean(row[0]!);
    const committeeId = clean(row[1]!);
    if (!referencedCommitteeIds.has(committeeId)) return;
    const periodStart = parseSbeDate(row[9]!);
    const periodEnd = parseSbeDate(row[10]!);
    const filedAt = parseSbeTimestamp(row[12]!);
    if (id && periodStart && periodEnd && filedAt) {
      filedDocuments.set(id, { committeeId, periodStart, periodEnd, filedAt });
    }
  });

  const d2ReportSummaries: IllinoisSbeD2ReportSummary[] = [];
  let d2RowsWithoutUsableDocument = 0;
  await readTsv(input.paths.d2Totals, "D2Totals.txt", HEADERS.d2Totals, (row) => {
    const committeeId = clean(row[1]!);
    if (!referencedCommitteeIds.has(committeeId)) return;
    const filedDocument = filedDocuments.get(clean(row[2]!));
    if (!filedDocument || filedDocument.committeeId !== committeeId) {
      d2RowsWithoutUsableDocument += 1;
      return;
    }
    d2ReportSummaries.push({
      reportId: clean(row[2]!),
      committeeId,
      periodStart: filedDocument.periodStart,
      periodEnd: filedDocument.periodEnd,
      filedAt: filedDocument.filedAt,
      totalReceipts: parseAmount(row[12]!, "TotalReceipts"),
      totalDisbursements: parseAmount(row[24]!, "TotalExpend"),
      cashOnHand: parseAmount(row[29]!, "EndFundsAvail"),
      debtsOwed: parseAmount(row[27]!, "TotalDebts"),
      sourceUrl,
    });
  });

  const candidateCommitteeRelations: IllinoisSbeCandidateCommitteeRelation[] = [];
  let candidatesWithoutElection = 0;
  let candidatesWithoutCommittee = 0;
  for (const candidate of candidates.values()) {
    const years = electionYears.get(candidate.id);
    const committeeIds = committeeIdsByCandidate.get(candidate.id);
    if (!years?.size) candidatesWithoutElection += 1;
    if (!committeeIds?.size) candidatesWithoutCommittee += 1;
    if (!years?.size || !committeeIds?.size) continue;
    for (const electionYear of years) {
      for (const committeeId of committeeIds) {
        const committee = committees.get(committeeId)!;
        candidateCommitteeRelations.push({
          candidateId: candidate.id,
          candidateName: candidate.name,
          electionYear,
          districtType: candidate.districtType,
          district: candidate.district,
          office: candidate.office,
          isAtLarge: candidate.isAtLarge,
          committeeId,
          committeeName: committee.name,
          committeeStatus: committee.status,
          sourceUrl,
        });
      }
    }
  }

  candidateCommitteeRelations.sort((a, b) =>
    [a.candidateId, String(a.electionYear), a.committeeId].join("\0").localeCompare(
      [b.candidateId, String(b.electionYear), b.committeeId].join("\0")
    )
  );
  d2ReportSummaries.sort((a, b) => a.reportId.localeCompare(b.reportId));

  const artifact = parseIllinoisSbeNormalizedArtifact(JSON.stringify({
    schemaVersion: ILLINOIS_SBE_NORMALIZED_ARTIFACT_SCHEMA_VERSION,
    complete: true,
    source: "illinois_sbe",
    acquiredAt: input.acquiredAt,
    sourceUrl,
    candidateCommitteeRelations,
    d2ReportSummaries,
  }));
  return {
    artifact,
    stats: {
      eligibleCandidates: candidates.size,
      rejectedCandidates,
      candidatesWithoutElection,
      candidatesWithoutCommittee,
      candidateCommitteeRelations: candidateCommitteeRelations.length,
      d2ReportSummaries: d2ReportSummaries.length,
      d2RowsWithoutUsableDocument,
    },
  };
}

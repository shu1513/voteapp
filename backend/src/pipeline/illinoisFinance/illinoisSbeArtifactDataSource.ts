import { readFile } from "node:fs/promises";

import {
  normalizeIllinoisCandidateNameKeys,
  resolveIllinoisCandidateCommittee,
  resolveIllinoisCandidateCommitteesFromRelations,
} from "./illinoisCandidateCommitteeResolver.js";
import type { IllinoisCandidateCommitteeResolver } from "./illinoisCandidateFinanceAutoLink.js";
import type {
  IllinoisCandidateFinanceData,
  IllinoisCandidateFinanceDueRow,
} from "./illinoisCandidateFinanceBatchSync.js";
import {
  isIllinoisFinanceCycleDate,
  normalizeIllinoisCommitteeKey,
  normalizeIllinoisFinanceTextKey,
} from "./illinoisFinanceAggregators.js";
import {
  illinoisMunicipalityMatches,
  toIllinoisSbeOfficeSearchInput,
} from "./illinoisFinanceEligibleOffices.js";
import {
  ILLINOIS_SBE_CONTRIBUTION_COMMITTEE_SEARCH_URL,
  ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL,
} from "./illinoisSbeClient.js";
import {
  parseIllinoisSbeContributionRecordsCsv,
  parseIllinoisSbeExpenditureRecordsCsv,
  type IllinoisSbeContributionRecord,
  type IllinoisSbeExpenditureRecord,
} from "./illinoisSbeCsvReader.js";
import {
  loadIllinoisSbeNormalizedArtifact,
  type IllinoisSbeNormalizedArtifact,
} from "./illinoisSbeNormalizedArtifact.js";

export type IllinoisSbeArtifactDataSet = {
  contributionRecords: IllinoisSbeContributionRecord[];
  expenditureRecords?: IllinoisSbeExpenditureRecord[];
  contributionSourceUrl: string;
  expenditureSourceUrl?: string | null;
  normalizedArtifact?: IllinoisSbeNormalizedArtifact;
};

export type IllinoisSbeArtifactDataSourceConfig = {
  contributionCsvPaths: readonly string[];
  expenditureCsvPaths?: readonly string[];
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  normalizedArtifactPath?: string | null;
};

function normalizePathList(paths: readonly string[] | undefined, label: string, required: boolean): string[] {
  const normalized = (paths ?? []).map((path) => path.trim()).filter(Boolean);
  if (required && normalized.length === 0) {
    throw new Error(`Illinois SBE ${label} artifact requires at least one CSV path`);
  }
  return normalized;
}

async function loadContributionRecords(input: {
  paths: readonly string[];
  sourceUrl: string;
}): Promise<IllinoisSbeContributionRecord[]> {
  const records: IllinoisSbeContributionRecord[] = [];
  for (const path of input.paths) {
    // The current Illinois parser materializes each CSV, so keep reads sequential to cap peak memory.
    const csv = await readFile(path, "utf8");
    records.push(...parseIllinoisSbeContributionRecordsCsv(csv, input.sourceUrl));
  }
  return records;
}

async function loadExpenditureRecords(input: {
  paths: readonly string[];
  sourceUrl: string;
}): Promise<IllinoisSbeExpenditureRecord[]> {
  const records: IllinoisSbeExpenditureRecord[] = [];
  for (const path of input.paths) {
    // The current Illinois parser materializes each CSV, so keep reads sequential to cap peak memory.
    const csv = await readFile(path, "utf8");
    records.push(...parseIllinoisSbeExpenditureRecordsCsv(csv, input.sourceUrl));
  }
  return records;
}

export async function loadIllinoisSbeArtifactDataSet(
  config: IllinoisSbeArtifactDataSourceConfig
): Promise<IllinoisSbeArtifactDataSet> {
  const normalizedArtifactPath = config.normalizedArtifactPath?.trim() || null;
  const contributionCsvPaths = normalizePathList(
    config.contributionCsvPaths,
    "contribution",
    normalizedArtifactPath === null
  );
  const expenditureCsvPaths = normalizePathList(config.expenditureCsvPaths, "expenditure", false);
  const contributionSourceUrl = config.contributionSourceUrl?.trim() || ILLINOIS_SBE_CONTRIBUTION_COMMITTEE_SEARCH_URL;
  const expenditureSourceUrl = config.expenditureSourceUrl?.trim() || ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL;
  const dataSet: IllinoisSbeArtifactDataSet = {
    contributionRecords: await loadContributionRecords({
      paths: contributionCsvPaths,
      sourceUrl: contributionSourceUrl,
    }),
    contributionSourceUrl,
  };

  if (normalizedArtifactPath) {
    dataSet.normalizedArtifact = await loadIllinoisSbeNormalizedArtifact(normalizedArtifactPath);
  }

  if (expenditureCsvPaths.length > 0) {
    dataSet.expenditureRecords = await loadExpenditureRecords({
      paths: expenditureCsvPaths,
      sourceUrl: expenditureSourceUrl,
    });
    dataSet.expenditureSourceUrl = expenditureSourceUrl;
  }

  return dataSet;
}

function directContributionMatchesDueRow(input: {
  record: IllinoisSbeContributionRecord;
  row: IllinoisCandidateFinanceDueRow;
}): boolean {
  return (
    normalizeIllinoisCommitteeKey(input.record.recipientCommitteeName) ===
    normalizeIllinoisCommitteeKey(input.row.committeeName)
  );
}

function candidateNameMatches(input: {
  record: IllinoisSbeExpenditureRecord;
  row: IllinoisCandidateFinanceDueRow;
}): boolean {
  const recordName = input.record.candidateName?.trim();
  if (!recordName) {
    return false;
  }
  const rowKeys = normalizeIllinoisCandidateNameKeys(input.row.candidateName);
  const recordKeys = normalizeIllinoisCandidateNameKeys(recordName);
  for (const key of recordKeys) {
    if (rowKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function officeDistrictMatches(input: {
  record: IllinoisSbeExpenditureRecord;
  row: IllinoisCandidateFinanceDueRow;
}): boolean {
  const officeSearch = toIllinoisSbeOfficeSearchInput({
    officeScope: input.row.officeScope,
    officeCanonicalName: input.row.officeName,
    district: input.row.district,
    districtType: input.row.sbeDistrictType,
    sbeOffice: input.row.sbeOffice,
    isAtLarge: input.row.isAtLarge,
  });
  if (!officeSearch) {
    return false;
  }
  const recordOffice = normalizeIllinoisFinanceTextKey(input.record.officeDistrict);
  const targetOffice = normalizeIllinoisFinanceTextKey(officeSearch.sbeOffice);
  if (!recordOffice || !targetOffice) {
    return false;
  }
  const district = officeSearch.district?.trim();
  if (!district) {
    return recordOffice === targetOffice;
  }
  if (officeSearch.sbeDistrictType) {
    if (!recordOffice.startsWith(`${targetOffice} `)) {
      return false;
    }
    const recordMunicipality = recordOffice.slice(targetOffice.length).trim().replace(/^OF\s+/, "");
    return illinoisMunicipalityMatches({
      voteAppDistrictName: district,
      sbeDistrictName: recordMunicipality,
      sbeDistrictType: officeSearch.sbeDistrictType,
    });
  }
  if (!recordOffice.includes(targetOffice)) {
    return false;
  }
  const districtKey = normalizeIllinoisFinanceTextKey(district).replace(/^0+/, "");
  const recordDistrictTokens = recordOffice
    .split(" ")
    .map((token) => token.replace(/(?:ST|ND|RD|TH)$/i, "").replace(/^0+/, ""))
    .filter(Boolean);
  return recordDistrictTokens.includes(districtKey);
}

function outsideExpenditureMatchesDueRow(input: {
  record: IllinoisSbeExpenditureRecord;
  row: IllinoisCandidateFinanceDueRow;
}): boolean {
  return (
    input.record.supportOppose !== null &&
    candidateNameMatches(input) &&
    officeDistrictMatches(input) &&
    isIllinoisFinanceCycleDate({
      rawDate: input.record.expendedDate,
      electionYear: input.row.electionYear,
    })
  );
}

function outsideGroupContributionMatches(input: {
  record: IllinoisSbeContributionRecord;
  outsideCommitteeKeys: ReadonlySet<string>;
}): boolean {
  return input.outsideCommitteeKeys.has(normalizeIllinoisCommitteeKey(input.record.recipientCommitteeName));
}

export function loadIllinoisFinanceDataForDueRowFromArtifacts(input: {
  row: IllinoisCandidateFinanceDueRow;
  artifacts: IllinoisSbeArtifactDataSet;
}): IllinoisCandidateFinanceData {
  const directContributionRecords = input.artifacts.contributionRecords.filter((record) =>
    directContributionMatchesDueRow({ record, row: input.row })
  );
  const data: IllinoisCandidateFinanceData = {
    directContributionRecords,
    directContributionSourceUrl: input.artifacts.contributionSourceUrl,
  };

  if (input.artifacts.normalizedArtifact && input.row.sbeCandidateId) {
    const committeeId = input.row.committeeKey.match(/^SBE:(.+)$/i)?.[1] ?? null;
    if (committeeId) {
      data.d2ReportSummaries = input.artifacts.normalizedArtifact.d2ReportSummaries.filter(
        (report) => report.committeeId === committeeId
      );
    }
  }

  if (input.artifacts.expenditureRecords !== undefined) {
    const outsideExpenditureRecords = input.artifacts.expenditureRecords.filter((record) =>
      outsideExpenditureMatchesDueRow({ record, row: input.row })
    );
    const outsideCommitteeKeys = new Set(
      outsideExpenditureRecords
        .map((record) => normalizeIllinoisCommitteeKey(record.expendingCommitteeName))
        .filter(Boolean)
    );
    data.outsideExpenditureRecords = outsideExpenditureRecords;
    data.outsideExpenditureSourceUrl = input.artifacts.expenditureSourceUrl;
    data.outsideGroupContributionRecords = input.artifacts.contributionRecords.filter((record) =>
      outsideGroupContributionMatches({ record, outsideCommitteeKeys })
    );
    data.outsideGroupContributionSourceUrl = input.artifacts.contributionSourceUrl;
  }

  return data;
}

export function createIllinoisSbeArtifactCandidateCommitteeResolver(
  artifacts: IllinoisSbeArtifactDataSet
): IllinoisCandidateCommitteeResolver {
  if (artifacts.normalizedArtifact) {
    return async (input) =>
      resolveIllinoisCandidateCommitteesFromRelations({
        ...input,
        relations: artifacts.normalizedArtifact!.candidateCommitteeRelations,
      });
  }
  return async (input) =>
    resolveIllinoisCandidateCommittee({
      ...input,
      contributionRecords: artifacts.contributionRecords,
      sourceUrl: artifacts.contributionSourceUrl,
    });
}

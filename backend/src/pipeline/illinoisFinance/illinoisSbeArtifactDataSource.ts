import { readFile } from "node:fs/promises";

import {
  normalizeIllinoisCandidateNameKeys,
  resolveIllinoisCandidateCommittee,
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
import { toIllinoisSbeOfficeSearchInput } from "./illinoisFinanceEligibleOffices.js";
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

export type IllinoisSbeArtifactDataSet = {
  contributionRecords: IllinoisSbeContributionRecord[];
  expenditureRecords?: IllinoisSbeExpenditureRecord[];
  contributionSourceUrl: string;
  expenditureSourceUrl?: string | null;
};

export type IllinoisSbeArtifactDataSourceConfig = {
  contributionCsvPaths: readonly string[];
  expenditureCsvPaths?: readonly string[];
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
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
  const contributionCsvPaths = normalizePathList(config.contributionCsvPaths, "contribution", true);
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
    normalizeIllinoisCommitteeKey(input.row.committeeKey)
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
  });
  if (!officeSearch) {
    return false;
  }
  const recordOffice = normalizeIllinoisFinanceTextKey(input.record.officeDistrict);
  const targetOffice = normalizeIllinoisFinanceTextKey(officeSearch.sbeOffice);
  if (!recordOffice || !targetOffice || !recordOffice.includes(targetOffice)) {
    return false;
  }
  const district = officeSearch.district?.trim();
  if (!district) {
    return true;
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
  return async (input) =>
    resolveIllinoisCandidateCommittee({
      ...input,
      contributionRecords: artifacts.contributionRecords,
      sourceUrl: artifacts.contributionSourceUrl,
    });
}

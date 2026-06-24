export const MICHIGAN_MITN_SOURCE = "MICHIGAN_MITN" as const;

export const MICHIGAN_MITN_LEGACY_DATA_URL =
  "https://www.michigan.gov/sos/elections/disclosure/cfr/mitn-help/mitn-legacy-campaign-finance-data";

export {
  autoLinkMichiganCandidateFinanceForCandidateElection,
  autoLinkMissingMichiganCandidateFinanceLinks,
  listMichiganCandidateElectionsMissingFinanceLinks,
} from "./michiganCandidateFinanceAutoLink.js";

export {
  listDueMichiganCandidateFinanceSyncRows,
  syncDueMichiganCandidateFinance,
} from "./michiganCandidateFinanceBatchSync.js";

export {
  syncMichiganCandidateFinance,
} from "./michiganCandidateFinanceSync.js";

export {
  replaceMichiganCandidateFinanceSnapshot,
  upsertMichiganFinanceLink,
} from "./michiganFinanceWriter.js";

export {
  aggregateMichiganOutsideGroupContributions,
} from "./michiganOutsideGroupContributionAggregator.js";

export {
  aggregateMichiganOutsideSpending,
  isMichiganIndependentExpenditureSchedule,
  supportOpposeFromMichiganSuppOpp,
} from "./michiganOutsideSpendingAggregator.js";

export {
  DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR,
  MICHIGAN_MITN_LEGACY_ARCHIVE_BASE_URL,
  MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS,
  buildMichiganMitnLegacyArchiveUrl,
  downloadMichiganMitnLegacyArchive,
  fetchMichiganMitnLegacyArchiveMetadata,
  getMichiganMitnLegacyArchiveCachePaths,
  normalizeMichiganMitnLegacyArchiveYear,
  parseMichiganMitnHttpsUrl,
  readMichiganMitnLegacyArchiveCacheMetadata,
  refreshMichiganMitnLegacyArchiveCache,
} from "./michiganMitnLegacyArtifactCache.js";

export {
  aggregateMichiganDirectContributions,
  isMichiganDirectDonorSupportReceipt,
  isMichiganTotalReceipt,
  michiganElectionCycleStartYear,
} from "./michiganDirectContributionAggregator.js";

export {
  normalizeMichiganCandidateNameForStorage,
  normalizeMichiganCandidateNameKeys,
  resolveMichiganCandidateCommittee,
} from "./michiganCandidateCommitteeResolver.js";

export {
  MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isMichiganFinanceEligibleOffice,
  mapMichiganMitnOffice,
  normalizeMichiganMitnLegislativeDistrict,
  normalizeMichiganMitnOfficeLabel,
  toMichiganFinanceOfficeKey,
  toMichiganMitnOfficeSearchInput,
} from "./michiganFinanceEligibleOffices.js";

export {
  MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS,
  MICHIGAN_MITN_LEGACY_CONTRIBUTIONS_CSV_FILE_NAME_PATTERN,
  MICHIGAN_MITN_LEGACY_EXPENDITURES_CSV_FILE_NAME_PATTERN,
  MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS,
  MICHIGAN_MITN_LEGACY_RECEIPTS_CSV_FILE_NAME_PATTERN,
  listMichiganMitnLegacyContributionCsvFileNames,
  listMichiganMitnLegacyExtractedFileNames,
  michiganMitnLegacyExpendituresCsvFileName,
  michiganMitnLegacyReceiptsCsvFileName,
  normalizeMichiganMitnLegacyCsvHeader,
  parseMichiganMitnLegacyCsvRows,
  readMichiganMitnLegacyContributionRows,
  readMichiganMitnLegacyCsvTableRows,
  readMichiganMitnLegacyExpenditureRows,
} from "./michiganMitnLegacyArchiveReader.js";

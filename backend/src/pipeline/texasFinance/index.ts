export const TEXAS_TEC_SOURCE = "TEXAS_TEC" as const;

export const TEXAS_TEC_CAMPAIGN_FINANCE_SEARCH_URL =
  "https://www.ethics.state.tx.us/search/cf/";

export const TEXAS_TEC_CSV_DATABASE_LAYOUT_URL =
  "https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt";

export const TEXAS_TEC_CSV_CODES_URL =
  "https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt";

export const TEXAS_TEC_SUPER_PAC_LIST_URL =
  "https://www.ethics.state.tx.us/search/cf/SuperPac.html";

export * from "./texasFinanceEligibleOffices.js";
export * from "./texasCandidateCommitteeResolver.js";
export * from "./texasCandidateFinanceAutoLink.js";
export * from "./texasCandidateFinanceBatchSync.js";
export * from "./texasCandidateFinanceSync.js";
export * from "./texasDirectContributionAggregator.js";
export * from "./texasFinanceWriter.js";
export * from "./texasOutsideGroupContributionAggregator.js";
export * from "./texasOutsideSpendingAggregator.js";

export {
  DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR,
  TEXAS_TEC_CSV_DATABASE_CACHE_METADATA_FILE_NAME,
  TEXAS_TEC_CSV_DATABASE_CACHE_ZIP_FILE_NAME,
  TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS,
  TEXAS_TEC_CSV_DATABASE_URL,
  downloadTexasTecCsvDatabase,
  fetchTexasTecCsvDatabaseMetadata,
  getTexasTecCsvDatabaseArtifactCachePaths,
  parseTexasTecHttpsUrl,
  readTexasTecCsvDatabaseArtifactCacheMetadata,
  refreshTexasTecCsvDatabaseArtifactCache,
} from "./texasTecCsvDatabaseArtifactCache.js";

export {
  TEXAS_TEC_CANDIDATES_CSV_FILE_NAME,
  TEXAS_TEC_CANDIDATE_COLUMNS,
  TEXAS_TEC_CONTRIBUTION_COLUMNS,
  TEXAS_TEC_CONTRIBUTION_CSV_FILE_PATTERN,
  TEXAS_TEC_EXPENDITURE_COLUMNS,
  TEXAS_TEC_EXPENDITURE_CSV_FILE_PATTERN,
  TEXAS_TEC_FILER_COLUMNS,
  TEXAS_TEC_FILERS_CSV_FILE_NAME,
  TEXAS_TEC_PURPOSE_COLUMNS,
  TEXAS_TEC_PURPOSE_CSV_FILE_NAME,
  TEXAS_TEC_SPAC_COLUMNS,
  TEXAS_TEC_SPACS_CSV_FILE_NAME,
  listTexasTecContributionCsvFileNames,
  listTexasTecCsvDatabaseZipEntries,
  listTexasTecExpenditureCsvFileNames,
  normalizeTexasTecCsvPartition,
  readTexasTecCandidateRows,
  readTexasTecContributionRows,
  readTexasTecCsvDatabaseTableRows,
  readTexasTecExpenditureRows,
  readTexasTecFilerRows,
  readTexasTecPurposeRows,
  readTexasTecSpacRows,
  texasTecContributionCsvFileName,
  texasTecExpenditureCsvFileName,
} from "./texasTecCsvDatabaseReader.js";

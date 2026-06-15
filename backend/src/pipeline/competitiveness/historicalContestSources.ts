import type { HistoricalContestOfficeType } from "./historicalContestKeys.js";

export type HistoricalContestSourceFormat = "medsl_aggregate_csv" | "medsl_precinct_csv";

export type HistoricalContestSourceDownloadMode = "direct" | "dataverse_guestbook";

export type HistoricalContestSourceFileDiscovery = {
  dataverseDatasetPersistentIds: readonly string[];
};

export type HistoricalContestSourceDefinition = {
  preset: string;
  source: string;
  sourceUrl: string;
  sourceFiles?: readonly string[];
  sourceFileDiscovery?: HistoricalContestSourceFileDiscovery;
  downloadMode?: HistoricalContestSourceDownloadMode;
  format: HistoricalContestSourceFormat;
  electionYear: number;
  officeTypes: readonly HistoricalContestOfficeType[];
  staleAfterRedistricting: boolean;
};

const MEDSL_2024_STATE_PRECINCT_FILES = [
  "https://dataverse.harvard.edu/api/access/datafile/13731163",
  "https://dataverse.harvard.edu/api/access/datafile/13731172",
  "https://dataverse.harvard.edu/api/access/datafile/13731179",
  "https://dataverse.harvard.edu/api/access/datafile/13731136",
  "https://dataverse.harvard.edu/api/access/datafile/13731130",
  "https://dataverse.harvard.edu/api/access/datafile/13731171",
  "https://dataverse.harvard.edu/api/access/datafile/13731178",
  "https://dataverse.harvard.edu/api/access/datafile/13731137",
  "https://dataverse.harvard.edu/api/access/datafile/13731141",
  "https://dataverse.harvard.edu/api/access/datafile/13731132",
  "https://dataverse.harvard.edu/api/access/datafile/13731164",
  "https://dataverse.harvard.edu/api/access/datafile/13731168",
  "https://dataverse.harvard.edu/api/access/datafile/13731166",
  "https://dataverse.harvard.edu/api/access/datafile/13731146",
  "https://dataverse.harvard.edu/api/access/datafile/13731173",
  "https://dataverse.harvard.edu/api/access/datafile/13731143",
  "https://dataverse.harvard.edu/api/access/datafile/13731162",
  "https://dataverse.harvard.edu/api/access/datafile/13731142",
  "https://dataverse.harvard.edu/api/access/datafile/13731147",
  "https://dataverse.harvard.edu/api/access/datafile/13731133",
  "https://dataverse.harvard.edu/api/access/datafile/13731175",
  "https://dataverse.harvard.edu/api/access/datafile/13731129",
  "https://dataverse.harvard.edu/api/access/datafile/13731148",
  "https://dataverse.harvard.edu/api/access/datafile/13731176",
  "https://dataverse.harvard.edu/api/access/datafile/13731155",
  "https://dataverse.harvard.edu/api/access/datafile/13731131",
  "https://dataverse.harvard.edu/api/access/datafile/13731134",
  "https://dataverse.harvard.edu/api/access/datafile/13731149",
  "https://dataverse.harvard.edu/api/access/datafile/13731151",
  "https://dataverse.harvard.edu/api/access/datafile/13731145",
  "https://dataverse.harvard.edu/api/access/datafile/13731152",
  "https://dataverse.harvard.edu/api/access/datafile/13731174",
  "https://dataverse.harvard.edu/api/access/datafile/13731177",
  "https://dataverse.harvard.edu/api/access/datafile/13731154",
  "https://dataverse.harvard.edu/api/access/datafile/13731135",
  "https://dataverse.harvard.edu/api/access/datafile/13731161",
  "https://dataverse.harvard.edu/api/access/datafile/13731165",
  "https://dataverse.harvard.edu/api/access/datafile/13731150",
  "https://dataverse.harvard.edu/api/access/datafile/13731153",
  "https://dataverse.harvard.edu/api/access/datafile/13731139",
  "https://dataverse.harvard.edu/api/access/datafile/13731158",
  "https://dataverse.harvard.edu/api/access/datafile/13731169",
  "https://dataverse.harvard.edu/api/access/datafile/13731160",
  "https://dataverse.harvard.edu/api/access/datafile/13731167",
  "https://dataverse.harvard.edu/api/access/datafile/13731170",
  "https://dataverse.harvard.edu/api/access/datafile/13731156",
  "https://dataverse.harvard.edu/api/access/datafile/13731144",
  "https://dataverse.harvard.edu/api/access/datafile/13731157",
  "https://dataverse.harvard.edu/api/access/datafile/13731159",
  "https://dataverse.harvard.edu/api/access/datafile/13731138",
  "https://dataverse.harvard.edu/api/access/datafile/13731140",
] as const;

const MEDSL_2022_PRECINCT_FILES = [
  "https://dataverse.harvard.edu/api/access/datafile/13996909",
  "https://dataverse.harvard.edu/api/access/datafile/10855180",
  "https://dataverse.harvard.edu/api/access/datafile/13996911",
  "https://dataverse.harvard.edu/api/access/datafile/13996910",
  "https://dataverse.harvard.edu/api/access/datafile/10855160",
  "https://dataverse.harvard.edu/api/access/datafile/10855159",
  "https://dataverse.harvard.edu/api/access/datafile/10855153",
  "https://dataverse.harvard.edu/api/access/datafile/10855143",
  "https://dataverse.harvard.edu/api/access/datafile/10855167",
  "https://dataverse.harvard.edu/api/access/datafile/10855141",
  "https://dataverse.harvard.edu/api/access/datafile/10855131",
  "https://dataverse.harvard.edu/api/access/datafile/10855136",
  "https://dataverse.harvard.edu/api/access/datafile/10855175",
  "https://dataverse.harvard.edu/api/access/datafile/10855156",
  "https://dataverse.harvard.edu/api/access/datafile/10855171",
  "https://dataverse.harvard.edu/api/access/datafile/10855164",
  "https://dataverse.harvard.edu/api/access/datafile/10855177",
  "https://dataverse.harvard.edu/api/access/datafile/10855151",
  "https://dataverse.harvard.edu/api/access/datafile/10855148",
  "https://dataverse.harvard.edu/api/access/datafile/10855161",
  "https://dataverse.harvard.edu/api/access/datafile/10855135",
  "https://dataverse.harvard.edu/api/access/datafile/10855157",
  "https://dataverse.harvard.edu/api/access/datafile/10855170",
  "https://dataverse.harvard.edu/api/access/datafile/10855158",
  "https://dataverse.harvard.edu/api/access/datafile/10855138",
  "https://dataverse.harvard.edu/api/access/datafile/10855150",
  "https://dataverse.harvard.edu/api/access/datafile/10855176",
  "https://dataverse.harvard.edu/api/access/datafile/10855162",
  "https://dataverse.harvard.edu/api/access/datafile/10855149",
  "https://dataverse.harvard.edu/api/access/datafile/10855142",
  "https://dataverse.harvard.edu/api/access/datafile/10855155",
  "https://dataverse.harvard.edu/api/access/datafile/10855179",
  "https://dataverse.harvard.edu/api/access/datafile/10855173",
  "https://dataverse.harvard.edu/api/access/datafile/10855137",
  "https://dataverse.harvard.edu/api/access/datafile/10855182",
  "https://dataverse.harvard.edu/api/access/datafile/10855145",
  "https://dataverse.harvard.edu/api/access/datafile/10855174",
  "https://dataverse.harvard.edu/api/access/datafile/10855147",
  "https://dataverse.harvard.edu/api/access/datafile/10855146",
  "https://dataverse.harvard.edu/api/access/datafile/10855169",
  "https://dataverse.harvard.edu/api/access/datafile/10855163",
  "https://dataverse.harvard.edu/api/access/datafile/10855168",
  "https://dataverse.harvard.edu/api/access/datafile/10855139",
  "https://dataverse.harvard.edu/api/access/datafile/10855152",
  "https://dataverse.harvard.edu/api/access/datafile/10855154",
  "https://dataverse.harvard.edu/api/access/datafile/10855134",
  "https://dataverse.harvard.edu/api/access/datafile/10855178",
  "https://dataverse.harvard.edu/api/access/datafile/10855166",
  "https://dataverse.harvard.edu/api/access/datafile/10855181",
  "https://dataverse.harvard.edu/api/access/datafile/10855172",
  "https://dataverse.harvard.edu/api/access/datafile/10855165",
] as const;

export const VERIFIED_HISTORICAL_CONTEST_SOURCES = [
  {
    preset: "medsl-2024-president-state",
    source: "MIT_2024",
    sourceUrl: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
    format: "medsl_aggregate_csv",
    electionYear: 2024,
    officeTypes: ["US_PRESIDENT"],
    staleAfterRedistricting: false,
  },
  {
    preset: "medsl-2024-senate-state",
    source: "MIT_2024",
    sourceUrl: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
    format: "medsl_aggregate_csv",
    electionYear: 2024,
    officeTypes: ["US_SENATE"],
    staleAfterRedistricting: false,
  },
  {
    preset: "medsl-2024-house-precinct",
    source: "MIT_2024",
    sourceUrl: "https://dataverse.harvard.edu/api/access/datafile/13731101",
    format: "medsl_precinct_csv",
    electionYear: 2024,
    officeTypes: ["US_HOUSE"],
    staleAfterRedistricting: false,
  },
  {
    preset: "medsl-2022-precinct",
    source: "MIT_2022",
    sourceUrl: "https://doi.org/10.7910/DVN/UYQIEP",
    sourceFiles: MEDSL_2022_PRECINCT_FILES,
    format: "medsl_precinct_csv",
    electionYear: 2022,
    officeTypes: ["US_SENATE", "US_HOUSE", "GOVERNOR", "STATE_SENATE", "STATE_HOUSE"],
    staleAfterRedistricting: false,
  },
  {
    preset: "medsl-2020-precinct-by-state",
    source: "MIT_2020",
    sourceUrl: "https://doi.org/10.7910/DVN/NT66Z3",
    sourceFileDiscovery: {
      dataverseDatasetPersistentIds: ["doi:10.7910/DVN/NT66Z3"],
    },
    downloadMode: "dataverse_guestbook",
    format: "medsl_precinct_csv",
    electionYear: 2020,
    officeTypes: ["US_PRESIDENT", "US_SENATE", "US_HOUSE", "GOVERNOR", "STATE_SENATE", "STATE_HOUSE"],
    staleAfterRedistricting: true,
  },
  {
    preset: "medsl-2018-precinct-by-state",
    source: "MIT_2018",
    sourceUrl: "https://doi.org/10.7910/DVN/NVQYMG",
    sourceFileDiscovery: {
      dataverseDatasetPersistentIds: ["doi:10.7910/DVN/NVQYMG"],
    },
    downloadMode: "dataverse_guestbook",
    format: "medsl_precinct_csv",
    electionYear: 2018,
    officeTypes: ["US_SENATE", "US_HOUSE", "GOVERNOR", "STATE_SENATE", "STATE_HOUSE"],
    staleAfterRedistricting: true,
  },
  {
    preset: "medsl-2024-state-precinct",
    source: "MIT_2024",
    sourceUrl: "https://doi.org/10.7910/DVN/NYTPDU",
    sourceFiles: MEDSL_2024_STATE_PRECINCT_FILES,
    format: "medsl_precinct_csv",
    electionYear: 2024,
    officeTypes: ["GOVERNOR", "STATE_SENATE", "STATE_HOUSE"],
    staleAfterRedistricting: false,
  },
] as const satisfies readonly HistoricalContestSourceDefinition[];

export type VerifiedHistoricalContestSourcePreset =
  (typeof VERIFIED_HISTORICAL_CONTEST_SOURCES)[number]["preset"];

export const VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET = Object.fromEntries(
  VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => [source.preset, source])
) as Record<VerifiedHistoricalContestSourcePreset, (typeof VERIFIED_HISTORICAL_CONTEST_SOURCES)[number]>;

export function listVerifiedHistoricalContestSourcePresets(): VerifiedHistoricalContestSourcePreset[] {
  return VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => source.preset);
}

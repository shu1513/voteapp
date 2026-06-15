import type { HistoricalContestOfficeType } from "./historicalContestKeys.js";

export type HistoricalContestSourceFormat = "medsl_aggregate_csv" | "medsl_precinct_csv";

export type HistoricalContestSourceDefinition = {
  preset: string;
  source: string;
  sourceUrl: string;
  format: HistoricalContestSourceFormat;
  electionYear: number;
  officeTypes: readonly HistoricalContestOfficeType[];
  staleAfterRedistricting: boolean;
};

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
] as const satisfies readonly HistoricalContestSourceDefinition[];

export type VerifiedHistoricalContestSourcePreset =
  (typeof VERIFIED_HISTORICAL_CONTEST_SOURCES)[number]["preset"];

export const VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET = Object.fromEntries(
  VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => [source.preset, source])
) as Record<VerifiedHistoricalContestSourcePreset, (typeof VERIFIED_HISTORICAL_CONTEST_SOURCES)[number]>;

export function listVerifiedHistoricalContestSourcePresets(): VerifiedHistoricalContestSourcePreset[] {
  return VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => source.preset);
}

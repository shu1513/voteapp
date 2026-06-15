import { describe, expect, it, vi } from "vitest";

import {
  importHistoricalContestMarginsFromCsv,
  parseMedslHistoricalContestCsv,
} from "../../../src/pipeline/competitiveness/historicalContestCsvImport.js";

const csv = [
  "year,state_po,state_fips,office,district,stage,candidate,party_detailed,party_simplified,candidatevotes,totalvotes",
  '2024,CA,06,US HOUSE,31,GEN,"Smith, Jane",Democratic,DEMOCRAT,109200,200000',
  "2024,CA,06,US HOUSE,31,GEN,Johnson,Republican,REPUBLICAN,90800,200000",
].join("\n");

describe("historicalContestCsvImport", () => {
  it("parses MEDSL CSV rows with quoted fields", () => {
    expect(parseMedslHistoricalContestCsv(csv)).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "31",
        candidate: "Smith, Jane",
        candidatevotes: "109200",
        totalvotes: "200000",
        party_simplified: "DEMOCRAT",
        party_detailed: "Democratic",
        stage: "GEN",
      },
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "31",
        candidate: "Johnson",
        candidatevotes: "90800",
        totalvotes: "200000",
        party_simplified: "REPUBLICAN",
        party_detailed: "Republican",
        stage: "GEN",
      },
    ]);
  });

  it("parses MEDSL CSV rows when the first header has a UTF-8 BOM", () => {
    const bomCsv = [
      "\uFEFFyear,state_po,state_fips,office,district,stage,candidate,party_detailed,party_simplified,candidatevotes,totalvotes",
      "2024,CA,06,US HOUSE,31,GEN,Johnson,Republican,REPUBLICAN,90800,200000",
    ].join("\n");

    expect(parseMedslHistoricalContestCsv(bomCsv)).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "31",
        candidate: "Johnson",
        candidatevotes: "90800",
        totalvotes: "200000",
        party_simplified: "REPUBLICAN",
        party_detailed: "Republican",
        stage: "GEN",
      },
    ]);
  });

  it("parses tab-separated MEDSL rows", () => {
    const tsv = [
      "year\tstate_po\tstate_fips\toffice\tdistrict\tstage\tcandidate\tparty_detailed\tparty_simplified\tcandidatevotes\ttotalvotes",
      "2024\tCA\t06\tUS HOUSE\t31\tGEN\tJohnson\tRepublican\tREPUBLICAN\t90800\t200000",
    ].join("\n");

    expect(parseMedslHistoricalContestCsv(tsv)).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "31",
        candidate: "Johnson",
        candidatevotes: "90800",
        totalvotes: "200000",
        party_simplified: "REPUBLICAN",
        party_detailed: "Republican",
        stage: "GEN",
      },
    ]);
  });

  it("parses statewide MEDSL files that use votes and omit district", () => {
    const statewideCsv = [
      "year,state,state_po,state_fips,office,candidate,party_detailed,party_simplified,votes,totalvotes,stage",
      "2024,ARIZONA,AZ,04,US SENATE,RUBEN GALLEGO,DEMOCRAT,DEMOCRAT,1676335,3347964,GEN",
    ].join("\n");

    expect(parseMedslHistoricalContestCsv(statewideCsv)).toEqual([
      {
        year: "2024",
        state_po: "AZ",
        state_fips: "04",
        office: "US SENATE",
        district: "STATEWIDE",
        candidate: "RUBEN GALLEGO",
        candidatevotes: "1676335",
        totalvotes: "3347964",
        party_simplified: "DEMOCRAT",
        party_detailed: "DEMOCRAT",
        stage: "GEN",
      },
    ]);
  });

  it("throws when required MEDSL columns are missing", () => {
    expect(() => parseMedslHistoricalContestCsv("year,state_po\n2024,CA\n")).toThrow(
      "Missing required MEDSL CSV column: state_fips"
    );
  });

  it("normalizes rows without writing in dry-run mode", async () => {
    const query = vi.fn();

    await expect(
      importHistoricalContestMarginsFromCsv(
        { query } as never,
        {
          csv,
          source: "MIT_2024",
          sourceUrl: "https://github.com/MEDSL/2024-elections-official",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      normalizedRecords: 1,
      skippedRows: [],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("writes normalized records when dry-run mode is off", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      importHistoricalContestMarginsFromCsv(
        { query } as never,
        {
          csv,
          source: "MIT_2024",
          importedAt: new Date("2026-06-14T12:00:00.000Z"),
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      normalizedRecords: 1,
      skippedRows: [],
      writeResult: {
        requested: 1,
        rowsWritten: 1,
      },
    });

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]?.slice(0, 8)).toEqual([
      "MIT_2024",
      null,
      2024,
      "CA",
      "06",
      "US_HOUSE",
      "us_house",
      "0631",
    ]);
  });
});

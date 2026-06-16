import { describe, expect, it, vi } from "vitest";

import {
  aggregateMedslPrecinctRowsToCandidateRows,
  importHistoricalContestMarginsFromPrecinctCsv,
  parseAndAggregateMedslHistoricalContestPrecinctCsv,
  parseMedslHistoricalContestPrecinctCsv,
  type MedslHistoricalContestPrecinctRow,
} from "../../../src/pipeline/competitiveness/historicalContestPrecinctCsvImport.js";

function row(overrides: Partial<MedslHistoricalContestPrecinctRow>): MedslHistoricalContestPrecinctRow {
  return {
    year: "2024",
    state_po: "CA",
    state_fips: "06",
    office: "US HOUSE",
    district: "012",
    candidate: "Candidate One",
    votes: "0",
    party_simplified: "DEMOCRAT",
    party_detailed: "DEMOCRAT",
    stage: "GEN",
    mode: "TOTAL",
    writein: "FALSE",
    precinct: "200100",
    county_name: "ALAMEDA",
    county_fips: "001",
    jurisdiction_name: "ALAMEDA COUNTY",
    jurisdiction_fips: "001",
    dataverse: "HOUSE",
    special: "FALSE",
    ...overrides,
  };
}

describe("historicalContestPrecinctCsvImport", () => {
  it("parses MEDSL precinct CSV rows", () => {
    const csv = [
      "precinct,office,party_detailed,party_simplified,mode,votes,county_name,county_fips,jurisdiction_name,jurisdiction_fips,candidate,district,dataverse,year,stage,state,special,writein,state_po,state_fips,state_cen,state_ic,date,magnitude",
      '200100,US HOUSE,DEMOCRAT,DEMOCRAT,TOTAL,102,ALAMEDA,001,ALAMEDA COUNTY,001,"SIMON, LATEEFAH",012,HOUSE,2024,GEN,CALIFORNIA,FALSE,FALSE,CA,06,93,71,2024-11-05,1',
    ].join("\n");

    expect(parseMedslHistoricalContestPrecinctCsv(csv)).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "012",
        candidate: "SIMON, LATEEFAH",
        votes: "102",
        party_simplified: "DEMOCRAT",
        party_detailed: "DEMOCRAT",
        stage: "GEN",
        mode: "TOTAL",
        writein: "FALSE",
        precinct: "200100",
        county_name: "ALAMEDA",
        county_fips: "001",
        jurisdiction_name: "ALAMEDA COUNTY",
        jurisdiction_fips: "001",
        dataverse: "HOUSE",
        special: "FALSE",
      },
    ]);
  });

  it("keeps optional precinct columns nullable", () => {
    const csv = [
      "year,state_po,state_fips,office,votes",
      "2024,WA,53,GOVERNOR,420",
    ].join("\n");

    expect(parseMedslHistoricalContestPrecinctCsv(csv)).toEqual([
      {
        year: "2024",
        state_po: "WA",
        state_fips: "53",
        office: "GOVERNOR",
        district: null,
        candidate: null,
        votes: "420",
        party_simplified: null,
        party_detailed: null,
        stage: null,
        mode: null,
        writein: null,
        precinct: null,
        county_name: null,
        county_fips: null,
        jurisdiction_name: null,
        jurisdiction_fips: null,
        dataverse: null,
        special: null,
      },
    ]);
  });

  it("parses older MEDSL precinct aliases from 2016 files", () => {
    const csv = [
      "year,stage,special,state,state_postal,state_fips,county_name,precinct,candidate,office,district,writein,party,mode,votes,candidate_party",
      "2016,GEN,FALSE,California,CA,06,ALAMEDA,1001,Candidate One,US PRESIDENT,,FALSE,democrat,TOTAL,500,DEMOCRAT",
    ].join("\n");

    expect(parseMedslHistoricalContestPrecinctCsv(csv)).toEqual([
      {
        year: "2016",
        state_po: "CA",
        state_fips: "06",
        office: "US PRESIDENT",
        district: null,
        candidate: "Candidate One",
        votes: "500",
        party_simplified: "democrat",
        party_detailed: "DEMOCRAT",
        stage: "GEN",
        mode: "TOTAL",
        writein: "FALSE",
        precinct: "1001",
        county_name: "ALAMEDA",
        county_fips: null,
        jurisdiction_name: null,
        jurisdiction_fips: null,
        dataverse: null,
        special: "FALSE",
      },
    ]);
  });

  it("returns no rows for empty CSV input", () => {
    expect(parseMedslHistoricalContestPrecinctCsv("")).toEqual([]);
  });

  it("throws when required MEDSL precinct columns are missing", () => {
    expect(() => parseMedslHistoricalContestPrecinctCsv("year,state_po,state_fips,office\n2024,CA,06,US HOUSE\n")).toThrow(
      "Missing required MEDSL CSV column: votes"
    );
  });

  it("applies the office allowlist during precinct imports", async () => {
    const query = vi.fn();
    const mixedOfficeCsv = [
      "year,state_po,state_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,CA,06,US PRESIDENT,,President One,600,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,US PRESIDENT,,President Two,400,REPUBLICAN,GEN,TOTAL",
      "2024,CA,06,GOVERNOR,,Governor One,550,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,GOVERNOR,,Governor Two,450,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: mixedOfficeCsv,
          source: "MIT_2024",
          officeTypes: ["GOVERNOR"],
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 4,
      aggregatedRows: 4,
      normalizedRecords: 1,
      skippedRows: [{ reason: "excluded_office" }, { reason: "excluded_office" }],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("allows newly supported statewide executive offices during precinct imports", async () => {
    const query = vi.fn();
    const mixedOfficeCsv = [
      "year,state_po,state_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,CA,06,STATE TREASURER,,Treasurer One,520,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,STATE TREASURER,,Treasurer Two,480,REPUBLICAN,GEN,TOTAL",
      "2024,CA,06,US PRESIDENT,,President One,600,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,US PRESIDENT,,President Two,400,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: mixedOfficeCsv,
          source: "MIT_2024",
          officeTypes: ["STATE_TREASURER"],
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 4,
      aggregatedRows: 4,
      normalizedRecords: 1,
      skippedRows: [{ reason: "excluded_office" }, { reason: "excluded_office" }],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("normalizes safe countywide precinct office labels before import", async () => {
    const query = vi.fn();
    const countyOfficeCsv = [
      "year,state_po,state_fips,county_name,county_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,,Sheriff One,56000,DEMOCRAT,GEN,TOTAL",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,,Sheriff Two,44000,REPUBLICAN,GEN,TOTAL",
      "2024,WA,53,KING,53033,KING COUNTY PROSECUTING ATTORNEY,,Prosecutor One,52000,DEMOCRAT,GEN,TOTAL",
      "2024,WA,53,KING,53033,KING COUNTY PROSECUTING ATTORNEY,,Prosecutor Two,48000,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: countyOfficeCsv,
          source: "MIT_2024",
          officeTypes: ["COUNTY_SHERIFF", "DISTRICT_ATTORNEY"],
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 4,
      aggregatedRows: 4,
      normalizedRecords: 2,
      skippedRows: [],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();

    expect(parseAndAggregateMedslHistoricalContestPrecinctCsv(countyOfficeCsv)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          office: "COUNTY SHERIFF",
          district: "53011",
          candidate: "Sheriff One",
          candidatevotes: "56000",
          totalvotes: "100000",
        }),
        expect.objectContaining({
          office: "DISTRICT ATTORNEY",
          district: "53033",
          candidate: "Prosecutor One",
          candidatevotes: "52000",
          totalvotes: "100000",
        }),
      ])
    );
  });

  it("normalizes every supported countywide office type during precinct imports", async () => {
    const query = vi.fn();
    const supportedCountyOfficeTypes = [
      "COUNTY_SHERIFF",
      "DISTRICT_ATTORNEY",
      "COUNTY_CLERK",
      "COUNTY_ASSESSOR",
      "COUNTY_AUDITOR",
      "COUNTY_TREASURER",
      "COUNTY_RECORDER",
      "COUNTY_CORONER",
    ] as const;
    const rows = [
      ["CLARK COUNTY SHERIFF", "Sheriff"],
      ["CLARK COUNTY PROSECUTOR", "Prosecutor"],
      ["CLARK COUNTY CLERK", "Clerk"],
      ["CLARK COUNTY ASSESSOR", "Assessor"],
      ["CLARK COUNTY AUDITOR", "Auditor"],
      ["CLARK COUNTY TREASURER", "Treasurer"],
      ["CLARK COUNTY RECORDER", "Recorder"],
      ["CLARK COUNTY CORONER", "Coroner"],
    ].flatMap(([office, candidate]) => [
      `2024,WA,53,CLARK,011,${office},,${candidate} One,56000,DEMOCRAT,GEN,TOTAL`,
      `2024,WA,53,CLARK,011,${office},,${candidate} Two,44000,REPUBLICAN,GEN,TOTAL`,
    ]);
    const countyOfficeCsv = [
      "year,state_po,state_fips,county_name,county_fips,office,district,candidate,votes,party_simplified,stage,mode",
      ...rows,
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: countyOfficeCsv,
          source: "MIT_2024",
          officeTypes: supportedCountyOfficeTypes,
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 16,
      aggregatedRows: 16,
      normalizedRecords: 8,
      skippedRows: [],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();

    const aggregateOffices = new Set(
      parseAndAggregateMedslHistoricalContestPrecinctCsv(countyOfficeCsv).map((row) => row.office)
    );
    expect(aggregateOffices).toEqual(
      new Set([
        "COUNTY SHERIFF",
        "DISTRICT ATTORNEY",
        "COUNTY CLERK",
        "COUNTY ASSESSOR",
        "COUNTY AUDITOR",
        "COUNTY TREASURER",
        "COUNTY RECORDER",
        "COUNTY CORONER",
      ])
    );
  });

  it("normalizes county attorney labels as district attorney when county-scoped", async () => {
    const query = vi.fn();
    const countyOfficeCsv = [
      "year,state_po,state_fips,county_name,county_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,MN,27,HENNEPIN,053,HENNEPIN COUNTY ATTORNEY,,Attorney One,56000,DEMOCRAT,GEN,TOTAL",
      "2024,MN,27,HENNEPIN,053,HENNEPIN COUNTY ATTORNEY,,Attorney Two,44000,REPUBLICAN,GEN,TOTAL",
      "2024,IA,19,POLK,153,COUNTY ATTORNEY,,County Attorney One,52000,DEMOCRAT,GEN,TOTAL",
      "2024,IA,19,POLK,153,COUNTY ATTORNEY,,County Attorney Two,48000,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: countyOfficeCsv,
          source: "MIT_2024",
          officeTypes: ["DISTRICT_ATTORNEY"],
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 4,
      aggregatedRows: 4,
      normalizedRecords: 2,
      skippedRows: [],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();

    expect(parseAndAggregateMedslHistoricalContestPrecinctCsv(countyOfficeCsv)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          office: "DISTRICT ATTORNEY",
          district: "27053",
          candidate: "Attorney One",
          candidatevotes: "56000",
          totalvotes: "100000",
        }),
        expect.objectContaining({
          office: "DISTRICT ATTORNEY",
          district: "19153",
          candidate: "County Attorney One",
          candidatevotes: "52000",
          totalvotes: "100000",
        }),
      ])
    );
  });

  it("leaves unsafe county, city, and seat-designated precinct offices unsupported", async () => {
    const query = vi.fn();
    const unsafeCountyOfficeCsv = [
      "year,state_po,state_fips,county_name,county_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,TX,48,HARRIS,201,HARRIS COUNTY COMMISSIONER,,Commissioner One,600,DEMOCRAT,GEN,TOTAL",
      "2024,TX,48,HARRIS,201,HARRIS COUNTY COMMISSIONER,,Commissioner Two,400,REPUBLICAN,GEN,TOTAL",
      "2024,TX,48,HARRIS,201,DISTRICT ATTORNEY,,District Attorney One,550,DEMOCRAT,GEN,TOTAL",
      "2024,TX,48,HARRIS,201,DISTRICT ATTORNEY,,District Attorney Two,450,REPUBLICAN,GEN,TOTAL",
      "2024,AR,05,BENTON,007,CITY OF BENTONVILLE MAYOR,,Mayor One,520,DEMOCRAT,GEN,TOTAL",
      "2024,AR,05,BENTON,007,CITY OF BENTONVILLE MAYOR,,Mayor Two,480,REPUBLICAN,GEN,TOTAL",
      "2024,AL,01,JEFFERSON,073,JEFFERSON COUNTY CONSTABLE,,Constable One,510,DEMOCRAT,GEN,TOTAL",
      "2024,AL,01,JEFFERSON,073,JEFFERSON COUNTY CONSTABLE,,Constable Two,490,REPUBLICAN,GEN,TOTAL",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,2,Sheriff District One,530,DEMOCRAT,GEN,TOTAL",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,2,Sheriff District Two,470,REPUBLICAN,GEN,TOTAL",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,COUNTYWIDE,Sheriff Text One,530,DEMOCRAT,GEN,TOTAL",
      "2024,WA,53,CLARK,011,CLARK COUNTY SHERIFF,COUNTYWIDE,Sheriff Text Two,470,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv: unsafeCountyOfficeCsv,
          source: "MIT_2024",
          officeTypes: ["COUNTY_SHERIFF", "DISTRICT_ATTORNEY"],
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 12,
      aggregatedRows: 12,
      normalizedRecords: 0,
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("aggregates TOTAL mode precinct rows instead of double-counting split modes", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: "Candidate One", votes: "60", mode: "ABSENTEE" }),
        row({ candidate: "Candidate One", votes: "40", mode: "NOT ABSENTEE" }),
        row({ candidate: "Candidate One", votes: "100", mode: "TOTAL" }),
        row({
          candidate: "Candidate Two",
          votes: "70",
          mode: "TOTAL",
          party_simplified: "REPUBLICAN",
          party_detailed: "REPUBLICAN",
        }),
      ])
    ).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "012",
        candidate: "Candidate One",
        candidatevotes: "100",
        totalvotes: "170",
        party_simplified: "DEMOCRAT",
        party_detailed: "DEMOCRAT",
        stage: "GEN",
      },
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "012",
        candidate: "Candidate Two",
        candidatevotes: "70",
        totalvotes: "170",
        party_simplified: "REPUBLICAN",
        party_detailed: "REPUBLICAN",
        stage: "GEN",
      },
    ]);
  });

  it("does not drop candidates that have split modes when another candidate has TOTAL mode", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: "Candidate One", votes: "100", mode: "TOTAL" }),
        row({
          candidate: "Candidate Two",
          votes: "40",
          mode: "ABSENTEE",
          party_simplified: "REPUBLICAN",
          party_detailed: "REPUBLICAN",
        }),
        row({
          candidate: "Candidate Two",
          votes: "30",
          mode: "NOT ABSENTEE",
          party_simplified: "REPUBLICAN",
          party_detailed: "REPUBLICAN",
        }),
      ])
    ).toEqual([
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "012",
        candidate: "Candidate One",
        candidatevotes: "100",
        totalvotes: "170",
        party_simplified: "DEMOCRAT",
        party_detailed: "DEMOCRAT",
        stage: "GEN",
      },
      {
        year: "2024",
        state_po: "CA",
        state_fips: "06",
        office: "US HOUSE",
        district: "012",
        candidate: "Candidate Two",
        candidatevotes: "70",
        totalvotes: "170",
        party_simplified: "REPUBLICAN",
        party_detailed: "REPUBLICAN",
        stage: "GEN",
      },
    ]);
  });

  it("sums non-total modes when no TOTAL mode exists", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: "Candidate One", votes: "60", mode: "ABSENTEE" }),
        row({ candidate: "Candidate One", votes: "40", mode: "NOT ABSENTEE" }),
        row({ candidate: "Candidate Two", votes: "70", mode: "ABSENTEE", party_simplified: "REPUBLICAN" }),
      ])
    ).toMatchObject([
      {
        candidate: "Candidate One",
        candidatevotes: "100",
        totalvotes: "170",
      },
      {
        candidate: "Candidate Two",
        candidatevotes: "70",
        totalvotes: "170",
      },
    ]);
  });

  it("aggregates fusion party lines by candidate name and keeps the largest vote party", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: " Jane   Smith ", votes: "100", party_simplified: "DEMOCRAT", party_detailed: "DEMOCRAT" }),
        row({
          candidate: "JANE SMITH",
          votes: "20",
          party_simplified: "WORKING FAMILIES",
          party_detailed: "WORKING FAMILIES",
        }),
        row({ candidate: "John Jones", votes: "90", party_simplified: "REPUBLICAN", party_detailed: "REPUBLICAN" }),
      ])
    ).toMatchObject([
      {
        candidate: "Jane Smith",
        candidatevotes: "120",
        totalvotes: "210",
        party_simplified: "DEMOCRAT",
        party_detailed: "DEMOCRAT",
      },
      {
        candidate: "John Jones",
        candidatevotes: "90",
        totalvotes: "210",
        party_simplified: "REPUBLICAN",
        party_detailed: "REPUBLICAN",
      },
    ]);
  });

  it("aggregates candidate names that alternate between last-first and first-last ordering", () => {
    const result = aggregateMedslPrecinctRowsToCandidateRows([
      row({ candidate: "SIMON, LATEEFAH", votes: "100", party_simplified: "DEMOCRAT" }),
      row({ candidate: "Lateefah Simon", votes: "20", party_simplified: "DEMOCRAT" }),
      row({ candidate: "Other Candidate", votes: "90", party_simplified: "REPUBLICAN" }),
    ]);

    expect(result).toHaveLength(2);
    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          candidate: "SIMON, LATEEFAH",
          candidatevotes: "120",
          totalvotes: "210",
          party_simplified: "DEMOCRAT",
        }),
        expect.objectContaining({
          candidate: "Other Candidate",
          candidatevotes: "90",
          totalvotes: "210",
          party_simplified: "REPUBLICAN",
        }),
      ])
    );
  });

  it("excludes non-candidate buckets from candidates and total votes", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: "Candidate One", votes: "600", party_simplified: "DEMOCRAT" }),
        row({ candidate: "Candidate Two", votes: "400", party_simplified: "REPUBLICAN" }),
        row({ candidate: "UNDERVOTES", votes: "100", party_simplified: null, party_detailed: null }),
        row({ candidate: "Over Votes", votes: "50", party_simplified: null, party_detailed: null }),
        row({ candidate: "BLANK", votes: "25", party_simplified: null, party_detailed: null }),
        row({ candidate: "Total Votes", votes: "1175", party_simplified: null, party_detailed: null }),
        row({ candidate: "WRITE-IN", votes: "5", party_simplified: null, party_detailed: null, writein: "TRUE" }),
      ])
    ).toMatchObject([
      {
        candidate: "Candidate One",
        candidatevotes: "600",
        totalvotes: "1000",
      },
      {
        candidate: "Candidate Two",
        candidatevotes: "400",
        totalvotes: "1000",
      },
    ]);
  });

  it("keeps named write-in candidates", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({ candidate: "Candidate One", votes: "600", party_simplified: "DEMOCRAT" }),
        row({ candidate: "Named Write In", votes: "10", party_simplified: null, party_detailed: null, writein: "TRUE" }),
        row({ candidate: "Candidate Two", votes: "390", party_simplified: "REPUBLICAN" }),
      ])
    ).toMatchObject([
      {
        candidate: "Candidate One",
        candidatevotes: "600",
        totalvotes: "1000",
      },
      {
        candidate: "Candidate Two",
        candidatevotes: "390",
        totalvotes: "1000",
      },
      {
        candidate: "Named Write In",
        candidatevotes: "10",
        totalvotes: "1000",
      },
    ]);
  });

  it("defaults missing statewide districts to STATEWIDE", () => {
    expect(
      aggregateMedslPrecinctRowsToCandidateRows([
        row({
          office: "GOVERNOR",
          district: null,
          candidate: "Governor Candidate",
          votes: "420",
          party_simplified: "DEMOCRAT",
        }),
      ])
    ).toMatchObject([
      {
        office: "GOVERNOR",
        district: "STATEWIDE",
        candidate: "Governor Candidate",
        candidatevotes: "420",
        totalvotes: "420",
      },
    ]);
  });

  it("reports missing non-statewide districts instead of aggregating them into a shared district", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,US HOUSE,,House Democrat,DEMOCRAT,DEMOCRAT,600,GEN,TOTAL",
      "2024,CA,06,US HOUSE,,House Republican,REPUBLICAN,REPUBLICAN,400,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query: vi.fn() } as never,
        {
          csv,
          source: "MIT_2024",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      aggregatedRows: 2,
      normalizedRecords: 0,
      skippedRows: [
        {
          reason: "invalid_district",
          row: {
            candidate: "House Democrat",
            district: "",
          },
        },
        {
          reason: "invalid_district",
          row: {
            candidate: "House Republican",
            district: "",
          },
        },
      ],
    });
  });

  it("keeps special elections separate and skips them instead of merging them with regular contests", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode,special",
      "2024,CA,06,US HOUSE,012,Regular Democrat,DEMOCRAT,DEMOCRAT,600,GEN,TOTAL,FALSE",
      "2024,CA,06,US HOUSE,012,Regular Republican,REPUBLICAN,REPUBLICAN,400,GEN,TOTAL,FALSE",
      "2024,CA,06,US HOUSE,012,Special Democrat,DEMOCRAT,DEMOCRAT,300,GEN,TOTAL,TRUE",
      "2024,CA,06,US HOUSE,012,Special Republican,REPUBLICAN,REPUBLICAN,200,GEN,TOTAL,TRUE",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query: vi.fn() } as never,
        {
          csv,
          source: "MIT_2024",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 4,
      aggregatedRows: 4,
      normalizedRecords: 1,
      skippedRows: [
        {
          reason: "special_election",
          row: {
            candidate: "Special Democrat",
            special: "TRUE",
          },
        },
        {
          reason: "special_election",
          row: {
            candidate: "Special Republican",
            special: "TRUE",
          },
        },
      ],
    });
  });

  it("parses and aggregates precinct CSV in one call", () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,STATE SENATE,022,Jane Smith,DEMOCRAT,DEMOCRAT,60,GEN,TOTAL",
      "2024,CA,06,STATE SENATE,022,John Jones,REPUBLICAN,REPUBLICAN,40,GEN,TOTAL",
    ].join("\n");

    expect(parseAndAggregateMedslHistoricalContestPrecinctCsv(csv)).toMatchObject([
      {
        office: "STATE SENATE",
        district: "022",
        candidate: "Jane Smith",
        candidatevotes: "60",
        totalvotes: "100",
      },
      {
        office: "STATE SENATE",
        district: "022",
        candidate: "John Jones",
        candidatevotes: "40",
        totalvotes: "100",
      },
    ]);
  });

  it("normalizes supported precinct offices without writing in dry-run mode", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,US HOUSE,012,House Democrat,DEMOCRAT,DEMOCRAT,600,GEN,TOTAL",
      "2024,CA,06,US HOUSE,012,House Republican,REPUBLICAN,REPUBLICAN,400,GEN,TOTAL",
      "2024,CA,06,GOVERNOR,,Governor Democrat,DEMOCRAT,DEMOCRAT,550,GEN,TOTAL",
      "2024,CA,06,GOVERNOR,,Governor Republican,REPUBLICAN,REPUBLICAN,450,GEN,TOTAL",
      "2024,CA,06,STATE SENATE,022,Senate Democrat,DEMOCRAT,DEMOCRAT,700,GEN,TOTAL",
      "2024,CA,06,STATE SENATE,022,Senate Republican,REPUBLICAN,REPUBLICAN,300,GEN,TOTAL",
      "2024,CA,06,STATE HOUSE,048,Assembly Democrat,DEMOCRAT,DEMOCRAT,520,GEN,TOTAL",
      "2024,CA,06,STATE HOUSE,048,Assembly Republican,REPUBLICAN,REPUBLICAN,480,GEN,TOTAL",
    ].join("\n");
    const query = vi.fn();

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv,
          source: "MIT_2024",
          sourceUrl: "https://github.com/MEDSL/2024-elections-official",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 8,
      aggregatedRows: 8,
      normalizedRecords: 4,
      skippedRows: [],
      writeResult: null,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("writes normalized precinct records when dry-run mode is off", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,STATE HOUSE,048,Assembly Democrat,DEMOCRAT,DEMOCRAT,520,GEN,TOTAL",
      "2024,CA,06,STATE HOUSE,048,Assembly Republican,REPUBLICAN,REPUBLICAN,480,GEN,TOTAL",
    ].join("\n");
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query } as never,
        {
          csv,
          source: "MIT_2024",
          importedAt: new Date("2026-06-14T12:00:00.000Z"),
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      aggregatedRows: 2,
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
      "STATE_HOUSE",
      "state_lower",
      "06048",
    ]);
  });

  it("reports malformed precinct votes through skippedRows", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,US HOUSE,012,House Democrat,DEMOCRAT,DEMOCRAT,600,GEN,TOTAL",
      "2024,CA,06,US HOUSE,012,Bad Vote Candidate,REPUBLICAN,REPUBLICAN,not-a-number,GEN,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query: vi.fn() } as never,
        {
          csv,
          source: "MIT_2024",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      aggregatedRows: 2,
      normalizedRecords: 1,
      skippedRows: [
        {
          reason: "invalid_votes",
          row: {
            candidate: "Bad Vote Candidate",
            candidatevotes: "not-a-number",
            totalvotes: "not-a-number",
          },
        },
      ],
    });
  });

  it("reports unsupported offices and non-general stages through skippedRows", async () => {
    const csv = [
      "year,state_po,state_fips,office,district,candidate,party_simplified,party_detailed,votes,stage,mode",
      "2024,CA,06,SHERIFF,,Sheriff Candidate,DEMOCRAT,DEMOCRAT,100,GEN,TOTAL",
      "2024,CA,06,US HOUSE,012,Primary Candidate,DEMOCRAT,DEMOCRAT,100,PRI,TOTAL",
    ].join("\n");

    await expect(
      importHistoricalContestMarginsFromPrecinctCsv(
        { query: vi.fn() } as never,
        {
          csv,
          source: "MIT_2024",
          dryRun: true,
        }
      )
    ).resolves.toMatchObject({
      parsedRows: 2,
      aggregatedRows: 2,
      normalizedRecords: 0,
      skippedRows: [
        {
          reason: "unsupported_office",
          row: {
            office: "SHERIFF",
            candidate: "Sheriff Candidate",
          },
        },
        {
          reason: "non_general_stage",
          row: {
            office: "US HOUSE",
            candidate: "Primary Candidate",
            stage: "PRI",
          },
        },
      ],
    });
  });
});

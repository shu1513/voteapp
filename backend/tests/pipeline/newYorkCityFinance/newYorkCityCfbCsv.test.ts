import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  mapNewYorkCityCfbContributionRow,
  mapNewYorkCityCfbFinancialAnalysisRow,
  readNewYorkCityCfbContributions,
} from "../../../src/pipeline/newYorkCityFinance/newYorkCityCfbCsv.js";

const tempDirs: string[] = [];
afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("newYorkCityCfbCsv", () => {
  it("maps contribution and financial-analysis contracts", () => {
    expect(mapNewYorkCityCfbContributionRow({
      ELECTION: "2025", OFFICECD: "1", RECIPID: "123", RECIPNAME: "DOE, JANE", FILING: "12",
      SCHEDULE: "ABC", REFNO: "R1", NAME: "Smith, Alex", C_CODE: "IND", OCCUPATION: "Teacher",
      EMPNAME: "NYC DOE", AMNT: "1,250.50", ADJTYPECD: "",
    })).toMatchObject({ candidateId: "123", officeCode: "1", amount: 1250.5, occupation: "Teacher" });

    expect(mapNewYorkCityCfbContributionRow({
      ELECTION: "2025", OFFICECD: "1", RECIPID: "123", RECIPNAME: "DOE, JANE", FILING: "13",
      SCHEDULE: "M", REFNO: "R2", NAME: "Smith, Alex", C_CODE: "IND", OCCUPATION: "Teacher",
      EMPNAME: "NYC DOE", AMNT: "-100", ADJTYPECD: "2",
    })).toMatchObject({ amount: -100, schedule: "M", adjustmentType: "2" });

    expect(mapNewYorkCityCfbFinancialAnalysisRow({
      el_cycle: "2025", from_stmt: "1", to_stmt: "16", office: "4", cand_name: "Jane Doe", cand_id: "123",
      boro_dist: "(Bk)", net_cntns: "1000.25", pubfnd_pmt: "200", net_expnd: "400", outstanding_bills: "10",
    })).toEqual({
      electionYear: 2025, fromStatement: 1, toStatement: 16, officeCode: "4", candidateName: "Jane Doe",
      candidateId: "123", boroughCode: "K", privateContributions: 1000.25, publicFunds: 200,
      netExpenditures: 400, outstandingBills: 10,
    });
  });

  it("streams quoted commas and newlines and filters before retaining rows", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nyc-cfb-csv-"));
    tempDirs.push(dir);
    const path = join(dir, "contributions.csv");
    await writeFile(path, [
      "ELECTION,OFFICECD,RECIPID,RECIPNAME,FILING,SCHEDULE,REFNO,NAME,C_CODE,OCCUPATION,EMPNAME,AMNT,ADJTYPECD",
      '2025,1,123,"DOE, JANE",12,ABC,R1,"SMITH, ALEX",IND,"Teacher, K-12","School\nDistrict",100.00,',
      "2025,5,999,COUNCIL CANDIDATE,12,ABC,R2,DONOR,IND,LAWYER,FIRM,50.00,",
    ].join("\n"));

    const result = await readNewYorkCityCfbContributions({ filePath: path, candidateIds: new Set(["123"]) });
    expect(result.rawRowCount).toBe(2);
    expect(result.malformedRowCount).toBe(0);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ candidateName: "DOE, JANE", employer: "School\nDistrict" });
  });

  it("rejects missing required headers", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nyc-cfb-csv-"));
    tempDirs.push(dir);
    const path = join(dir, "bad.csv");
    await writeFile(path, "ELECTION,RECIPID\n2025,1\n");
    await expect(readNewYorkCityCfbContributions({ filePath: path })).rejects.toThrow("CSV missing required headers");
  });

  it("counts rows whose cell count does not match the header as malformed", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nyc-cfb-csv-"));
    tempDirs.push(dir);
    const path = join(dir, "malformed.csv");
    await writeFile(path, [
      "ELECTION,OFFICECD,RECIPID,RECIPNAME,FILING,SCHEDULE,REFNO,NAME,C_CODE,OCCUPATION,EMPNAME,AMNT,ADJTYPECD",
      "2025,1,123,DOE JANE,12,ABC,R1,DONOR,IND,TEACHER,SCHOOL,100.00,",
      "2025,1,123,DOE,JANE,12,ABC,R2,DONOR,IND,TEACHER,SCHOOL,50.00,",
    ].join("\n"));
    const result = await readNewYorkCityCfbContributions({ filePath: path });
    expect(result).toMatchObject({ rawRowCount: 2, malformedRowCount: 1 });
    expect(result.rows).toHaveLength(1);
  });
});

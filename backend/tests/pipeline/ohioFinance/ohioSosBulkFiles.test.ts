import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  isOhioSos31uExpenditureRow,
  streamOhioSosBulkFile,
  OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY,
  OHIO_SOS_CANDIDATE_COVER_FAMILY,
  OHIO_SOS_CANDIDATE_LIST_FAMILY,
  OHIO_SOS_PAC_EXPENDITURES_FAMILY,
  type OhioSosContributionRow,
  type OhioSosCoverPageRow,
  type OhioSosCandidateCommitteeListRow,
  type OhioSosExpenditureRow,
} from "../../../src/pipeline/ohioFinance/ohioSosBulkFiles.js";

// Fixtures are verbatim excerpts of the real 2026-08-04 acquisition-spike
// downloads: Windows-1252 bytes, CR-only row separators, unmodified headers.

function fixturePath(name: string): string {
  return fileURLToPath(new URL(`../../fixtures/ohioFinance/${name}`, import.meta.url));
}

async function readFixture<T>(name: string, family: Parameters<typeof streamOhioSosBulkFile<T>>[0]["family"]) {
  const rows: T[] = [];
  const stats = await streamOhioSosBulkFile<T>({
    path: fixturePath(name),
    family,
    visit: (row) => rows.push(row),
  });
  return { rows, stats };
}

describe("Ohio SoS active candidate list", () => {
  it("reads the duplicated OFFICE header, taking the second column as party", async () => {
    const { rows, stats } = await readFixture<OhioSosCandidateCommitteeListRow>(
      "act_can_list_sample.csv",
      OHIO_SOS_CANDIDATE_LIST_FAMILY
    );

    expect(stats.rowCount).toBe(5);
    expect(stats.rowSeparator).toBe("\r");
    expect(rows[0]).toEqual({
      committeeName: "CITIZENS FOR KALMBACH",
      masterKey: "15877",
      candidateFirstName: "DANIEL",
      candidateLastName: "KALMBACH",
      office: "HOUSE",
      district: "87",
      party: "REPUBLICAN",
    });
    expect(rows.map((row) => row.party)).toEqual([
      "REPUBLICAN",
      "DEMOCRAT",
      "DEMOCRAT",
      "REPUBLICAN",
      "DEMOCRAT",
    ]);
  });

  it("has no transaction dates to report", async () => {
    const { stats } = await readFixture<OhioSosCandidateCommitteeListRow>(
      "act_can_list_sample.csv",
      OHIO_SOS_CANDIDATE_LIST_FAMILY
    );
    expect(stats.minTransactionDateIso).toBeNull();
    expect(stats.maxTransactionDateIso).toBeNull();
    expect(stats.reportKeys31u).toEqual([]);
  });
});

describe("Ohio SoS candidate cover pages", () => {
  it("maps the canonical summary columns to signed cents", async () => {
    const { rows, stats } = await readFixture<OhioSosCoverPageRow>(
      "can_cover_sample.csv",
      OHIO_SOS_CANDIDATE_COVER_FAMILY
    );

    expect(stats.rowCount).toBe(4);
    expect(rows[0]).toMatchObject({
      committeeName: "OHIOANS WITH SHERROD BROWN",
      masterKey: "1",
      candidateFirstName: "SHERROD",
      candidateLastName: "BROWN",
      reportKey: "126457",
      reportYear: 1990,
      reportDescription: "PRE-PRIMARY",
      dateReportFiledIso: "1990-04-26",
      totalContributionsCents: 4_713_286,
      totalExpendituresCents: 8_386_227,
      balanceOnHandCents: 48_857_378,
      valueIndependentExpendituresCents: 0,
    });
  });

  it("keeps a negative balance on hand negative", async () => {
    const { rows } = await readFixture<OhioSosCoverPageRow>(
      "can_cover_sample.csv",
      OHIO_SOS_CANDIDATE_COVER_FAMILY
    );
    const negative = rows.find((row) => row.committeeName === "FRIENDS OF SHIRLEY HAWK");
    expect(negative?.balanceOnHandCents).toBe(-3100);
  });

  it("reports the filed-date range across the file", async () => {
    const { stats } = await readFixture<OhioSosCoverPageRow>(
      "can_cover_sample.csv",
      OHIO_SOS_CANDIDATE_COVER_FAMILY
    );
    expect(stats.minTransactionDateIso).toBe("1990-04-26");
    expect(stats.maxTransactionDateIso).toBe("2000-12-15");
    expect(stats.implausibleDateRowCount).toBe(0);
  });
});

describe("Ohio SoS candidate contributions", () => {
  it("maps contributor, committee, and candidate columns", async () => {
    const { rows, stats } = await readFixture<OhioSosContributionRow>(
      "cac_con_sample.csv",
      OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY
    );

    expect(stats.rowCount).toBe(5);
    expect(rows[0]).toMatchObject({
      committeeName: "OHIOANS FOR AMY ACTON AND DAVID PEPPER",
      masterKey: "16171",
      reportYear: 2026,
      reportKey: "504663322",
      shortDescription: "31-A Stmt of Contribution",
      contributorFirstName: "JANET",
      contributorLastName: "ABARAY",
      nonIndividual: null,
      city: "LIBERTY TWP",
      state: "OH",
      zip: "45011-8453",
      fileDateIso: "2026-04-28",
      amountCents: 250_000,
      candidateFirstName: "AMY",
      candidateLastName: "ACTON",
      office: "GOVERNOR",
      district: "100",
      party: "DEMOCRAT",
    });
  });

  it("carries the raw employer/occupation text through without interpreting it", async () => {
    const { rows } = await readFixture<OhioSosContributionRow>(
      "cac_con_sample.csv",
      OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY
    );
    expect(rows.map((row) => row.empOccupation)).toEqual([
      "RETIRED RETIRED",
      "ATTORNEY",
      "ATTORNEY - SELF",
      "NOT EMPLOYED",
      null,
    ]);
  });

  it("reads a non-individual contributor with no personal-name fields", async () => {
    const { rows } = await readFixture<OhioSosContributionRow>(
      "cac_con_sample.csv",
      OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY
    );
    expect(rows[4]).toMatchObject({
      contributorFirstName: null,
      contributorLastName: null,
      nonIndividual: "BOILERMAKERS UNION LO 744",
      amountCents: 50_000,
    });
  });
});

describe("Ohio SoS manifest diagnostics", () => {
  // Real filers mistype the year: the 2026 candidate-contribution file alone
  // carries 0202, 0206, 2926, 3026, and 3036. Letting those into the range
  // would make the manifest's date bounds meaningless.
  it("excludes implausible dates from the range and counts them instead", async () => {
    const rows = [
      "COM_NAME,MASTER_KEY,RPT_YEAR,REPORT_KEY,REPORT_DESCRIPTION,SHORT_DESCRIPTION,FIRST_NAME,MIDDLE_NAME,LAST_NAME,SUFFIX_NAME,NON_INDIVIDUAL,ADDRESS,CITY,STATE,ZIP,EXPEND_DATE,AMOUNT,EVENT_DATE,PURPOSE",
      "A,1,2026,9,POST-PRIMARY,31-B  Stmt of Expenditures,,,,,PAYEE,,,,,03/17/0202,100,,",
      "A,1,2026,9,POST-PRIMARY,31-B  Stmt of Expenditures,,,,,PAYEE,,,,,05/05/5025,100,,",
      "A,1,2026,9,POST-PRIMARY,31-B  Stmt of Expenditures,,,,,PAYEE,,,,,04/28/2026,100,,",
      "A,1,2026,9,POST-PRIMARY,31-B  Stmt of Expenditures,,,,,PAYEE,,,,,,100,,",
      "A,1,2026,9,POST-PRIMARY,31-B  Stmt of Expenditures,,,,,PAYEE,,,,,04/29/2026,,,",
      "",
    ].join("\r");
    const path = join(await mkdtemp(join(tmpdir(), "ohio-bulk-")), "PAC_EXP_TYPOS.CSV");
    await writeFile(path, rows, "latin1");

    const stats = await streamOhioSosBulkFile({
      path,
      family: OHIO_SOS_PAC_EXPENDITURES_FAMILY,
      now: new Date("2026-08-04T00:00:00Z"),
    });

    expect(stats.rowCount).toBe(5);
    expect(stats.minTransactionDateIso).toBe("2026-04-28");
    expect(stats.maxTransactionDateIso).toBe("2026-04-29");
    expect(stats.implausibleDateRowCount).toBe(2);
    expect(stats.missingDateRowCount).toBe(1);
    // Blank AMOUNT is real (blank in-kind rows, filer test rows); it is
    // counted, never read as zero.
    expect(stats.missingAmountRowCount).toBe(1);
  });
});

describe("Ohio SoS PAC expenditures", () => {
  it("collects only the report keys of 31-U independent-expenditure rows", async () => {
    const { rows, stats } = await readFixture<OhioSosExpenditureRow>(
      "pac_exp_31u_sample.csv",
      OHIO_SOS_PAC_EXPENDITURES_FAMILY
    );

    expect(stats.rowCount).toBe(6);
    // Two 31-B rows share the file; only the four 31-U rows contribute a key,
    // and their single report key is deduplicated.
    expect(stats.reportKeys31u).toEqual(["501544249"]);
    expect(rows.filter(isOhioSos31uExpenditureRow)).toHaveLength(4);
  });

  it("maps a 31-U row with no candidate, office, or direction columns", async () => {
    const { rows } = await readFixture<OhioSosExpenditureRow>(
      "pac_exp_31u_sample.csv",
      OHIO_SOS_PAC_EXPENDITURES_FAMILY
    );
    const independentExpenditure = rows.find(isOhioSos31uExpenditureRow);

    expect(independentExpenditure).toMatchObject({
      committeeName: "NATIONAL FEDERATION OF INDEPENDENT BUSINESS OHIO POLITICAL ACTION COMMITTEE",
      masterKey: "1792",
      reportKey: "501544249",
      shortDescription: "31-U Ind Exp by committee",
      nonIndividual: "NFIB",
      expendDateIso: "2026-06-03",
      amountCents: 85_518,
      purpose: "Reimbursement for Political Ads for 2026 Primary-Larry Kidd",
    });
    // Decision 4: the annual file identifies the spender but never the target.
    expect(independentExpenditure?.candidateFirstName).toBeNull();
    expect(independentExpenditure?.office).toBeNull();
  });
});

import { describe, expect, it, vi } from "vitest";

import { syncMaineCandidateFinance } from "../../../src/pipeline/maineFinance/maineCandidateFinanceSync.js";
import type {
  MaineCfisContributionRow,
  MaineCfisExpenditureRow,
} from "../../../src/pipeline/maineFinance/maineCfisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function contribution(overrides: Partial<MaineCfisContributionRow> = {}): MaineCfisContributionRow {
  return {
    OrgID: "1001",
    LegacyID: "618",
    "Committee Name": "Paul for Maine",
    "Candidate Name": "Reagan LeeAnn Paul",
    "Receipt Amount": "100.0000",
    "Receipt Date": "03/11/2024",
    Office: "Representative",
    District: "37",
    "Last Name": "Voter",
    "First Name": "Pat",
    "Middle Name": "",
    Suffix: "",
    Address1: "100 Main St",
    Address2: "",
    City: "Augusta",
    State: "ME",
    Zip: "04330",
    Description: "",
    "Receipt ID": "R-1",
    "Filed Date": "03/15/2024",
    "Report Name": "2024 Pre-General",
    "Receipt Source Type": "Individual",
    "Receipt Type": "Monetary (Itemized)",
    "Committee Type": "Candidate Committee",
    Amended: "N",
    Employer: "LARGAY LAW OFFICES, P.A.",
    Occupation: "Attorney/Legal",
    "Occupation Comment": "",
    "Employment Information Requested": "N",
    "Forgiven Loan": "N",
    ElectionType: "General",
    ...overrides,
  };
}

function expenditure(overrides: Partial<MaineCfisExpenditureRow> = {}): MaineCfisExpenditureRow {
  return {
    "Election Year": "2024",
    OrgID: "242",
    LegacyID: "611",
    "Committee Type": "Political Action Committee",
    "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
    "Candidate Name": "",
    Jurisdiction: "STATE",
    Office: "",
    District: "",
    Party: "",
    IncumbentStatus: "",
    "Financing Type": "",
    "Payee Last Name": "MEDIA VENDOR LLC",
    "Payee First Name": "",
    "Payee Middle Name": "",
    Suffix: "",
    Address1: "100 Main St",
    Address2: "",
    City: "Augusta",
    State: "ME",
    Zip: "04330",
    "Expenditure ID": "E-1",
    "Expenditure Date": "10/03/2024",
    "Expenditure Purpose": "Independent Expenditure",
    "Expenditure Amount": "1600.0000",
    Explanation: "Digital ads",
    "Date Filed": "10/04/2024",
    Amended: "N",
    "IE Report": "Y",
    "24-Hour Report": "Y",
    "Report Name": "2024 24-Hour IE",
    "Operating Expense": "N",
    "Support/Oppose Ballot Question": "",
    "Support/Oppose Candidate": "Support",
    "Ballot Question Number": "",
    "Ballot Question Description/Title": "",
    Candidate: "Paul, Reagan LeeAnn",
    "Candidate ID": "481737",
    "Candidate Jurisdiction": "STATE",
    "Candidate Office": "Representative",
    "Candidate District": "37",
    "Candidate Party": "Republican",
    "Candidate IncumbentStatus": "",
    "Candidate Financing Type": "",
    ...overrides,
  };
}

function createDb() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    client,
    db: {
      query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
      connect: vi.fn().mockResolvedValue(client),
    },
  };
}

describe("maineCandidateFinanceSync", () => {
  it("syncs a trusted Maine committee snapshot with direct and outside breakdowns", async () => {
    const { db, client } = createDb();
    const result = await syncMaineCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Reagan LeeAnn Paul",
      electionYear: 2024,
      officeScope: "state_lower",
      officeName: "Representative",
      district: "37",
      cfisCandidateId: "481737",
      sourceUrl: "https://mainecampaignfinance.com/",
      contributionSourceUrl: "https://mainecampaignfinance.com/api/DataDownload/CSVDownloadReport",
      expenditureSourceUrl: "https://mainecampaignfinance.com/api/DataDownload/CSVDownloadReport",
      now: new Date("2026-06-25T12:00:00.000Z"),
      trustedCommittee: {
        committeeId: "1001",
        committeeName: "Paul for Maine",
        sourceUrl: "https://mainecampaignfinance.com/",
      },
      contributionRows: [
        contribution(),
        contribution({
          OrgID: "242",
          LegacyID: "611",
          "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
          "Candidate Name": "",
          "Receipt Amount": "35000.0000",
          "Receipt ID": "PAC-1",
          "Last Name": "OLD CONSTRUCTION COMPANY LLC",
          "First Name": "",
          "Receipt Source Type": "Business/Organization",
          "Committee Type": "Political Action Committee",
          Occupation: "",
          Employer: "",
        }),
      ],
      expenditureRows: [expenditure()],
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2024,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 1600,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionRowCount: 1,
    });
    expect(result.resolution).toMatchObject({
      status: "matched",
      committeeId: "1001",
      source: "cfis_bulk",
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.me_candidate_finance_links"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.me_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.me_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.me_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("classifies every donor but caps the persisted donor rows per group", async () => {
    const { db, client } = createDb();
    function pacDonor(overrides: Partial<MaineCfisContributionRow>): MaineCfisContributionRow {
      return contribution({
        OrgID: "242",
        LegacyID: "611",
        "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
        "Candidate Name": "",
        "First Name": "",
        "Receipt Source Type": "Business/Organization",
        "Committee Type": "Political Action Committee",
        Occupation: "",
        Employer: "",
        ...overrides,
      });
    }

    const result = await syncMaineCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Reagan LeeAnn Paul",
      electionYear: 2024,
      officeScope: "state_lower",
      officeName: "Representative",
      district: "37",
      cfisCandidateId: "481737",
      now: new Date("2026-06-25T12:00:00.000Z"),
      trustedCommittee: {
        committeeId: "1001",
        committeeName: "Paul for Maine",
      },
      // Cap of 1: the smaller IBEW donor must be dropped from the WRITTEN
      // donor rows, yet still feed the classifications and the rebuilt
      // labor_unions industry total.
      outsideMaxBreakdownsPerCategory: 1,
      contributionRows: [
        pacDonor({ "Receipt ID": "PAC-1", "Receipt Amount": "50000.0000", "Last Name": "IBEW LOCAL 540" }),
        pacDonor({ "Receipt ID": "PAC-2", "Receipt Amount": "25000.0000", "Last Name": "IBEW LOCAL 8" }),
      ],
      expenditureRows: [expenditure()],
    });

    // 1 capped donor row + 1 industry row built from BOTH donors.
    expect(result.outsideGroupBreakdownsWritten).toBe(2);
    const breakdownInsertParams = client.query.mock.calls
      .filter((call) => String(call[0]).includes("me_candidate_finance_outside_group_breakdowns"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(breakdownInsertParams).toContain("IBEW LOCAL 540");
    expect(breakdownInsertParams).not.toContain("IBEW LOCAL 8");
    // The rebuilt industry total covers the dropped donor too.
    expect(breakdownInsertParams).toContain("labor_unions");
    expect(breakdownInsertParams).toContain(75_000);
    // Both donors persisted classification rows.
    const classificationParams = client.query.mock.calls
      .filter((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(classificationParams).toContain("IBEW LOCAL 540");
    expect(classificationParams).toContain("IBEW LOCAL 8");
  });

  it("returns unmatched without writing when resolver cannot find one committee", async () => {
    const { db } = createDb();
    const result = await syncMaineCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Reagan LeeAnn Paul",
      electionYear: 2024,
      officeScope: "state_lower",
      officeName: "Representative",
      district: "37",
      contributionRows: [contribution({ "Candidate Name": "Other Candidate" })],
      expenditureRows: [expenditure()],
    });

    expect(result.resolution).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
    expect(result.linkWritten).toBe(false);
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("supports dry runs without writing", async () => {
    const { db } = createDb();
    const result = await syncMaineCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Reagan LeeAnn Paul",
      electionYear: 2024,
      officeScope: "state_lower",
      officeName: "Representative",
      district: "37",
      contributionRows: [contribution()],
      dryRun: true,
    });

    expect(result.resolution.status).toBe("matched");
    expect(result.linkWritten).toBe(false);
    expect(result.summaryWritten).toBe(false);
    expect(result.directBreakdownsWritten).toBe(0);
    expect(result.totalReceipts).toBe(100);
    expect(db.connect).not.toHaveBeenCalled();
  });
});

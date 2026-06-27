import { describe, expect, it } from "vitest";

import {
  getIllinoisSbeExportCapStatus,
  hasIllinoisSbeExportCapWarning,
  illinoisSbeContributionRecordFromRow,
  illinoisSbeExpenditureRecordFromRow,
  parseIllinoisSbeCsvRows,
  planIllinoisSbeExportPartitions,
  splitIllinoisSbeAmountWindow,
  splitIllinoisSbeDateWindow,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

describe("illinoisSbeClient", () => {
  it("parses quoted Illinois SBE CSV rows and normalizes headers", () => {
    expect(
      parseIllinoisSbeCsvRows(
        'Contributed By,Received By,Amount\r\n"Doe, Jane\nOccupation: Attorney\nEmployer: Acme","Individual Contributions\nFriends of Jane","$1,250.50"\r\n'
      )
    ).toEqual([
      {
        contributed_by: "Doe, Jane\nOccupation: Attorney\nEmployer: Acme",
        received_by: "Individual Contributions\nFriends of Jane",
        amount: "$1,250.50",
      },
    ]);
  });

  it("maps Illinois contribution rows with contributor occupation and recipient committee", () => {
    expect(
      illinoisSbeContributionRecordFromRow({
        contributed_by: "Doe, Jane\n123 Main St\nOccupation: Attorney\nEmployer: Acme",
        received_by: "Individual Contributions\nFriends of Jane",
        amount: "$1,250.50",
        amount_received_date: "3/1/2022",
      })
    ).toMatchObject({
      contributorName: "Doe, Jane",
      contributorAddress: "123 Main St",
      occupation: "Attorney",
      employer: "Acme",
      contributionType: "Individual Contributions",
      recipientCommitteeName: "Friends of Jane",
      amount: 1250.5,
      receivedDate: "3/1/2022",
    });
  });

  it("maps Illinois independent expenditure rows conservatively", () => {
    expect(
      illinoisSbeExpenditureRecordFromRow({
        received_by: "Vendor LLC\n1 Market St",
        expended_by: "Independent Expenditures\nPeople for Schools",
        amount: "($500.00)",
        expended_by_date: "10/1/2022",
        candidate_name: "Jane Doe",
        office_district: "Governor",
        supporting_opposing: "Supporting",
      })
    ).toMatchObject({
      payeeName: "Vendor LLC",
      payeeAddress: "1 Market St",
      expenditureType: "Independent Expenditures",
      expendingCommitteeName: "People for Schools",
      amount: -500,
      expendedDate: "10/1/2022",
      candidateName: "Jane Doe",
      officeDistrict: "Governor",
      supportOppose: "support",
    });
  });

  it("detects capped export risk and proposes date or amount partitions", () => {
    expect(hasIllinoisSbeExportCapWarning("The maximum number of records available for download is 25,000.")).toBe(
      true
    );
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 25_000,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toEqual({
      rowCount: 25000,
      cap: 25000,
      capped: true,
      warningTextPresent: true,
      reason: "row_count_reached_cap",
    });
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 10,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toEqual({
      rowCount: 10,
      cap: 25000,
      capped: true,
      warningTextPresent: true,
      reason: "warning_text_present",
    });
    expect(splitIllinoisSbeDateWindow({ fromDate: "1/1/2021", toDate: "12/31/2022" })).toEqual([
      { fromDate: "1/1/2021", toDate: "12/31/2021" },
      { fromDate: "1/1/2022", toDate: "12/31/2022" },
    ]);
    expect(splitIllinoisSbeAmountWindow({ minAmount: 0, maxAmount: 1000 })).toEqual([
      { minAmount: 0, maxAmount: 500 },
      { minAmount: 500.01, maxAmount: 1000 },
    ]);
  });

  it("plans capped export partitions by date first and amount as a fallback", () => {
    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 25_000,
        fromDate: "1/1/2021",
        toDate: "12/31/2022",
        minAmount: 0,
        maxAmount: 1000,
      })
    ).toMatchObject({
      strategy: "date",
      partitions: [
        { fromDate: "1/1/2021", toDate: "12/31/2021", minAmount: 0, maxAmount: 1000 },
        { fromDate: "1/1/2022", toDate: "12/31/2022", minAmount: 0, maxAmount: 1000 },
      ],
    });

    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 100,
        resultText: "The maximum number of records available for download is 25,000.",
        minAmount: 0,
        maxAmount: 1000,
      })
    ).toMatchObject({
      strategy: "amount",
      partitions: [
        { minAmount: 0, maxAmount: 500 },
        { minAmount: 500.01, maxAmount: 1000 },
      ],
    });

    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 1,
        fromDate: "1/1/2022",
        toDate: "12/31/2022",
      })
    ).toMatchObject({
      strategy: null,
      partitions: null,
      status: { capped: false },
    });
  });
});

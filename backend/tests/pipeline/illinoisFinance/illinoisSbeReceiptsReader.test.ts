import { describe, expect, it } from "vitest";

import {
  contributionTypeFromIllinoisSbeD2Part,
  loadIllinoisSbeReceiptsByCommitteeId,
  toIllinoisSbeContributionRecordFromReceipt,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeReceiptsReader.js";

const RECEIPTS_TSV = "tests/fixtures/illinoisFinance/receipts.txt";

describe("loadIllinoisSbeReceiptsByCommitteeId", () => {
  it("keeps allow-listed, unarchived receipts within the year window", async () => {
    const result = await loadIllinoisSbeReceiptsByCommitteeId({
      path: RECEIPTS_TSV,
      committeeIds: new Set(["201"]),
      minReceiptYear: 2024,
    });

    expect(result.visitedRowCount).toBe(7);
    expect(result.archivedRowCount).toBe(1);
    expect(result.keptRowCount).toBe(4);
    expect([...result.receiptsByCommitteeId.keys()]).toEqual(["201"]);

    const records = result.receiptsByCommitteeId.get("201")!;
    expect(records.map((record) => record.contributorName)).toEqual([
      "Alice Smith",
      "Alice Smith",
      "Acme Pac",
      "Dana Doe",
    ]);
    expect(records[0]).toEqual({
      committeeId: "201",
      contributorName: "Alice Smith",
      contributorAddress: "1 Main St Aurora IL 60505",
      occupation: "Attorney",
      employer: 'Big "Q" Firm',
      amount: 500,
      receivedDate: "2025-03-01 00:00:00",
      d2Part: "1A",
      description: null,
    });
  });

  it("keeps every year when no minimum receipt year is set", async () => {
    const result = await loadIllinoisSbeReceiptsByCommitteeId({
      path: RECEIPTS_TSV,
      committeeIds: new Set(["201"]),
    });
    expect(result.keptRowCount).toBe(5);
  });

  it("returns nothing for committees outside the allow-list", async () => {
    const result = await loadIllinoisSbeReceiptsByCommitteeId({
      path: RECEIPTS_TSV,
      committeeIds: new Set(["201", "202"]),
      minReceiptYear: 2024,
    });
    expect(result.receiptsByCommitteeId.has("999")).toBe(false);
    expect(result.receiptsByCommitteeId.has("202")).toBe(false);
  });

  it("rejects an invalid minimum receipt year", async () => {
    await expect(
      loadIllinoisSbeReceiptsByCommitteeId({
        path: RECEIPTS_TSV,
        committeeIds: new Set(["201"]),
        minReceiptYear: 24,
      })
    ).rejects.toThrow("Invalid Illinois SBE receipts minReceiptYear: 24");
  });
});

describe("contributionTypeFromIllinoisSbeD2Part", () => {
  it("maps the leading D-2 part digit", () => {
    expect(contributionTypeFromIllinoisSbeD2Part("1A")).toBe("Individual Contribution");
    expect(contributionTypeFromIllinoisSbeD2Part("1B")).toBe("Individual Contribution");
    expect(contributionTypeFromIllinoisSbeD2Part("2A")).toBe("Transfer In");
    expect(contributionTypeFromIllinoisSbeD2Part("3A")).toBe("Loan Received");
    expect(contributionTypeFromIllinoisSbeD2Part("4A")).toBe("Other Receipt");
    expect(contributionTypeFromIllinoisSbeD2Part("5A")).toBe("In-Kind Contribution");
    expect(contributionTypeFromIllinoisSbeD2Part("9Z")).toBeNull();
    expect(contributionTypeFromIllinoisSbeD2Part(null)).toBeNull();
    expect(contributionTypeFromIllinoisSbeD2Part("")).toBeNull();
  });
});

describe("toIllinoisSbeContributionRecordFromReceipt", () => {
  it("builds a contribution record the direct aggregator can consume", () => {
    const record = toIllinoisSbeContributionRecordFromReceipt({
      receipt: {
        committeeId: "201",
        contributorName: "Alice Smith",
        contributorAddress: "1 Main St Aurora IL 60505",
        occupation: "Attorney",
        employer: "Big Q Firm",
        amount: 500,
        receivedDate: "2025-03-01 00:00:00",
        d2Part: "1A",
        description: null,
      },
      recipientCommitteeName: "Aurora Forward",
      sourceUrl: "https://example.test/bulk",
    });

    expect(record).toEqual({
      contributorName: "Alice Smith",
      contributorAddress: "1 Main St Aurora IL 60505",
      occupation: "Attorney",
      employer: "Big Q Firm",
      amount: 500,
      receivedDate: "2025-03-01 00:00:00",
      reportReceivedDate: null,
      contributionType: "Individual Contribution",
      recipientCommitteeName: "Aurora Forward",
      description: null,
      vendorName: null,
      vendorAddress: null,
      sourceUrl: "https://example.test/bulk",
    });
  });
});

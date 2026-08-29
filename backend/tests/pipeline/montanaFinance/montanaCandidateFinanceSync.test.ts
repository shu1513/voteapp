import { describe, expect, it, vi } from "vitest";

import type {
  MontanaCersDetailRow,
  MontanaCersExportRow,
  MontanaCersReportDetailArtifact,
  MontanaCersReportInventoryRow,
} from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import { MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  syncMontanaCandidateFinance,
  type MontanaCandidateFinanceArtifacts,
  type MontanaOutsideSpendingArtifacts,
} from "../../../src/pipeline/montanaFinance/montanaCandidateFinanceSync.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function detailRow(overrides: Partial<MontanaCersDetailRow>): MontanaCersDetailRow {
  return {
    amountTypeDescr: "Primary",
    cashAmtCents: 0,
    inKindAmtCents: 0,
    totalAmtCents: 0,
    debtAmtCents: 0,
    entityName: "Doe, Jane",
    occupationDescr: null,
    employerDescr: null,
    datePaid: null,
    lineItemCompositeDescr: null,
    purposeDescr: null,
    electioneeringInd: "N",
    candidateContrInd: "N",
    ...overrides,
  };
}

function emptyArtifact(reportId: number): MontanaCersReportDetailArtifact {
  const lists = Object.fromEntries(
    MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS.map((name) => [name, [] as MontanaCersDetailRow[]])
  ) as MontanaCersReportDetailArtifact["lists"];
  return { reportId, lists };
}

function inventoryRow(overrides: Partial<MontanaCersReportInventoryRow>): MontanaCersReportInventoryRow {
  return {
    reportId: 1,
    entitySubId: 21020,
    formTypeCode: "C5",
    formTypeDescr: null,
    fromDateStr: "01/01/2026",
    toDateStr: "03/15/2026",
    reportTypeDescr: "Periodic",
    statusCode: "FILED",
    statusDescr: "Filed",
    primCashBegCents: 0,
    genCashBegCents: 0,
    receivedDate: 1_000,
    amendedDate: null,
    ...overrides,
  };
}

function csvRow(overrides: Partial<MontanaCersExportRow>): MontanaCersExportRow {
  return {
    candidateId: 21020,
    candidateName: "Bedey, David F.",
    reportingDateRange: "01/01/2026 - 03/15/2026",
    entityName: "Doe, Jane",
    occupation: "Retired",
    employer: "Retired",
    datePaid: "01/01/2026",
    purpose: null,
    description: null,
    lineItem: "Individual Contributions",
    amountCents: 10_000,
    electionType: "Primary",
    amountSubtype: "Cash",
    officeTitle: "Senate District No. 43",
    ...overrides,
  };
}

function artifacts(): MontanaCandidateFinanceArtifacts {
  const artifact = emptyArtifact(1);
  artifact.lists.individual = [detailRow({ cashAmtCents: 10_000, totalAmtCents: 10_000 })];
  const first = inventoryRow({ reportId: 1 });
  const second = inventoryRow({
    reportId: 2,
    fromDateStr: "03/16/2026",
    toDateStr: "04/15/2026",
    primCashBegCents: 10_000,
    receivedDate: 2_000,
  });
  return {
    inventory: [first, second],
    contributionRows: [csvRow({})],
    expenditureRows: [],
    detailArtifactsByReportId: new Map([
      [1, artifact],
      [2, emptyArtifact(2)],
    ]),
    sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
  };
}

function writingDb() {
  const answer = (sql: unknown) => {
    if (String(sql).includes("INSERT INTO public.mt_candidate_finance_links")) {
      return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 1 });
  };
  const client = { query: vi.fn(answer), release: vi.fn() };
  const db = { query: vi.fn(answer), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function baseInput(db: { query: unknown; connect: unknown }) {
  return {
    db: db as never,
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "David Bedey",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "43",
    committee: {
      committeeId: "21020",
      committeeName: "Bedey, David F.",
      linkSource: "cers_portal" as const,
      sourceUrl: null,
    },
    now: new Date("2026-08-28T00:00:00.000Z"),
  };
}

describe("syncMontanaCandidateFinance", () => {
  it("aggregates injected artifacts and writes one snapshot", async () => {
    const { db, client } = writingDb();
    const result = await syncMontanaCandidateFinance({ ...baseInput(db), artifacts: artifacts() });
    expect(result.status).toBe("synced");
    expect(result.canonicalReportCount).toBe(2);
    expect(result.aggregation?.directContributionTotal).toBe(100);
    expect(result.aggregation?.cashOnHand).toBe(100);
    const linkInsert = client.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO public.mt_candidate_finance_links")
    );
    expect(linkInsert?.[1]).toContain("21020");
    expect(linkInsert?.[1]).toContain("DAVID BEDEY");
  });

  it("reports no_filed_reports, writes no money, and stamps the checked-at rotation row", async () => {
    const { db, client } = writingDb();
    const input = artifacts();
    const result = await syncMontanaCandidateFinance({
      ...baseInput(db),
      artifacts: {
        ...input,
        // Only an Incorporated C7: registration exists, nothing countable filed.
        inventory: [
          inventoryRow({ reportId: 9, formTypeCode: "C7", statusCode: "INCRP", statusDescr: "Incorporated" }),
        ],
        detailArtifactsByReportId: new Map(),
      },
    });
    expect(result.status).toBe("no_filed_reports");
    expect(result.summaryWritten).toBe(false);
    expect(result.checkedAtStamped).toBe(true);
    // The snapshot writer is never invoked...
    expect(client.query).not.toHaveBeenCalled();
    // ...but the guarded checked-at stamp lands, so the due list rotates
    // past sub-$500 no-filers instead of starving real filers. The guard
    // must refuse to touch a summary that carries any money.
    const stamp = db.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO public.mt_candidate_finance_summaries")
    );
    expect(String(stamp?.[0])).toContain("ON CONFLICT (link_id, election_year) DO UPDATE");
    expect(String(stamp?.[0])).toContain("total_receipts IS NULL");
    expect(String(stamp?.[0])).toContain("direct_contribution_total IS NULL");
    expect(stamp?.[1]).toEqual([LINK_ID, 2026, new Date("2026-08-28T00:00:00.000Z")]);
  });

  it("does not stamp checked-at during a no_filed_reports dry run", async () => {
    const { db, client } = writingDb();
    const input = artifacts();
    const result = await syncMontanaCandidateFinance({
      ...baseInput(db),
      dryRun: true,
      artifacts: { ...input, inventory: [], detailArtifactsByReportId: new Map() },
    });
    expect(result.status).toBe("no_filed_reports");
    expect(result.checkedAtStamped).toBe(false);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("does not write in dry-run mode", async () => {
    const { db, client } = writingDb();
    const result = await syncMontanaCandidateFinance({ ...baseInput(db), artifacts: artifacts(), dryRun: true });
    expect(result.status).toBe("synced");
    expect(result.summaryWritten).toBe(false);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("throws on a broken chain and preserves the prior snapshot", async () => {
    const { db, client } = writingDb();
    const input = artifacts();
    const inventory = [...input.inventory];
    inventory[1] = inventoryRow({
      reportId: 2,
      fromDateStr: "03/16/2026",
      toDateStr: "04/15/2026",
      primCashBegCents: 5_000,
      receivedDate: 2_000,
    });
    await expect(
      syncMontanaCandidateFinance({ ...baseInput(db), artifacts: { ...input, inventory } })
    ).rejects.toThrow("cash-begin chain failed");
    expect(client.query).not.toHaveBeenCalled();
  });

  it("fails closed on unexpected report statuses and ineligible offices", async () => {
    const { db } = writingDb();
    const input = artifacts();
    await expect(
      syncMontanaCandidateFinance({
        ...baseInput(db),
        artifacts: { ...input, inventory: [...input.inventory, inventoryRow({ reportId: 8, statusCode: "DRAFT" })] },
      })
    ).rejects.toThrow("unexpected status");
    await expect(
      syncMontanaCandidateFinance({
        ...baseInput(db),
        officeScope: "county",
        officeName: "Sheriff",
        artifacts: input,
      })
    ).rejects.toThrow("not Montana-finance eligible");
  });

  it("rejects a non-numeric stored committee id", async () => {
    const { db } = writingDb();
    await expect(
      syncMontanaCandidateFinance({
        ...baseInput(db),
        committee: { committeeId: "C1234", committeeName: "X", linkSource: "manual" },
        artifacts: artifacts(),
      })
    ).rejects.toThrow("Invalid Montana CERS entity id");
  });
});

describe("syncMontanaCandidateFinance outside leg", () => {
  function outsideArtifacts(): MontanaOutsideSpendingArtifacts {
    return {
      sweep: {
        year: 2026,
        committees: [
          {
            committeeId: 100,
            committeeName: "Good PAC",
            committeeTypeCode: "IN",
            committeeTypeDescr: "Independent",
            electionYear: null,
          },
        ],
        transactionsByCommitteeId: new Map([
          [
            100,
            [
              {
                transId: 1,
                transTypeDescr: "Independent Expenditure",
                amountTypeDescr: "Primary" as const,
                cashAmtCents: 12_345,
                inKindAmtCents: 0,
                totalAmtCents: 12_345,
                datePaid: Date.UTC(2026, 4, 1),
                candidateIssue: "David Bedey (SD-43)",
                purposeDescr: "Mailers",
                electioneeringInd: "N" as const,
              },
            ],
          ],
        ]),
      },
      registrationRows: [
        {
          candidateId: 21020,
          lastName: "Bedey",
          firstName: "David",
          middleInitial: "F.",
          electionYear: 2026,
          officeTitle: "Senate District No. 43",
          officeCode: "236",
          partyDescr: "Republican",
          candidateStatusDescr: "Active",
          resCountyDescr: null,
        },
      ],
      sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
    };
  }

  it("writes resolved outside totals and groups alongside the direct snapshot", async () => {
    const { db, client } = writingDb();
    const result = await syncMontanaCandidateFinance({
      ...baseInput(db),
      artifacts: artifacts(),
      outsideArtifacts: outsideArtifacts(),
    });
    expect(result.outsideSpendingSkippedReason).toBeNull();
    expect(result.outsideSpending).toMatchObject({ supportTotal: 123.45, opposeTotal: null });
    expect(result.outsideGroupsWritten).toBe(1);
    const summaryInsert = client.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO public.mt_candidate_finance_summaries")
    );
    expect(summaryInsert?.[1]).toContain(123.45);
    const groupInsert = client.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO public.mt_candidate_finance_outside_groups")
    );
    expect(groupInsert?.[1]).toContain("100");
    expect(groupInsert?.[1]).toContain("Good PAC");
    expect(groupInsert?.[1]).toContain("support");
  });

  it("skips the outside leg and preserves the prior snapshot when the bundle is unavailable", async () => {
    const { db, client } = writingDb();
    const result = await syncMontanaCandidateFinance({
      ...baseInput(db),
      artifacts: artifacts(),
      outsideArtifacts: null,
    });
    expect(result.outsideSpending).toBeNull();
    expect(result.outsideSpendingSkippedReason).toMatch(/unavailable/);
    expect(result.outsideGroupsWritten).toBe(0);
    // No outside-group statements at all: undefined input means the writer
    // neither upserts nor stale-deletes, so a prior outside snapshot stays.
    const touchesGroups = client.query.mock.calls.some(([sql]) =>
      String(sql).includes("mt_candidate_finance_outside_groups")
    );
    expect(touchesGroups).toBe(false);
  });

  it("clears stale groups when a present sweep resolves nothing for the candidate", async () => {
    const { db, client } = writingDb();
    const bundle = outsideArtifacts();
    bundle.sweep.transactionsByCommitteeId.set(100, []);
    const result = await syncMontanaCandidateFinance({
      ...baseInput(db),
      artifacts: artifacts(),
      outsideArtifacts: bundle,
    });
    expect(result.outsideSpending).toMatchObject({ supportTotal: null, opposeTotal: null, outsideGroups: [] });
    // An EMPTY groups array still runs the stale-delete pass.
    const deletesGroups = client.query.mock.calls.some(([sql]) =>
      String(sql).includes("DELETE FROM public.mt_candidate_finance_outside_groups")
    );
    expect(deletesGroups).toBe(true);
  });
});

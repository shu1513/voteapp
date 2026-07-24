import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildOpenFecCandidateCommitteesUrl,
  buildOpenFecCandidateTotalsUrl,
  buildOpenFecCommitteeAggregateUrl,
  buildOpenFecCommitteeTotalsUrl,
  buildOpenFecOutsideSpendingGroupsByCandidateUrl,
  buildOpenFecOutsideSpendingTotalsByCandidateUrl,
  getCandidateTotals,
  getCommitteeAggregatesByEmployer,
  getCommitteeAggregatesByOccupation,
  getCommitteeTotals,
  getOutsideSpendingTotalsByCandidate,
  listCandidateCommittees,
  listOutsideSpendingGroupsByCandidate,
} from "../../../src/pipeline/finance/openFecFinanceClient.js";
import { OpenFecClientError } from "../../../src/pipeline/presidential/openFecClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("openFecFinanceClient", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("builds candidate and committee finance URLs without API keys", () => {
    const candidateTotalsUrl = new URL(buildOpenFecCandidateTotalsUrl(" p80001571 ", 2024));
    expect(candidateTotalsUrl.origin + candidateTotalsUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/candidate/P80001571/totals/"
    );
    expect(candidateTotalsUrl.searchParams.get("cycle")).toBe("2024");
    expect(candidateTotalsUrl.searchParams.has("api_key")).toBe(false);

    const candidateCommitteesUrl = new URL(buildOpenFecCandidateCommitteesUrl("H0CA12000", 2024));
    expect(candidateCommitteesUrl.origin + candidateCommitteesUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/candidate/H0CA12000/committees/"
    );
    expect(candidateCommitteesUrl.searchParams.get("per_page")).toBe("100");

    const committeeTotalsUrl = new URL(buildOpenFecCommitteeTotalsUrl(" c00867937 ", 2024));
    expect(committeeTotalsUrl.origin + committeeTotalsUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/committee/C00867937/totals/"
    );
  });

  it("builds contribution aggregate and outside-spending URLs", () => {
    const employerUrl = new URL(
      buildOpenFecCommitteeAggregateUrl({ committeeId: "C00867937", electionYear: 2024, type: "employer", perPage: 25 })
    );
    expect(employerUrl.origin + employerUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/schedules/schedule_a/by_employer/"
    );
    expect(employerUrl.searchParams.get("committee_id")).toBe("C00867937");
    expect(employerUrl.searchParams.get("two_year_transaction_period")).toBe("2024");
    expect(employerUrl.searchParams.get("per_page")).toBe("25");
    expect(employerUrl.searchParams.get("sort")).toBe("-total");

    const outsideTotalsUrl = new URL(buildOpenFecOutsideSpendingTotalsByCandidateUrl("P80001571", 2024));
    expect(outsideTotalsUrl.origin + outsideTotalsUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/schedules/schedule_e/totals/by_candidate/"
    );
    expect(outsideTotalsUrl.searchParams.get("candidate_id")).toBe("P80001571");

    const outsideGroupsUrl = new URL(
      buildOpenFecOutsideSpendingGroupsByCandidateUrl({
        fecCandidateId: "P80001571",
        electionYear: 2024,
        supportOppose: "support",
        perPage: 10,
      })
    );
    expect(outsideGroupsUrl.origin + outsideGroupsUrl.pathname).toBe(
      "https://api.open.fec.gov/v1/schedules/schedule_e/by_candidate/"
    );
    expect(outsideGroupsUrl.searchParams.get("support_oppose_indicator")).toBe("S");
    expect(outsideGroupsUrl.searchParams.get("per_page")).toBe("10");
    expect(outsideGroupsUrl.searchParams.get("sort")).toBe("-total");
  });

  it("parses candidate totals", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        results: [
          {
            receipts: "-1103506.85",
            disbursements: 1118067.55,
            last_cash_on_hand_end_period: 4682993.3,
            last_debts_owed_by_committee: 12270000,
            individual_itemized_contributions: 457010,
            individual_unitemized_contributions: 18400.23,
            other_political_committee_contributions: 85500,
            transfers_from_other_authorized_committee: 1250,
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      getCandidateTotals("H2MI13204", 2026, { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual({
      fecCandidateId: "H2MI13204",
      electionYear: 2026,
      totalReceipts: -1103506.85,
      totalDisbursements: 1118067.55,
      cashOnHand: 4682993.3,
      debtsOwed: 12270000,
      individualItemizedTotal: 457010,
      individualUnitemizedTotal: 18400.23,
      otherCommitteeContributions: 85500,
      transfersFromAffiliatedCommittees: 1250,
      sourceUrl: "https://www.fec.gov/data/candidate/H2MI13204/?cycle=2026",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.searchParams.get("api_key")).toBe("k1");
  });

  it("parses candidate committees and committee totals", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          results: [
            {
              committee_id: "c00867937",
              name: "Trump 47 Committee, Inc.",
              designation: "J",
              designation_full: "Joint fundraising committee",
              committee_type: "P",
              committee_type_full: "Presidential",
              cycles: [2024, "2028"],
            },
          ],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: [
            {
              receipts: 100,
              disbursements: 50,
              cash_on_hand_end_period: 25,
              debts_owed_by_committee: 5,
            },
          ],
        })
      ) as unknown as typeof fetch;

    await expect(
      listCandidateCommittees("P80001571", 2024, { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        committeeId: "C00867937",
        name: "Trump 47 Committee, Inc.",
        designation: "J",
        designationFull: "Joint fundraising committee",
        committeeType: "P",
        committeeTypeFull: "Presidential",
        cycles: [2024, 2028],
        sourceUrl: "https://www.fec.gov/data/committee/C00867937/",
      },
    ]);

    await expect(
      getCommitteeTotals("C00867937", 2024, { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual({
      committeeId: "C00867937",
      electionYear: 2024,
      totalReceipts: 100,
      totalDisbursements: 50,
      cashOnHand: 25,
      debtsOwed: 5,
      sourceUrl: "https://www.fec.gov/data/committee/C00867937/?cycle=2024",
    });
  });

  it("parses employer and occupation aggregates", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          results: [
            { employer: "Google", total: 3121141.49, count: 1000 },
            { employer: "", total: 99 },
          ],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: [{ occupation: "Attorney", contribution_receipt_amount: "47061962.12", contribution_count: 2000 }],
        })
      ) as unknown as typeof fetch;

    await expect(
      getCommitteeAggregatesByEmployer(
        { committeeId: "C00703975", electionYear: 2024, perPage: 5 },
        { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        type: "employer",
        label: "Google",
        amount: 3121141.49,
        count: 1000,
        sourceUrl:
          "https://www.fec.gov/data/receipts/individual-contributions/?committee_id=C00703975&two_year_transaction_period=2024",
      },
    ]);

    await expect(
      getCommitteeAggregatesByOccupation(
        { committeeId: "C00703975", electionYear: 2024, perPage: 5 },
        { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        type: "occupation",
        label: "Attorney",
        amount: 47061962.12,
        count: 2000,
        sourceUrl:
          "https://www.fec.gov/data/receipts/individual-contributions/?committee_id=C00703975&two_year_transaction_period=2024",
      },
    ]);
  });

  it("parses outside spending totals and groups", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          results: [
            { support_oppose_indicator: "S", total: 237654179.2 },
            { support_oppose_indicator: "O", total: "158532691.81" },
          ],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: [
            {
              committee_id: "C00492140",
              committee_name: "AB PAC",
              support_oppose_indicator: "O",
              total: 58551575.41,
              count: 180,
            },
            {
              committee_id: "C00825851",
              committee_name: "Make America Great Again Inc.",
              support_oppose_indicator: "S",
              total: 57695784.7,
              count: 42,
            },
          ],
        })
      ) as unknown as typeof fetch;

    await expect(
      getOutsideSpendingTotalsByCandidate("P80001571", 2024, { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      supportTotal: 237654179.2,
      opposeTotal: 158532691.81,
      sourceUrl: "https://www.fec.gov/data/independent-expenditures/?candidate_id=P80001571&cycle=2024",
    });

    await expect(
      listOutsideSpendingGroupsByCandidate(
        { fecCandidateId: "P80001571", electionYear: 2024, supportOppose: "support", perPage: 10 },
        { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        committeeId: "C00825851",
        committeeName: "Make America Great Again Inc.",
        supportOppose: "support",
        amount: 57695784.7,
        count: 42,
        sourceUrl:
          "https://www.fec.gov/data/independent-expenditures/?candidate_id=P80001571&committee_id=C00825851&cycle=2024&support_oppose_indicator=S",
      },
    ]);
  });

  it("rejects invalid finance inputs before fetch", () => {
    expect(() => buildOpenFecCandidateTotalsUrl("X00000001", 2024)).toThrow(OpenFecClientError);
    expect(() => buildOpenFecCommitteeTotalsUrl("P80001571", 2024)).toThrow(OpenFecClientError);
    expect(() => buildOpenFecCandidateTotalsUrl("P80001571", 1800)).toThrow(OpenFecClientError);
    expect(() =>
      buildOpenFecOutsideSpendingGroupsByCandidateUrl({ fecCandidateId: "P80001571", electionYear: 2024, perPage: 101 })
    ).toThrow(OpenFecClientError);
  });
});

import { describe, expect, it, vi } from "vitest";

import {
  MINNESOTA_CFB_FINANCIAL_SUMMARY_API_URL,
  buildMinnesotaCandidateFinancialSummaryUrl,
  fetchMinnesotaCandidateFinancialSummary,
  parseMinnesotaCandidateFinancialSummaryHtml,
} from "../../../src/pipeline/minnesotaFinance/minnesotaCandidateFinancialSummaryClient.js";

const DEMUTH_SUMMARY_HTML = `
  <div class="row">
    <table class="spec-spreadsheet">
      <tr><th colspan="2">2026 - Election year</th></tr>
      <tr><th>Individuals contributions</th><td>$216,391.85</td></tr>
      <tr><th>Lobbyist contributions</th><td>$6,275.00</td></tr>
      <tr><th>Committee/fund contributions</th><td>$3,100.00</td></tr>
      <tr><th>Party unit contributions</th><td>$0.00</td></tr>
      <tr><th>Other contributions</th><td>$0.00</td></tr>
      <tr><th>Total receipts</th><td>$225,802.93</td></tr>
      <tr><th>Total expenditures</th><td>$124,941.19</td></tr>
    </table>
    <table class="spec-spreadsheet">
      <tr><th colspan='2'>2025 - Election year</th></tr>
      <tr><th>Individuals contributions</th><td>$409,460.13</td></tr>
      <tr><th>Lobbyist contributions</th><td>$22,300.00</td></tr>
      <tr><th>Committee/fund contributions</th><td>$10,750.00</td></tr>
      <tr><th>Party unit contributions</th><td>$41,900.00</td></tr>
      <tr><th>Other contributions</th><td>$0.00</td></tr>
      <tr><th>Total receipts</th><td>$484,411.53</td></tr>
      <tr><th>Total expenditures</th><td>$41,331.05</td></tr>
    </table>
  </div>
`;

describe("Minnesota candidate financial summary client", () => {
  it("parses and sums the authoritative two-year CFB summary", () => {
    expect(
      parseMinnesotaCandidateFinancialSummaryHtml({
        committeeId: "19287",
        electionYear: 2026,
        html: DEMUTH_SUMMARY_HTML,
      })
    ).toEqual({
      committeeId: "19287",
      electionYear: 2026,
      totalReceipts: 710_214.46,
      directContributionTotal: 710_176.98,
      totalDisbursements: 166_272.24,
      sourceUrl: buildMinnesotaCandidateFinancialSummaryUrl({ committeeId: "19287", electionYear: 2026 }),
    });
  });

  it("rejects an incomplete summary instead of writing a misleading subtotal", () => {
    expect(() =>
      parseMinnesotaCandidateFinancialSummaryHtml({
        committeeId: "19287",
        electionYear: 2026,
        html: DEMUTH_SUMMARY_HTML.replace(
          "<tr><th>Other contributions</th><td>$0.00</td></tr>",
          ""
        ),
      })
    ).toThrow("missing Other contributions for 2026");
  });

  it("returns null when CFB explicitly reports no data for the segment", () => {
    expect(
      parseMinnesotaCandidateFinancialSummaryHtml({
        committeeId: "19388",
        electionYear: 2026,
        html: `
          <div class="col-md-6">Data not available for 2026</div>
          <div class="col-md-6">Data not available for 2025</div>
        `,
      })
    ).toBeNull();
  });

  it("establishes a CFB session before requesting the financial tab", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("<html>candidate</html>", {
          status: 200,
          headers: { "set-cookie": "PHPSESSID=test-session; Path=/; HttpOnly" },
        })
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ tabcontent: DEMUTH_SUMMARY_HTML }), {
          status: 200,
          headers: { "content-type": "application/json" },
        })
      );

    await expect(
      fetchMinnesotaCandidateFinancialSummary(
        { committeeId: "19287", electionYear: 2026 },
        { fetchImpl: fetchImpl as typeof fetch }
      )
    ).resolves.toMatchObject({
      totalReceipts: 710_214.46,
      directContributionTotal: 710_176.98,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(fetchImpl.mock.calls[0]?.[0]).toBe(
      buildMinnesotaCandidateFinancialSummaryUrl({ committeeId: "19287", electionYear: 2026 })
    );
    expect(fetchImpl.mock.calls[1]?.[0]).toBe(MINNESOTA_CFB_FINANCIAL_SUMMARY_API_URL);
    const postInit = fetchImpl.mock.calls[1]?.[1] as RequestInit;
    expect(postInit.method).toBe("POST");
    expect(new Headers(postInit.headers).get("cookie")).toBe("PHPSESSID=test-session");
    const body = new URLSearchParams(String(postInit.body));
    expect(Object.fromEntries(body)).toEqual({
      id: "19287",
      year: "2026",
      "year_data[ElectionSegmentEndDate]": "2026",
      "year_data[ElectionSegmentStartDate]": "2025",
      tabname: "financial",
    });
  });
});

import { describe, expect, it } from "vitest";
import {
  getSanFranciscoCandidateTargetedSpending,
  getSanFranciscoCommitteeSummaryRows,
  moneyStringToCents,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";

function jsonFetch(rows: unknown[]): typeof fetch {
  return async () =>
    new Response(JSON.stringify(rows), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
}

describe("moneyStringToCents", () => {
  it("converts decimal strings exactly", () => {
    expect(moneyStringToCents("4841006.0")).toBe(484100600);
    expect(moneyStringToCents("92.4")).toBe(9240);
    expect(moneyStringToCents("10917642.52")).toBe(1091764252);
    expect(moneyStringToCents("0")).toBe(0);
    expect(moneyStringToCents("-25.5")).toBe(-2550);
  });

  it("rounds a sub-cent third digit on the magnitude", () => {
    expect(moneyStringToCents("1.005")).toBe(101);
    expect(moneyStringToCents("1.004")).toBe(100);
    expect(moneyStringToCents("-1.005")).toBe(-101);
  });

  it("rejects non-money text", () => {
    expect(moneyStringToCents("n/a")).toBeNull();
    expect(moneyStringToCents("$5")).toBeNull();
    expect(moneyStringToCents("")).toBeNull();
    expect(moneyStringToCents(undefined)).toBeNull();
  });
});

describe("getSanFranciscoCommitteeSummaryRows", () => {
  const row = (overrides: Record<string, unknown>) => ({
    filing_nid: "1",
    filing_id_number: "100",
    filing_type: "FiledOriginal",
    form_type: "FPPC460",
    start_date: "2024-01-01T00:00:00.000",
    end_date: "2024-06-30T00:00:00.000",
    line_5_col_a: "896998.68",
    line_11_col_a: "1107012.93",
    ...overrides,
  });

  it("maps summary rows to cents", async () => {
    const rows = await getSanFranciscoCommitteeSummaryRows(
      { fppcId: "1463099" },
      { fetchImpl: jsonFetch([row({})]) },
    );
    expect(rows).toEqual([
      {
        filingNid: "1",
        filingIdNumber: "100",
        filingType: "FiledOriginal",
        formType: "FPPC460",
        periodStart: "2024-01-01T00:00:00.000",
        periodEnd: "2024-06-30T00:00:00.000",
        contributionsCents: 89699868,
        expendituresCents: 110701293,
      },
    ]);
  });

  it("fails loudly when the current-version-only guarantee breaks", async () => {
    await expect(
      getSanFranciscoCommitteeSummaryRows(
        { fppcId: "1463099" },
        {
          fetchImpl: jsonFetch([
            row({}),
            row({ filing_id_number: "101" }),
          ]),
        },
      ),
    ).rejects.toThrow(/duplicate filing_nid 1/);
  });

  it("rejects malformed FPPC ids", async () => {
    await expect(
      getSanFranciscoCommitteeSummaryRows(
        { fppcId: "abc" },
        { fetchImpl: jsonFetch([]) },
      ),
    ).rejects.toThrow(/Invalid San Francisco FPPC id/);
  });
});

describe("getSanFranciscoCandidateTargetedSpending", () => {
  it("maps grouped rows and tolerates missing ids", async () => {
    const rows = await getSanFranciscoCandidateTargetedSpending(
      {
        candidateLastName: "Wong",
        transactionDateFrom: "2024-06-02",
        transactionDateTo: "2026-07-02",
      },
      {
        fetchImpl: jsonFetch([
          {
            fppc_id: "1488188",
            filer_name: "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
            form_type: "F496",
            support_oppose_code: "S",
            amount: "209221.7",
            transaction_count: "12",
          },
          {
            filer_name: "Unregistered Spender",
            form_type: "D",
            amount: "not-money",
          },
          {
            fppc_id: "pending",
            filer_name: "Pending Committee",
            form_type: "D",
            support_oppose_code: "",
            amount: "1680.19",
            transaction_count: "16",
          },
        ]),
      },
    );
    expect(rows).toEqual([
      {
        spenderFppcId: "1488188",
        spenderName: "GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026",
        formType: "F496",
        supportOpposeCode: "S",
        amountCents: 20922170,
        transactionCount: 12,
      },
      {
        spenderFppcId: null,
        spenderName: "Pending Committee",
        formType: "D",
        supportOpposeCode: null,
        amountCents: 168019,
        transactionCount: 16,
      },
    ]);
  });

  it("orders grouped pages with a total order and keeps paging past filtered rows", async () => {
    const requestedUrls: string[] = [];
    const pageLimit = 2;
    // Page 1 is "full" (rawCount = pageLimit) but one element is malformed
    // and filtered out — pagination must still fetch page 2.
    const pages = [
      [
        {
          fppc_id: "1488188",
          filer_name: "GROWSF",
          form_type: "F496",
          support_oppose_code: "S",
          amount: "1.00",
          transaction_count: "1",
        },
        "not-an-object",
      ],
      [],
    ];
    const fetchImpl: typeof fetch = async (input) => {
      requestedUrls.push(String(input));
      return new Response(JSON.stringify(pages.shift() ?? []), { status: 200 });
    };
    const rows = await getSanFranciscoCandidateTargetedSpending(
      { candidateLastName: "Wong" },
      { fetchImpl, pageLimit },
    );
    expect(rows).toHaveLength(1);
    expect(requestedUrls).toHaveLength(2);
    expect(new URL(requestedUrls[0]!).searchParams.get("$order")).toBe(
      "amount DESC,fppc_id,filer_name,form_type,support_oppose_code",
    );
  });

  it("rejects malformed transaction dates", async () => {
    await expect(
      getSanFranciscoCandidateTargetedSpending(
        { candidateLastName: "Wong", transactionDateFrom: "06/02/2026" },
        { fetchImpl: jsonFetch([]) },
      ),
    ).rejects.toThrow(/Invalid San Francisco transaction date/);
  });
});

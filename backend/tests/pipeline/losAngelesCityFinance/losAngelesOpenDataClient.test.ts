import { describe, expect, it, vi } from "vitest";
import { getLosAngelesCommitteeContributions } from "../../../src/pipeline/losAngelesCityFinance/losAngelesOpenDataClient.js";

describe("Los Angeles Open Data client", () => {
  it("uses bounded stable paging and retries 429", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          statusText: "Too Many Requests",
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify([
            {
              cmt_id: "1471359",
              cmt_nm: "Bass",
              schedule: "A",
              con_amount: "10.00",
              con_amount_pd_forgiven: "0.00",
            },
          ]),
        ),
      )
      .mockResolvedValueOnce(new Response("[]"));
    const rows = await getLosAngelesCommitteeContributions(
      { committeeId: "1471359", electionDate: "2026-06-02" },
      { fetchImpl, pageLimit: 1, maxPages: 3 },
    );
    expect(rows).toHaveLength(1);
    const url = new URL(String(fetchImpl.mock.calls[1]?.[0]));
    expect(url.searchParams.get("$order")).toContain(":id");
    expect(url.searchParams.get("$where")).toContain(
      "election_date='2026-06-02T00:00:00.000'",
    );
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });
});

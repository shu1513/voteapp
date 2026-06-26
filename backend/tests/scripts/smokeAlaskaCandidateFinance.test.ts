import { afterEach, describe, expect, it, vi } from "vitest";

import { runAlaskaCandidateFinanceLiveSmoke } from "../../src/scripts/smokeAlaskaCandidateFinance.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("smokeAlaskaCandidateFinance", () => {
  it("skips live smoke in CI unless explicitly allowed", async () => {
    vi.stubEnv("CI", "true");

    const output = await runAlaskaCandidateFinanceLiveSmoke({
      now: new Date("2026-01-02T03:04:05.000Z"),
    });

    expect(output).toMatchObject({
      type: "alaska_candidate_finance_live_smoke",
      ts: "2026-01-02T03:04:05.000Z",
      ok: true,
      skipped: true,
      checks: [expect.objectContaining({ name: "ci_guard", passed: true })],
    });
  });

  it("runs the live smoke checks against mocked APOC CSV responses", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          [
            "Filer,Filer Type,Name,Date,Type,Contributor/Vendor,Occupation,Employer,Amount,Status",
            "DUNLEAVY FOR GOVERNOR,Candidate,Mike Dunleavy,10/01/2022,Income,Pat Smith,Engineer,North Co,100.00,Complete",
          ].join("\n"),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          [
            "Filer Name,Filer,Filer Type,Report Year,Type,Date,Position,Candidate/Proposition,Amount,Status",
            "Alaska Future PAC,8001,Group,2022,Expenditure,09/15/2022,Support,Mike Dunleavy,25000.00,Complete",
          ].join("\n"),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          [
            "Filer Name,Filer,Filer Type,Report Year,Type,Date,Contributor,Employer,Occupation,Amount,Status",
            "Alaska Future PAC,8001,Group,2022,Contribution,09/01/2022,Northern Energy LLC,,,30000.00,Complete",
          ].join("\n"),
          { status: 200 }
        )
      );

    const output = await runAlaskaCandidateFinanceLiveSmoke({
      fetchFn,
      allowInCi: true,
      now: new Date("2026-01-02T03:04:05.000Z"),
    });

    expect(output.ok).toBe(true);
    expect(output.skipped).toBe(false);
    expect(output.data_source).toMatchObject({
      mode: "live",
    });
    expect(output.probe?.direct_campaign.matched_row_count).toBe(1);
    expect(fetchFn).toHaveBeenCalledTimes(3);
  });
});

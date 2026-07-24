import { describe, expect, it, vi } from "vitest";

import {
  buildOklahomaGuardianCandidateDetailUrl,
  fetchOklahomaGuardianCandidateDetail,
  parseOklahomaGuardianCandidateDetailHtml,
} from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianCandidateDetail.js";

const DETAIL_HTML = `
  <span id="ctl00_Content_lblCandName">JOHN CHRISTOPHER PFEIFFER</span>
  <span id="ctl00_Content_lblCandID">11813</span>
  <span id="ctl00_Content_lblCandOffice">COMMISSIONER OF LABOR</span>
  <span id="ctl00_Content_lblCandDistrict"></span>
  <span id="ctl00_Content_dgdCampaigns_ctl02_lblElection">2026 NOVEMBER GENERAL ELECTION</span>
  <span id="ctl00_Content_dgdCampaigns_ctl02_lblElectionCycle">2026 ELECTION CYCLE (4 YEAR) BEGINNING 2022</span>
`;

describe("oklahomaGuardianCandidateDetail", () => {
  it("builds a fixed official candidate-detail URL", () => {
    expect(buildOklahomaGuardianCandidateDetailUrl(" 11813 ")).toBe(
      "https://guardian.ok.gov/PublicSite/SearchPages/OrganizationDetail.aspx?OrganizationID=11813"
    );
    expect(() => buildOklahomaGuardianCandidateDetailUrl("11813&other=true")).toThrow(
      "Invalid Oklahoma Guardian organization ID"
    );
  });

  it("parses identity, office, district, and election-cycle metadata", () => {
    expect(
      parseOklahomaGuardianCandidateDetailHtml({
        html: DETAIL_HTML,
        organizationId: "11813",
      })
    ).toEqual({
      organizationId: "11813",
      candidateName: "JOHN CHRISTOPHER PFEIFFER",
      officeName: "COMMISSIONER OF LABOR",
      district: null,
      electionYears: [2026],
      sourceUrl:
        "https://guardian.ok.gov/PublicSite/SearchPages/OrganizationDetail.aspx?OrganizationID=11813",
    });
  });

  it("rejects a response for a different organization", () => {
    expect(() =>
      parseOklahomaGuardianCandidateDetailHtml({ html: DETAIL_HTML, organizationId: "11409" })
    ).toThrow("candidate detail ID mismatch");
  });

  it("rejects HTML missing required candidate metadata", () => {
    expect(() =>
      parseOklahomaGuardianCandidateDetailHtml({
        html: `<span id="ctl00_Content_lblCandID">11813</span>`,
        organizationId: "11813",
      })
    ).toThrow("Incomplete Oklahoma Guardian candidate detail");
  });

  it("fetches and parses an official candidate-detail page", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(DETAIL_HTML, { status: 200, headers: { "content-type": "text/html" } })
    );

    await expect(fetchOklahomaGuardianCandidateDetail({ organizationId: "11813", fetchImpl })).resolves.toMatchObject({
      organizationId: "11813",
      officeName: "COMMISSIONER OF LABOR",
      electionYears: [2026],
    });
    expect(fetchImpl).toHaveBeenCalledWith(
      "https://guardian.ok.gov/PublicSite/SearchPages/OrganizationDetail.aspx?OrganizationID=11813",
      expect.objectContaining({ headers: { accept: "text/html,application/xhtml+xml" } })
    );
  });

  it("rejects non-OK Guardian responses with the HTTP status", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(new Response("unavailable", { status: 503 }));

    await expect(fetchOklahomaGuardianCandidateDetail({ organizationId: "11813", fetchImpl })).rejects.toThrow(
      "HTTP 503"
    );
  });

  it("reports Guardian request timeouts", async () => {
    const fetchImpl = vi.fn((_url: string | URL | Request, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("aborted", "AbortError"));
        });
      })
    );

    await expect(
      fetchOklahomaGuardianCandidateDetail({ organizationId: "11813", fetchImpl, timeoutMs: 1 })
    ).rejects.toThrow("timed out after 1ms");
  });

  it("preserves response-body read failures", async () => {
    const response = new Response(DETAIL_HTML, { status: 200 });
    vi.spyOn(response, "text").mockRejectedValue(new Error("body decode failed"));

    await expect(
      fetchOklahomaGuardianCandidateDetail({
        organizationId: "11813",
        fetchImpl: vi.fn().mockResolvedValue(response),
      })
    ).rejects.toThrow("body decode failed");
  });
});

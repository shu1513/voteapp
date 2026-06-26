import { describe, expect, it, vi } from "vitest";

import {
  buildPennsylvaniaIndependentExpenditureElectionPostbackBody,
  fetchPennsylvaniaIndependentExpenditures,
  parsePennsylvaniaIndependentExpenditureDataJson,
  parsePennsylvaniaIndependentExpenditureElectionOptions,
  parsePennsylvaniaIndependentExpenditureHiddenFields,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaIndependentExpenditureClient.js";

const LANDING_HTML = `
  <html>
    <body>
      <form method="post">
        <input type="hidden" name="__VIEWSTATE" value="view-state-1" />
        <input type="hidden" name="__EVENTVALIDATION" value="event-validation-1" />
        <select name="ctl00$MainContent$ddlElection" id="ddlElection">
          <option value="">Select</option>
          <option value="2024G">2024 General</option>
          <option value="2026G" selected>2026 General</option>
        </select>
        <script>
          var dataJson = [
            {
              "CandidateQuestion": "Jane Doe",
              "Organization": "Pennsylvanians for Action",
              "Amount": "$10,000.00",
              "IsSupported": true,
              "IsOpposed": false,
              "ElectionID": "2026G"
            }
          ];
        </script>
      </form>
    </body>
  </html>
`;

describe("pennsylvaniaIndependentExpenditureClient", () => {
  it("parses direct dataJson rows from the ASP.NET page", () => {
    expect(parsePennsylvaniaIndependentExpenditureDataJson(LANDING_HTML)).toEqual([
      {
        CandidateQuestion: "Jane Doe",
        Organization: "Pennsylvanians for Action",
        Amount: "$10,000.00",
        IsSupported: true,
        IsOpposed: false,
        ElectionID: "2026G",
      },
    ]);
  });

  it("parses JSON.parse encoded dataJson rows", () => {
    const encodedHtml = `
      <script>
        var dataJson = JSON.parse('[{\\u0022CandidateQuestion\\u0022:\\u0022Jane Doe\\u0022,\\u0022Organization\\u0022:\\u0022PA &amp; Action\\u0022,\\u0022Amount\\u0022:\\u0022250\\u0022}]');
      </script>
    `;

    expect(parsePennsylvaniaIndependentExpenditureDataJson(encodedHtml)).toEqual([
      {
        CandidateQuestion: "Jane Doe",
        Organization: "PA & Action",
        Amount: "250",
      },
    ]);
  });

  it("parses raw dataJson arrays without HTML-decoding the JSON source", () => {
    const rawArrayHtml = `
      <script>
        var dataJson = [{"CandidateQuestion":"Jane Doe","Organization":"PA &quot;Action&quot;","Amount":"250"}];
      </script>
    `;

    expect(parsePennsylvaniaIndependentExpenditureDataJson(rawArrayHtml)).toEqual([
      {
        CandidateQuestion: "Jane Doe",
        Organization: "PA &quot;Action&quot;",
        Amount: "250",
      },
    ]);
  });

  it("parses hidden fields and election options", () => {
    expect(parsePennsylvaniaIndependentExpenditureHiddenFields(LANDING_HTML)).toEqual(
      new Map([
        ["__VIEWSTATE", "view-state-1"],
        ["__EVENTVALIDATION", "event-validation-1"],
      ])
    );
    expect(parsePennsylvaniaIndependentExpenditureElectionOptions(LANDING_HTML)).toEqual([
      { id: "2024G", label: "2024 General", selected: false },
      { id: "2026G", label: "2026 General", selected: true },
    ]);
  });

  it("builds an ASP.NET postback body for an election selection", () => {
    const body = buildPennsylvaniaIndependentExpenditureElectionPostbackBody({
      html: LANDING_HTML,
      electionId: "2024G",
    });

    expect(body.get("__VIEWSTATE")).toBe("view-state-1");
    expect(body.get("__EVENTVALIDATION")).toBe("event-validation-1");
    expect(body.get("__EVENTTARGET")).toBe("ctl00$MainContent$ddlElection");
    expect(body.get("__EVENTARGUMENT")).toBe("");
    expect(body.get("ctl00$MainContent$ddlElection")).toBe("2024G");
  });

  it("fetches the landing page and posts back when an election is requested", async () => {
    const postbackHtml = LANDING_HTML.replace("Pennsylvanians for Action", "Future PA PAC");
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(LANDING_HTML, { status: 200 }))
      .mockResolvedValueOnce(new Response(postbackHtml, { status: 200 }));

    const result = await fetchPennsylvaniaIndependentExpenditures(
      {
        url: "https://www.campaignfinanceonline.pa.gov/pages/IndependentExpenditures.aspx",
        electionId: "2024G",
      },
      { fetchImpl: fetchMock as unknown as typeof fetch }
    );

    expect(result).toMatchObject({
      sourceUrl: "https://www.campaignfinanceonline.pa.gov/pages/IndependentExpenditures.aspx",
      electionId: "2024G",
      rows: [
        expect.objectContaining({
          Organization: "Future PA PAC",
        }),
      ],
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    const [, postInit] = fetchMock.mock.calls[1] ?? [];
    expect(postInit?.method).toBe("POST");
    expect(String(postInit?.body)).toContain("ctl00%24MainContent%24ddlElection=2024G");
  });
});

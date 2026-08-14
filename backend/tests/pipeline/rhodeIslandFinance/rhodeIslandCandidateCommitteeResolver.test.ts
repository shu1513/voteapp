import { describe, expect, it } from "vitest";

import {
  normalizeRhodeIslandCandidateNameForStorage,
  resolveRhodeIslandCandidateCommitteeRows,
  rhodeIslandLastNameSearchToken,
  rhodeIslandOrganizationNameMatchesCandidate,
  searchAndResolveRhodeIslandCandidateCommittee,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandCandidateCommitteeResolver.js";
import {
  createErtsTransport,
  type ErtsHttpResponse,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsClient.js";
import type { ErtsOrganizationSearchRow } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandErtsParsers.js";

function row(overrides: Partial<ErtsOrganizationSearchRow> = {}): ErtsOrganizationSearchRow {
  return {
    organizationName: "DANIEL J MCKEE",
    postbackTarget: "dgdOrgSearchResults$ctl03$lnkOrgID",
    status: "Active",
    ...overrides,
  };
}

function resolve(overrides: {
  candidateName?: string;
  officeScope?: string;
  officeName?: string;
  searchRows?: ErtsOrganizationSearchRow[];
  searchHasMorePages?: boolean;
}) {
  return resolveRhodeIslandCandidateCommitteeRows({
    candidateName: "Daniel McKee",
    officeScope: "statewide",
    officeName: "Governor",
    searchRows: [row()],
    searchHasMorePages: false,
    ...overrides,
  });
}

describe("rhodeIslandLastNameSearchToken", () => {
  it("takes the final normalized token, flipping comma forms and dropping suffixes", () => {
    expect(rhodeIslandLastNameSearchToken("Daniel J. McKee")).toBe("MCKEE");
    expect(rhodeIslandLastNameSearchToken("McKee, Daniel J.")).toBe("MCKEE");
    expect(rhodeIslandLastNameSearchToken("Lester L. Wilks Jr.")).toBe("WILKS");
    expect(rhodeIslandLastNameSearchToken("Mary Van Dyke")).toBe("DYKE");
  });
});

describe("normalizeRhodeIslandCandidateNameForStorage", () => {
  it("normalizes, strips suffixes and parentheticals", () => {
    expect(normalizeRhodeIslandCandidateNameForStorage("Daniel J. McKee, Jr. (Dan)")).toBe("DANIEL J MCKEE");
  });
});

describe("rhodeIslandOrganizationNameMatchesCandidate", () => {
  it("matches with and without a middle initial, and through nicknames", () => {
    expect(rhodeIslandOrganizationNameMatchesCandidate("DANIEL J MCKEE", "Daniel McKee")).toBe(true);
    expect(rhodeIslandOrganizationNameMatchesCandidate("DANIEL J MCKEE", "Daniel J. McKee")).toBe(true);
    expect(rhodeIslandOrganizationNameMatchesCandidate("DANIEL J MCKEE", "Dan McKee")).toBe(true);
  });

  it("rejects a middle-name conflict, a suffix conflict, and a different surname", () => {
    expect(rhodeIslandOrganizationNameMatchesCandidate("DANIEL J MCKEE", "Daniel A. McKee")).toBe(false);
    expect(rhodeIslandOrganizationNameMatchesCandidate("JOHN SMITH SR", "John Smith Jr.")).toBe(false);
    expect(rhodeIslandOrganizationNameMatchesCandidate("TIMOTHY L MCKEE", "Daniel McKee")).toBe(false);
  });

  it("never matches a committee-style name by substring", () => {
    expect(rhodeIslandOrganizationNameMatchesCandidate("FRIENDS OF DANIEL MCKEE", "Daniel McKee")).toBe(false);
  });
});

describe("resolveRhodeIslandCandidateCommitteeRows", () => {
  it("matches the single Active row and counts the Inactive ones it passed over", () => {
    const resolution = resolve({
      searchRows: [
        row(),
        row({ organizationName: "TIMOTHY L MCKEE", postbackTarget: "dgdOrgSearchResults$ctl04$lnkOrgID" }),
        row({ status: "Inactive", postbackTarget: "dgdOrgSearchResults$ctl05$lnkOrgID" }),
      ],
    });
    expect(resolution).toEqual({
      status: "matched",
      match: {
        organizationName: "DANIEL J MCKEE",
        postbackTarget: "dgdOrgSearchResults$ctl03$lnkOrgID",
        status: "Active",
      },
      inactiveMatchCount: 1,
    });
  });

  it("refuses a paginated search — the rows are an incomplete slice", () => {
    expect(resolve({ searchHasMorePages: true })).toMatchObject({
      status: "unmatched",
      reason: "paginated_search_results",
    });
  });

  it("refuses an office outside the eligible list", () => {
    expect(resolve({ officeScope: "place", officeName: "Mayor" })).toMatchObject({
      status: "unmatched",
      reason: "unsupported_office",
    });
  });

  it("refuses when only an Inactive registration matches", () => {
    expect(resolve({ searchRows: [row({ status: "Inactive" })] })).toMatchObject({
      status: "unmatched",
      reason: "no_active_organization_match",
    });
  });

  it("treats an unknown status as not-Active instead of widening the gate", () => {
    expect(resolve({ searchRows: [row({ status: "Pending" })] })).toMatchObject({
      status: "unmatched",
      reason: "no_active_organization_match",
    });
  });

  it("reports two Active matches as ambiguous — row order is not identity evidence", () => {
    const resolution = resolve({
      searchRows: [row(), row({ postbackTarget: "dgdOrgSearchResults$ctl04$lnkOrgID" })],
    });
    expect(resolution).toMatchObject({
      status: "ambiguous",
      reason: "multiple_active_organization_matches",
    });
  });

  it("reports no name match and a missing candidate name distinctly", () => {
    expect(resolve({ searchRows: [row({ organizationName: "TIMOTHY L MCKEE" })] })).toMatchObject({
      status: "unmatched",
      reason: "no_organization_match",
    });
    expect(resolve({ candidateName: "  " })).toMatchObject({
      status: "unmatched",
      reason: "missing_candidate_name",
    });
  });
});

describe("searchAndResolveRhodeIslandCandidateCommittee", () => {
  const SEARCH_GRID =
    '<table id="dgdOrgSearchResults">' +
    "<tr><td>Organization Name</td><td>Address</td><td>City</td><td>State</td><td>Status</td></tr>" +
    "<tr><td><a href=\"javascript:__doPostBack('dgdOrgSearchResults$ctl03$lnkOrgID','')\">DANIEL J MCKEE</a></td>" +
    "<td>12 HILLSIDE ROAD</td><td>CUMBERLAND</td><td>RI</td><td>Active</td></tr>" +
    '<tr align="right"><td colspan="5"><span>1</span></td></tr>' +
    "</table>";

  function fakePortal() {
    const posts: Array<URLSearchParams | undefined> = [];
    const html = (body: string, finalUrl?: string): ErtsHttpResponse => ({
      status: 200,
      finalUrl: finalUrl ?? "https://www.ricampaignfinance.com/RIPublic/",
      contentType: "text/html",
      body: new TextEncoder().encode(body),
    });
    const transport = createErtsTransport({
      fetch: async (_url, body) => {
        posts.push(body);
        if (!body) return html('<input type="hidden" name="__VIEWSTATE" value="vs" />');
        if (body.has("lnkSearchOrg"))
          return html('<input name="txtOrgLastName" /><input type="hidden" name="__VIEWSTATE" value="vs" />');
        if (body.has("lnkSubSearchOrg"))
          return html(`${SEARCH_GRID}<input type="hidden" name="__VIEWSTATE" value="vs" />`);
        if (body.get("__EVENTTARGET")?.startsWith("dgdOrgSearchResults"))
          return html('<input type="hidden" name="__VIEWSTATE" value="vs" />');
        if (body.has("btnSearch"))
          return html(
            "<html>report</html>",
            "https://www.ricampaignfinance.com/RIPublic/Reporting/TransactionReport.aspx?OrgID=2235"
          );
        throw new Error(`Unexpected post: ${body.toString()}`);
      },
      sleep: async () => {},
    });
    return { transport, posts };
  }

  const input = {
    candidateName: "Daniel McKee",
    officeScope: "statewide",
    officeName: "Governor",
    cycleBeginUs: "01/01/2025",
    cycleEndUs: "12/31/2026",
  };

  it("searches by surname token, selects the matched row, and reads the OrgID off the redirect", async () => {
    const { transport, posts } = fakePortal();
    const resolution = await searchAndResolveRhodeIslandCandidateCommittee(input, transport);
    expect(resolution).toEqual({
      status: "matched",
      orgId: "2235",
      organizationName: "DANIEL J MCKEE",
      searchLastName: "MCKEE",
      confidence: "exact",
      source: "erts_organization_search",
      sourceUrl: "https://www.ricampaignfinance.com/RIPublic/Contributions.aspx",
      inactiveMatchCount: 0,
    });
    // 5 requests: entry, panel, search, select, dated search.
    expect(posts).toHaveLength(5);
    expect(posts[2]?.get("txtOrgLastName")).toBe("MCKEE");
    expect(posts[3]?.get("__EVENTTARGET")).toBe("dgdOrgSearchResults$ctl03$lnkOrgID");
  });

  it("stops after the search when nothing resolves — no row is ever selected", async () => {
    const { transport, posts } = fakePortal();
    const resolution = await searchAndResolveRhodeIslandCandidateCommittee(
      { ...input, candidateName: "Zachary McKee" },
      transport
    );
    expect(resolution).toMatchObject({ status: "unmatched", reason: "no_organization_match" });
    expect(posts).toHaveLength(3);
  });
});

import { describe, expect, it } from "vitest";
import { parseHoustonLegacySearchResults } from "../../../src/pipeline/houstonFinance/houstonLegacyCampaignFinanceClient.js";

describe("Houston legacy search parser", () => {
  it("extracts report identity from the official results grid", () => {
    const cells = ["", "", "John", "Whitmire", "COH", "7/17/2023", "114213", "COH", "2023", "7/17/2023 8:26 AM", "", "", "", "15110"];
    const html = `<table id="ctl00_ContentPlaceHolder1_grdCandidate"><tr><td>header</td></tr><tr><td><a href="javascript:__doPostBack('ctl00$ContentPlaceHolder1$grdCandidate','Select$2')">view</a></td>${cells.slice(1).map((cell) => `<td>${cell}</td>`).join("")}</tr></table>`;
    expect(parseHoustonLegacySearchResults(html)).toEqual([expect.objectContaining({ reportId: "114213", filerName: "John Whitmire", filerType: "COH", campaignYear: 2023, legacySelectionIndex: 2 })]);
  });
});

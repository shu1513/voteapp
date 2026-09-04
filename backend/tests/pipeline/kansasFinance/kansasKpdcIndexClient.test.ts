import { describe, expect, it } from "vitest";

import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import {
  buildKansasKpdcUrl,
  fetchKansasKpdcIndexPage,
  fetchKansasKpdcPdf,
  kansasKpdcCandidateTreePath,
  kansasKpdcStatewideFilerPrefix,
  parseKansasKpdcCandidateRows,
  parseKansasKpdcFileName,
  parseKansasKpdcIndexPdfLinks,
} from "../../../src/pipeline/kansasFinance/kansasKpdcIndexClient.js";

// Live shape of the hand-authored House link tree (unclosed <td>s, styled
// spans, absolute hrefs on BARE kansas.gov), captured 2026-08-28.
const HOUSE_INDEX_FIXTURE = `
<table>
  <tr>
    <td class="table1column1"><p><span>District</span></p>
    <td class="table1column2"><p><span>Candidate</span></p>
  </tr>
  <tr>
    <td class="table1column1" style="background-color: #cccccc" align="center" valign="top">
      <p><span style="font-size: 11pt"><span style="font-weight: bold">01</span></span></p>
    <td class="table1column2" width="11%" valign="top">
      <p><span style="font-size: 11pt">Helwig, Dale</span></p>
    <td class="table1column4"><span><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H001DH_AT.pdf">AT</a></span></td>
    <td class="table1column5"><span><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/202607/H001DH_202607.pdf">202607</a></span></td>
  </tr>
  <tr>
    <td><p>No filings this row</p></td>
    <td><a href="http://get.adobe.com/reader/otherversions/">Adobe</a></td>
  </tr>
  <tr>
    <td><p>96</p>
    <td><p>Miller, Jane</p>
    <td><a href="https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H096JM2_AT.pdf">AT</a></td>
    <td><a href="https://evil.example.com/H096JM2_202607.pdf">202607</a></td>
  </tr>
</table>`;

describe("buildKansasKpdcUrl", () => {
  it("resolves relative paths against CFAScanned and pins www.kansas.gov", () => {
    expect(buildKansasKpdcUrl("House/2026ElecCycle/HLinks2026EC.htm")).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"
    );
    expect(buildKansasKpdcUrl("https://kansas.gov/ethics/CFAScanned/x.pdf")).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/x.pdf"
    );
  });

  it("rewrites the dead ethics.ks.gov host onto www.kansas.gov/ethics", () => {
    // 91 live House-tree links (amendments, affidavits) still use this host.
    expect(buildKansasKpdcUrl("http://ethics.ks.gov/CFAScanned/House/2026ElecCycle/202607/H003DP_amend2607.pdf")).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/202607/H003DP_amend2607.pdf"
    );
  });

  it("upgrades http on allowed hosts and rejects everything else", () => {
    expect(buildKansasKpdcUrl("http://www.kansas.gov/ethics/CFAScanned/x.pdf")).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/x.pdf"
    );
    expect(() => buildKansasKpdcUrl("https://evil.example.com/x.pdf")).toThrow("not a KPDC CFAScanned URL");
    expect(() => buildKansasKpdcUrl("https://ethics.ks.gov/other/x.pdf")).toThrow("not a KPDC CFAScanned URL");
    expect(() => buildKansasKpdcUrl("ftp://www.kansas.gov/ethics/x.pdf")).toThrow("not a KPDC CFAScanned URL");
  });

  it("pins the archive boundary: no path escapes, no ports, no credentials", () => {
    // URL resolution normalizes "..", so a traversal lands outside the
    // /ethics/CFAScanned/ prefix and is refused.
    expect(() => buildKansasKpdcUrl("../outside.pdf")).toThrow("not a KPDC CFAScanned URL");
    expect(() => buildKansasKpdcUrl("https://www.kansas.gov/other/x.pdf")).toThrow(
      "not a KPDC CFAScanned URL"
    );
    expect(() => buildKansasKpdcUrl("https://www.kansas.gov:8443/ethics/CFAScanned/x.pdf")).toThrow(
      "not a KPDC CFAScanned URL"
    );
    expect(() => buildKansasKpdcUrl("https://user:pw@www.kansas.gov/ethics/CFAScanned/x.pdf")).toThrow(
      "not a KPDC CFAScanned URL"
    );
    // Interior dot segments that STAY inside the archive are fine.
    expect(buildKansasKpdcUrl("House/../House/2026ElecCycle/x.pdf")).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/x.pdf"
    );
  });
});

describe("parseKansasKpdcIndexPdfLinks", () => {
  const links = parseKansasKpdcIndexPdfLinks(
    HOUSE_INDEX_FIXTURE,
    "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"
  );

  it("collects PDF links with normalized URLs and row context", () => {
    expect(links).toHaveLength(3);
    expect(links[0]).toEqual({
      url: "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H001DH_AT.pdf",
      fileName: "H001DH_AT.pdf",
      linkText: "AT",
      rowText: "01 Helwig, Dale AT 202607",
    });
    expect(links[1]!.fileName).toBe("H001DH_202607.pdf");
    expect(links[1]!.rowText).toBe("01 Helwig, Dale AT 202607");
  });

  it("skips non-PDF anchors and off-site PDF links", () => {
    expect(links.map((link) => link.fileName)).toEqual([
      "H001DH_AT.pdf",
      "H001DH_202607.pdf",
      "H096JM2_AT.pdf",
    ]);
  });
});

describe("parseKansasKpdcFileName", () => {
  it.each([
    ["H001DH_AT.pdf", "H001DH", "appointment_of_treasurer", null, null],
    ["H001DH_202607.pdf", "H001DH", "report", "202607", null],
    ["H003DP_amend2607.pdf", "H003DP", "report", "202607", 1],
    ["SW05AB_amendAT.pdf", "SW05AB", "appointment_of_treasurer", null, 1],
    ["H065SC_2026PLF.pdf", "H065SC", "last_minute", null, null],
    ["H010XY_Aff2607.pdf", "H010XY", "affidavit", "202607", null],
    ["H011ZQ_Term2607.pdf", "H011ZQ", "termination", "202607", null],
    // IE statements: two underscores — the filer code keeps the first one.
    ["IE_KC1_2607.pdf", "IE_KC1", "report", "202607", null],
    // Filer codes can carry disambiguating digits.
    ["H096JM2_AT.pdf", "H096JM2", "appointment_of_treasurer", null, null],
    // 2024 trees: numbered replacements, the general last-minute report, lowercase affidavits.
    ["H001JJ_2amend2410.pdf", "H001JJ", "report", "202410", 2],
    ["S01CB_4amend2410.pdf", "S01CB", "report", "202410", 4],
    ["S01CB_2amendAT.pdf", "S01CB", "appointment_of_treasurer", null, 2],
    ["H044BB_2024GLF.pdf", "H044BB", "last_minute", null, null],
    ["H010XY_aff2407.pdf", "H010XY", "affidavit", "202407", null],
  ] as const)("classifies %s", (fileName, filerCode, kind, periodKey, amendmentOrdinal) => {
    const info = parseKansasKpdcFileName(fileName);
    expect(info.filerCode).toBe(filerCode);
    expect(info.kind).toBe(kind);
    expect(info.periodKey).toBe(periodKey);
    expect(info.amendment).toBe(amendmentOrdinal !== null);
    expect(info.amendmentOrdinal).toBe(amendmentOrdinal);
  });

  it("returns kind unknown instead of guessing", () => {
    expect(parseKansasKpdcFileName("HLinks2026EC.htm").kind).toBe("unknown");
    expect(parseKansasKpdcFileName("H001DH_mystery.pdf").kind).toBe("unknown");
  });
});

// Live 2026 House shape: the second <tr> is missing between Smith and Woody
// (their cells run on), a note cell follows Woody, and one district cell has
// no name cell at all. Synthetic names on purpose.
const MERGED_ROWS_FIXTURE = `
<table>
  <tr><td class="table1column1"><p>District</p><td class="table1column2"><p>Candidate</p></tr>
  <tr>
    <td class="table1column1" align="center"><p><span><span style="font-weight: bold">33</span></span></p></td>
    <td class="table1column2"><p><span> Holloway, Margaret </span></p></td>
    <td><span><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H033MH_AT.pdf">AT</a></span></td>
    <td class="table1column5">
    <td>&nbsp;</td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/202607/H033MH_202607.pdf"><font>202607</font></a></td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/LastMinute/H033MH_2026PLF.pdf">PLF</a></td>
  </tr>
  <td class="table1column1"><p><span style="font-weight: bold">33</span></p></td>
  <td class="table1column2"><p> Brunson, Steven </p></td>
  <td><a href="http://ethics.ks.gov/CFAScanned/House/2026ElecCycle/Treasurers/H031SB_AT.pdf">AT</a></td>
  <td>N/A</td>
  <td><p>Candidate is now in District #31</p></td>
  <tr>
    <td><p>34</p></td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H034ZZ_AT.pdf">AT</a></td>
    <td><a href="https://evil.example.com/H034ZZ_202607.pdf">202607</a></td>
  </tr>
</table>`;

// Live 2026 statewide shape: section headings, no district cells, one
// SW0n prefix per office.
const STATEWIDE_FIXTURE = `
<table>
  <tr><td colspan="5"><b><font color="#0000FF">GUBERNATORIAL CANDIDATES</font></b></td><td>&nbsp;</td></tr>
  <tr>
    <td class="table1column2" width="18%">Rowan, Stacy</td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/Treasurers/SW01SR_AT.pdf">AT</a></td>
    <td>N/A</td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/202601/SW01SR_202601.pdf">202601</a></td>
  </tr>
  <tr>
    <td class="table1column2" width="18%">Van Dyke, Mary Ann</td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/Treasurers/SW01MV_AT.pdf">AT</a></td>
  </tr>
  <tr><td colspan="5"><b><font color="#0000FF">ATTORNEY GENERAL CANDIDATES</font></b></td></tr>
  <tr>
    <td>Rowan, Stacy</td>
    <td><a href="https://kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/Treasurers/SW02SR_AT.pdf">AT</a></td>
  </tr>
</table>`;

describe("parseKansasKpdcCandidateRows", () => {
  it("walks cells so filers sharing one <tr> stay separate, and keeps nameless district cells", () => {
    const { rows, orphanLinks } = parseKansasKpdcCandidateRows(
      MERGED_ROWS_FIXTURE,
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"
    );
    expect(orphanLinks).toBe(0);
    expect(rows.map((row) => [row.district, row.filedName, row.links.map((link) => link.fileName)])).toEqual([
      [33, "Holloway, Margaret", ["H033MH_AT.pdf", "H033MH_202607.pdf", "H033MH_2026PLF.pdf"]],
      [33, "Brunson, Steven", ["H031SB_AT.pdf"]],
      [34, "", ["H034ZZ_AT.pdf"]],
    ]);
    // The dead host is rewritten; the off-site link is dropped.
    expect(rows[1]!.links[0]!.url).toBe("https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/Treasurers/H031SB_AT.pdf");
    expect(rows[0]!.links[1]!.linkText).toBe("202607");
  });

  it("starts a new filer at each name cell on the statewide tree and counts links before any filer", () => {
    const { rows } = parseKansasKpdcCandidateRows(
      STATEWIDE_FIXTURE,
      "https://www.kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/SWLinks2026EC.htm"
    );
    expect(rows.map((row) => [row.district, row.filedName, row.links.length])).toEqual([
      [null, "Rowan, Stacy", 2],
      [null, "Van Dyke, Mary Ann", 1],
      [null, "Rowan, Stacy", 1],
    ]);
    const orphan = parseKansasKpdcCandidateRows(
      `<table><tr><td><a href="https://kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/x_AT.pdf">AT</a></td></tr></table>`,
      "https://www.kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/SWLinks2026EC.htm"
    );
    expect(orphan).toEqual({ rows: [], orphanLinks: 1 });
  });
});

describe("candidate tree paths", () => {
  const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
  const senate = kansasCfrOfficeForRace({ officeScope: "state_upper", officeCanonicalName: "State Senator" })!;
  const attorneyGeneral = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Attorney General" })!;

  it("names the House, Senate (regular and special), and statewide trees", () => {
    expect(kansasKpdcCandidateTreePath(house, 2026)).toBe("House/2026ElecCycle/HLinks2026EC.htm");
    expect(kansasKpdcCandidateTreePath(senate, 2028)).toBe("Senate/2028ElecCycle/SLinks2028EC.htm");
    expect(kansasKpdcCandidateTreePath(senate, 2026)).toBe("Senate/2026SpecialElection/SLinks2026SpecialElection.htm");
    expect(kansasKpdcCandidateTreePath(attorneyGeneral, 2022)).toBe("StWide/2022ElecCycle/SWLinks2022EC.htm");
    expect(() => kansasKpdcCandidateTreePath(senate, 2027)).toThrow("No Kansas State Senator KPDC tree for a 2027 election");
  });

  it("maps statewide offices to their SW0n filer prefixes (not the viewer codes)", () => {
    expect(kansasKpdcStatewideFilerPrefix(attorneyGeneral)).toBe("SW02");
    expect(kansasKpdcStatewideFilerPrefix(kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Secretary of State" })!)).toBe("SW04");
    expect(kansasKpdcStatewideFilerPrefix(house)).toBeNull();
  });
});

describe("KPDC fetchers", () => {
  const fetchOk = (body: string | Uint8Array, status = 200) =>
    (async () => new Response(body, { status })) as unknown as typeof fetch;

  it("fetches an index page as HTML", async () => {
    const result = await fetchKansasKpdcIndexPage("House/2026ElecCycle/HLinks2026EC.htm", {
      fetchImpl: fetchOk("<html><body>tree</body></html>"),
    });
    expect(result.url).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"
    );
    expect(result.html).toContain("tree");
  });

  it("accepts a PDF body and rejects a non-PDF body", async () => {
    const pdf = await fetchKansasKpdcPdf("Others/x.pdf", { fetchImpl: fetchOk("%PDF-1.4 fake") });
    expect(pdf.bytes.slice(0, 4)).toEqual(new Uint8Array([0x25, 0x50, 0x44, 0x46]));
    await expect(
      fetchKansasKpdcPdf("Others/x.pdf", { fetchImpl: fetchOk("<html>error page</html>") })
    ).rejects.toThrow("did not answer a PDF");
  });

  it("rejects a non-200 answer", async () => {
    await expect(
      fetchKansasKpdcPdf("Others/x.pdf", { fetchImpl: fetchOk("gone", 404) })
    ).rejects.toThrow("answered 404");
  });
});

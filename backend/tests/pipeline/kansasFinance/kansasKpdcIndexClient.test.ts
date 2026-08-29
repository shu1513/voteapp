import { describe, expect, it } from "vitest";

import {
  buildKansasKpdcUrl,
  fetchKansasKpdcIndexPage,
  fetchKansasKpdcPdf,
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
    ["H001DH_AT.pdf", "H001DH", "appointment_of_treasurer", null, false],
    ["H001DH_202607.pdf", "H001DH", "report", "202607", false],
    ["H003DP_amend2607.pdf", "H003DP", "report", "202607", true],
    ["SW05AB_amendAT.pdf", "SW05AB", "appointment_of_treasurer", null, true],
    ["H065SC_2026PLF.pdf", "H065SC", "last_minute", null, false],
    ["H010XY_Aff2607.pdf", "H010XY", "affidavit", "202607", false],
    ["H011ZQ_Term2607.pdf", "H011ZQ", "termination", "202607", false],
    // IE statements: two underscores — the filer code keeps the first one.
    ["IE_KC1_2607.pdf", "IE_KC1", "report", "202607", false],
    // Filer codes can carry disambiguating digits.
    ["H096JM2_AT.pdf", "H096JM2", "appointment_of_treasurer", null, false],
  ] as const)("classifies %s", (fileName, filerCode, kind, periodKey, amendment) => {
    const info = parseKansasKpdcFileName(fileName);
    expect(info.filerCode).toBe(filerCode);
    expect(info.kind).toBe(kind);
    expect(info.periodKey).toBe(periodKey);
    expect(info.amendment).toBe(amendment);
  });

  it("returns kind unknown instead of guessing", () => {
    expect(parseKansasKpdcFileName("HLinks2026EC.htm").kind).toBe("unknown");
    expect(parseKansasKpdcFileName("H001DH_mystery.pdf").kind).toBe("unknown");
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

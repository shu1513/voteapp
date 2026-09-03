import { describe, expect, it, vi } from "vitest";

import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import type { KansasKpdcCandidateRow } from "../../../src/pipeline/kansasFinance/kansasKpdcIndexClient.js";
import { buildKansasPaperInventory, createKansasKpdcRowLoader } from "../../../src/pipeline/kansasFinance/kansasPaperInventory.js";
import { kansasReportingPeriods, type KansasFilingHeader } from "../../../src/pipeline/kansasFinance/kansasReportInventory.js";

const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const governor = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Governor" })!;
const attorneyGeneral = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Attorney General" })!;

// Synthetic filers on purpose.
const link = (fileName: string) => ({ url: `https://www.kansas.gov/ethics/CFAScanned/x/${fileName}`, fileName, linkText: fileName });
const row = (district: number | null, filedName: string, fileNames: string[]): KansasKpdcCandidateRow => ({
  district,
  filedName,
  links: fileNames.map(link),
});

const housePeriods = [...kansasReportingPeriods(house, 2026), ...kansasReportingPeriods(house, 2024)];
const base = {
  candidateName: "HOLLOWAY, MARGARET",
  districtNumber: 85,
  office: house,
  periods: housePeriods,
  windowStart: "2025-01-01",
  efileFilings: [] as KansasFilingHeader[],
};

describe("buildKansasPaperInventory", () => {
  it("maps a filer's due-month tokens to periods, taking only versions due inside the window", () => {
    const result = buildKansasPaperInventory({
      ...base,
      rows: [
        row(85, "Holloway, Margaret", ["H085MH_AT.pdf", "H085MH_202601.pdf", "H085MH_amend2607.pdf", "H085MH_202607.pdf", "H085MH_2026PLF.pdf", "H085MH_Aff2607.pdf"]),
        row(85, "Holloway, Margaret", ["H085MH_202407.pdf", "H085MH_2amend2501.pdf", "H085MH_amend2501.pdf", "H085MH_202501.pdf"]),
        row(85, "Holloway, Daniel", ["H085DH_202607.pdf"]),
        row(86, "Holloway, Margaret", ["H086MH_202607.pdf"]),
      ],
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result).toMatchObject({ filedNames: ["Holloway, Margaret"], explainedByEfile: 0, lastMinute: 1, skipped: 2, unmapped: [] });
    expect(result.headers.map((header) => [header.periodStart, header.periodEnd, header.amended, header.amendmentOrdinal, header.termination])).toEqual([
      ["2025-01-01", "2025-12-31", false, null, false],
      ["2026-01-01", "2026-07-23", true, 1, false],
      ["2026-01-01", "2026-07-23", false, null, false],
      ["2024-10-25", "2024-12-31", true, 2, false],
      ["2024-10-25", "2024-12-31", true, 1, false],
      ["2024-10-25", "2024-12-31", false, null, false],
    ]);
    expect(result.headers.every((header) => header.fileDate === null && header.channel === "paper")).toBe(true);
  });

  it("reports filenames it cannot place instead of guessing, and dedupes a filer listed twice", () => {
    const result = buildKansasPaperInventory({
      ...base,
      rows: [
        row(85, "Holloway, Margaret", ["H085MH_202607.pdf", "H085MH_mystery.pdf", "H085MH_202404.pdf", "H085MH_Term2607.pdf"]),
        row(85, "Holloway, Margaret", ["H085MH_202607.pdf"]),
      ],
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.unmapped).toEqual(["H085MH_mystery.pdf", "H085MH_202404.pdf"]);
    expect(result.headers.map((header) => [header.periodStart, header.termination])).toEqual([
      ["2026-01-01", false],
      ["2026-01-01", true],
    ]);
  });

  it("subtracts e-filed covers by due month and amended flag", () => {
    const efile = (start: string, end: string, amended: boolean): KansasFilingHeader => ({
      periodStart: start,
      periodEnd: end,
      fileDate: "07/27/2026",
      amendmentDate: amended ? "08/01/2026" : null,
      amended,
      termination: false,
      channel: "efile",
    });
    const result = buildKansasPaperInventory({
      ...base,
      rows: [row(85, "Holloway, Margaret", ["H085MH_202601.pdf", "H085MH_amend2607.pdf", "H085MH_2amend2607.pdf", "H085MH_202607.pdf"])],
      // One e-filed pre-primary amendment (of two) and the e-filed original; the annual stays paper.
      efileFilings: [efile("1/1/2026", "7/23/2026", false), efile("1/1/2026", "7/23/2026", true)],
    });
    expect(result.status).toBe("resolved");
    if (result.status !== "resolved") return;
    expect(result.explainedByEfile).toBe(2);
    expect(result.headers.map((header) => [header.periodStart, header.amendmentOrdinal])).toEqual([
      ["2025-01-01", null],
      ["2026-01-01", 2],
    ]);
  });

  it("scopes statewide rows by the office's SW0n prefix and ignores their district column", () => {
    const periods = [...kansasReportingPeriods(governor, 2026), ...kansasReportingPeriods(governor, 2022)];
    const rows = [
      row(null, "Rowan, Stacy", ["SW01SR_AT.pdf", "SW01SR_202501.pdf", "SW01SR_202601.pdf"]),
      row(null, "Rowan, Stacy", ["SW02SR_AT.pdf", "SW02SR_202601.pdf"]),
      // A row whose links disagree on the office is nobody's.
      row(null, "Rowan, Stacy", ["SW01SR_202607.pdf", "SW03SR_202607.pdf"]),
    ];
    const governorResult = buildKansasPaperInventory({ ...base, candidateName: "ROWAN, STACY", districtNumber: null, office: governor, periods, windowStart: "2023-01-01", rows });
    expect(governorResult.status).toBe("resolved");
    if (governorResult.status !== "resolved") return;
    expect(governorResult.headers.map((header) => header.periodStart)).toEqual(["2024-01-01", "2025-01-01"]);
    const agResult = buildKansasPaperInventory({ ...base, candidateName: "ROWAN, STACY", districtNumber: null, office: attorneyGeneral, periods, windowStart: "2023-01-01", rows });
    expect(agResult.status).toBe("resolved");
    if (agResult.status !== "resolved") return;
    expect(agResult.headers.map((header) => header.periodStart)).toEqual(["2025-01-01"]);
  });

  it("passes resolver outcomes through", () => {
    expect(buildKansasPaperInventory({ ...base, rows: [row(85, "Holloway, Daniel", ["H085DH_202607.pdf"])] })).toEqual({ status: "unresolved", reason: "no_matching_filer" });
    expect(
      buildKansasPaperInventory({ ...base, rows: [row(85, "Holloway, Margaret B", ["H085MH_202607.pdf"]), row(85, "Holloway, Margaret T", ["H085MT_202607.pdf"])] })
    ).toEqual({ status: "unresolved", reason: "conflicting_filed_names", filedNames: ["Holloway, Margaret B", "Holloway, Margaret T"] });
    expect(buildKansasPaperInventory({ ...base, rows: [row(null, "Holloway, Margaret", ["H085MH_202607.pdf"])] })).toEqual({
      status: "unresolved",
      reason: "filings_missing_district",
      filedNames: ["Holloway, Margaret"],
    });
  });
});

describe("createKansasKpdcRowLoader", () => {
  it("fetches each tree once and parses its candidate rows", async () => {
    const html = `<table><tr><td>85</td><td>Holloway, Margaret</td><td><a href="https://kansas.gov/ethics/CFAScanned/House/2026ElecCycle/202607/H085MH_202607.pdf">202607</a></td></tr></table>`;
    const fetchImpl = vi.fn(async () => new Response(html, { status: 200 })) as unknown as typeof fetch;
    const load = createKansasKpdcRowLoader({ fetchOptions: { fetchImpl } });
    const [first, second] = await Promise.all([load(house, 2026), load(house, 2026)]);
    expect(first).toBe(second);
    expect(first).toEqual([{ district: 85, filedName: "Holloway, Margaret", links: [expect.objectContaining({ fileName: "H085MH_202607.pdf" })] }]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect((fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]![0]).toBe(
      "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"
    );
  });
});

import { deflateRawSync } from "node:zlib";
import { describe, expect, it, vi } from "vitest";

import {
  fetchMichiganMitnCommitteeSearch,
  fetchMichiganMitnContributionExportXlsx,
  michiganMitnExportRowsToLegacyContributionRows,
  parseMichiganMitnCommitteeSearchHtml,
  parseMichiganMitnExportXlsxRows,
} from "../../../src/pipeline/michiganFinance/michiganMitnPublicSearchClient.js";

// --- helpers ---------------------------------------------------------------

function crc32(buffer: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/** Builds a minimal real zip (deflate) holding the given files. */
function buildZip(files: { name: string; content: string }[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;

  for (const file of files) {
    const nameBuffer = Buffer.from(file.name, "utf8");
    const raw = Buffer.from(file.content, "utf8");
    const compressed = deflateRawSync(raw);
    const checksum = crc32(raw);

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0, 6);
    local.writeUInt16LE(8, 8); // deflate
    local.writeUInt32LE(0, 10);
    local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(raw.length, 22);
    local.writeUInt16LE(nameBuffer.length, 26);
    local.writeUInt16LE(0, 28);
    localParts.push(local, nameBuffer, compressed);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(0, 8);
    central.writeUInt16LE(8, 10);
    central.writeUInt32LE(0, 12);
    central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(compressed.length, 20);
    central.writeUInt32LE(raw.length, 24);
    central.writeUInt16LE(nameBuffer.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt32LE(offset, 42);
    centralParts.push(central, nameBuffer);

    offset += local.length + nameBuffer.length + compressed.length;
  }

  const centralSize = centralParts.reduce((sum, part) => sum + part.length, 0);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(files.length, 8);
  eocd.writeUInt16LE(files.length, 10);
  eocd.writeUInt32LE(centralSize, 12);
  eocd.writeUInt32LE(offset, 16);

  return Buffer.concat([...localParts, ...centralParts, eocd]);
}

function sheetXml(rows: string[][]): string {
  const body = rows
    .map(
      (cells, rowIndex) =>
        `<row r="${rowIndex + 1}">` +
        cells
          .map((value, columnIndex) => {
            const ref = `${String.fromCharCode(65 + columnIndex)}${rowIndex + 1}`;
            return value === ""
              ? `<c r="${ref}" t="inlineStr"/>`
              : `<c r="${ref}" t="inlineStr"><is><t xml:space="preserve">${value}</t></is></c>`;
          })
          .join("") +
        `</row>`
    )
    .join("");
  return `<?xml version="1.0"?><worksheet><sheetData>${body}</sheetData></worksheet>`;
}

const EXPORT_HEADER = [
  "Record Type (1=Parent, 2=Child)",
  "Receipt ID",
  "Filing Status (C=Complete,I=Incomplete)",
  "Document Type",
  "Document Statement Year",
  "Document Statement Type",
  "Receiving Committee Name",
  "Receiving Committee ID#",
  "Receiving Committee Type",
  "Receiving Candidate First Name",
  "Receiving Candidate Last Name",
  "Type of Contribution",
  "Contributor First Name",
  "Organization Name/Contributor Last Name",
  "Contributor Occupation",
  "Contributor Employer",
  "Date of Contribution",
  "Amount of Contribution",
  "Cumulative from this person/org",
];

const COMMITTEE_RESULT_HTML = `
<table>
  <tr><th>Committee ID</th><th>Committee Type</th><th>Committee Name</th><th>Status</th></tr>
  <tr><td><a href="#">521877</a></td><td>Candidate</td><td>ARIC NESBITT FOR GOVERNOR</td><td>Active</td></tr>
  <tr><td><a href="#">518698</a></td><td>Candidate</td><td>ARIC NESBITT FOR STATE SENATE</td><td>Active</td></tr>
  <tr><td><a href="#">514761</a></td><td>Candidate</td><td>ARIC NESBITT FOR STATE REPRESENTATIVE</td><td>Dissolved</td></tr>
</table>`;

// --- tests -----------------------------------------------------------------

describe("michiganMitnPublicSearchClient", () => {
  it("parses committee search result rows", () => {
    expect(parseMichiganMitnCommitteeSearchHtml(COMMITTEE_RESULT_HTML)).toEqual([
      { committeeId: "521877", committeeType: "Candidate", committeeName: "ARIC NESBITT FOR GOVERNOR", status: "Active" },
      {
        committeeId: "518698",
        committeeType: "Candidate",
        committeeName: "ARIC NESBITT FOR STATE SENATE",
        status: "Active",
      },
      {
        committeeId: "514761",
        committeeType: "Candidate",
        committeeName: "ARIC NESBITT FOR STATE REPRESENTATIVE",
        status: "Dissolved",
      },
    ]);
  });

  it("throws on the not-allowed and row-cap error fragments", () => {
    expect(() => parseMichiganMitnCommitteeSearchHtml("<div>The attempted search is not allowed.</div>")).toThrow(
      "rejected the request shape"
    );
    expect(() =>
      parseMichiganMitnCommitteeSearchHtml("<div>narrow the search criteria to return fewer than 25,000 records</div>")
    ).toThrow("row cap");
  });

  it("sends the full committee-search field set with the office filter applied", async () => {
    const fetchFn = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => COMMITTEE_RESULT_HTML,
      arrayBuffer: async () => new ArrayBuffer(0),
    });

    await fetchMichiganMitnCommitteeSearch({
      candidateLastName: "Nesbitt",
      candidateFirstName: "Aric",
      officeSought: "Governor",
      fetchFn,
    });

    const [url, init] = fetchFn.mock.calls[0]!;
    expect(url).toContain("page=page.miboeCommitteePublicSearch&action=search");
    expect(init.headers["hx-request"]).toBe("true");
    const body = new URLSearchParams(init.body);
    expect(body.get("option")).toBe("committee");
    expect(body.get("form.committeeType")).toBe("13");
    expect(body.get("form.candidateLastName")).toBe("Nesbitt");
    expect(body.get("form.candidateFirstName")).toBe("Aric");
    expect(body.get("form.officeSought")).toBe("121");
    // the server rejects partial bodies — every field must be present
    expect(body.has("form.sponsoringOrganization")).toBe(true);
    expect(body.has("form.officeHeldDistrict")).toBe(true);
  });

  it("downloads and validates a contribution export", async () => {
    const zip = buildZip([{ name: "xl/worksheets/sheet1.xml", content: sheetXml([["a"]]) }]);
    const fetchFn = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "",
      arrayBuffer: async () => zip.buffer.slice(zip.byteOffset, zip.byteOffset + zip.byteLength),
    });

    const buffer = await fetchMichiganMitnContributionExportXlsx({
      committeeId: "521877",
      statementYear: 2026,
      fetchFn,
    });
    expect(buffer.subarray(0, 2).toString("latin1")).toBe("PK");

    const [url, init] = fetchFn.mock.calls[0]!;
    expect(url).toContain("page=page.miboeContributionPublicSearch&action=export");
    const body = new URLSearchParams(init.body);
    expect(body.get("form.committeeId")).toBe("521877");
    expect(body.get("form.campaignStatementYear")).toBe("76");
    expect(body.get("form.contributionType")).toBe("individual");
  });

  it("rejects non-xlsx export responses and unknown statement years", async () => {
    const fetchFn = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "",
      arrayBuffer: async () => Buffer.from("<html>error</html>").buffer,
    });
    await expect(
      fetchMichiganMitnContributionExportXlsx({ committeeId: "521877", statementYear: 2026, fetchFn })
    ).rejects.toThrow("did not return an xlsx attachment");
    await expect(
      fetchMichiganMitnContributionExportXlsx({ committeeId: "521877", statementYear: 2031, fetchFn })
    ).rejects.toThrow("statement-year id");
  });

  it("reads inline-string xlsx rows, keeping empty cells positional", () => {
    const zip = buildZip([
      {
        name: "xl/worksheets/sheet1.xml",
        content: sheetXml([
          ["Receipt ID", "Amount of Contribution", "Contributor Employer"],
          ["26-1", "500.00", ""],
          ["26-2", "", "KELLOGG &amp; CO"],
        ]),
      },
    ]);
    expect(parseMichiganMitnExportXlsxRows(zip)).toEqual([
      ["Receipt ID", "Amount of Contribution", "Contributor Employer"],
      ["26-1", "500.00", ""],
      ["26-2", "", "KELLOGG & CO"],
    ]);
  });

  it("maps export rows onto the legacy contribution-row shape", () => {
    const rows = michiganMitnExportRowsToLegacyContributionRows([
      EXPORT_HEADER,
      [
        "1",
        "26-889077",
        "I",
        "Campaign Statements",
        "2026",
        "July CS",
        "ARIC NESBITT FOR GOVERNOR",
        "521877",
        "Candidate",
        "ARIC",
        "NESBITT",
        "Direct Contributions",
        "RAYMOND",
        "KOUZA",
        "RETIRED",
        "",
        "06/24/2026",
        "1000.00",
        "1000.00",
      ],
    ]);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      contribution_id: "26-889077",
      doc_stmnt_year: "2026",
      doc_type_desc: "July CS",
      com_legal_name: "ARIC NESBITT FOR GOVERNOR",
      cfr_com_id: "521877",
      com_type: "CAN",
      can_first_name: "ARIC",
      can_last_name: "NESBITT",
      contribtype: "Direct Contributions",
      f_name: "RAYMOND",
      l_name_or_org: "KOUZA",
      occupation: "RETIRED",
      received_date: "06/24/2026",
      amount: "1000.00",
      aggregate: "1000.00",
    });
  });

  it("skips blank export rows and requires recognized headers", () => {
    expect(michiganMitnExportRowsToLegacyContributionRows([EXPORT_HEADER, ["", "", ""]])).toEqual([]);
    expect(() => michiganMitnExportRowsToLegacyContributionRows([["Nothing", "Recognized"], ["a", "b"]])).toThrow(
      "no recognized header columns"
    );
  });
});

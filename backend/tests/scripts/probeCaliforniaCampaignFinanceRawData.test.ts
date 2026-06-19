import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  downloadCalAccessRawDataZip,
  fetchCalAccessRawDataZipMetadata,
  parseProbeCaliforniaCampaignFinanceRawDataScriptArgs,
  runProbeCaliforniaCampaignFinanceRawDataScript,
} from "../../src/scripts/probeCaliforniaCampaignFinanceRawData.js";

type ZipFixtureEntry = {
  fileName: string;
  content: string;
};

let tempDirs: string[] = [];

function makeStoredZip(entries: readonly ZipFixtureEntry[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let localOffset = 0;

  for (const entry of entries) {
    const fileName = Buffer.from(entry.fileName, "utf8");
    const content = Buffer.from(entry.content, "utf8");

    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(0, 6);
    localHeader.writeUInt16LE(0, 8);
    localHeader.writeUInt32LE(0, 10);
    localHeader.writeUInt32LE(0, 14);
    localHeader.writeUInt32LE(content.length, 18);
    localHeader.writeUInt32LE(content.length, 22);
    localHeader.writeUInt16LE(fileName.length, 26);
    localHeader.writeUInt16LE(0, 28);
    localParts.push(localHeader, fileName, content);

    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0, 8);
    centralHeader.writeUInt16LE(0, 10);
    centralHeader.writeUInt32LE(0, 12);
    centralHeader.writeUInt32LE(0, 16);
    centralHeader.writeUInt32LE(content.length, 20);
    centralHeader.writeUInt32LE(content.length, 24);
    centralHeader.writeUInt16LE(fileName.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(localOffset, 42);
    centralParts.push(centralHeader, fileName);

    localOffset += localHeader.length + fileName.length + content.length;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8);
  eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(centralDirectory.length, 12);
  eocd.writeUInt32LE(localOffset, 16);
  eocd.writeUInt16LE(0, 20);

  return Buffer.concat([...localParts, centralDirectory, eocd]);
}

async function writeFixtureZip(entries: readonly ZipFixtureEntry[]): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "calaccess-script-"));
  tempDirs.push(dir);
  const zipPath = path.join(dir, "dbwebexport.zip");
  await writeFile(zipPath, makeStoredZip(entries));
  return zipPath;
}

async function tempOutputPath(): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "calaccess-download-"));
  tempDirs.push(dir);
  return path.join(dir, "dbwebexport.zip");
}

function zipResponse(zip: Buffer, init: ResponseInit = {}): Response {
  return new Response(zip, {
    status: 200,
    statusText: "OK",
    headers: {
      "content-type": "application/x-zip-compressed",
      "content-length": String(zip.length),
      etag: '"test-etag"',
      "last-modified": "Thu, 18 Jun 2026 08:40:47 GMT",
    },
    ...init,
  });
}

describe("probeCaliforniaCampaignFinanceRawData script", () => {
  afterEach(async () => {
    vi.restoreAllMocks();
    const dirs = tempDirs;
    tempDirs = [];
    await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("parses default head-only remote probe options", () => {
    expect(parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(["--head-only"])).toEqual({
      inputKind: "url",
      url: CAL_ACCESS_RAW_DATA_ZIP_URL,
      localZip: null,
      outputPath: null,
      headOnly: true,
      sampleFileNames: [],
      samplePatterns: [],
      validateManifest: false,
      maxRowsPerFile: 5,
      maxFiles: 20,
      timeoutMs: 30_000,
    });
  });

  it("parses local ZIP probe options", () => {
    const options = parseProbeCaliforniaCampaignFinanceRawDataScriptArgs([
      "--local-zip=./tmp/dbwebexport.zip",
      "--sample-file=CalAccess/DBEXPORT/FILER.TSV",
      "--sample-pattern=DBEXPORT/.*\\.TSV$",
      "--max-rows=2",
      "--max-files=3",
      "--timeout-ms=5000",
    ]);

    expect(options).toMatchObject({
      inputKind: "local_zip",
      url: null,
      localZip: expect.stringMatching(/tmp\/dbwebexport\.zip$/),
      outputPath: null,
      headOnly: false,
      sampleFileNames: ["CalAccess/DBEXPORT/FILER.TSV"],
      samplePatterns: ["DBEXPORT/.*\\.TSV$"],
      validateManifest: false,
      maxRowsPerFile: 2,
      maxFiles: 3,
      timeoutMs: 5000,
    });
  });

  it("requires an explicit output path for remote downloads", () => {
    expect(() => parseProbeCaliforniaCampaignFinanceRawDataScriptArgs([])).toThrow(
      "Provide --output=... when downloading the CAL-ACCESS raw data ZIP"
    );
    expect(() =>
      parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(["--url=http://example.test/dbwebexport.zip", "--head-only"])
    ).toThrow("Invalid --url URL protocol: http:. Only https is allowed.");
    expect(() =>
      parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(["--local-zip=./dbwebexport.zip", "--head-only"])
    ).toThrow("--head-only can only be used with remote --url input");
    expect(() =>
      parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(["--head-only", "--manifest"])
    ).toThrow("--manifest cannot be used with --head-only because manifest validation requires ZIP samples");
    expect(() => parseProbeCaliforniaCampaignFinanceRawDataScriptArgs(["--url", "--head-only"])).toThrow(
      "Missing value for --url"
    );
  });

  it("fetches remote ZIP metadata with HEAD", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(zipResponse(Buffer.from("zip"), { status: 200 })) as unknown as typeof fetch;

    await expect(fetchCalAccessRawDataZipMetadata("https://example.test/dbwebexport.zip", { fetchImpl })).resolves.toEqual({
      url: "https://example.test/dbwebexport.zip",
      contentLength: 3,
      contentType: "application/x-zip-compressed",
      etag: '"test-etag"',
      lastModified: "Thu, 18 Jun 2026 08:40:47 GMT",
    });
    expect(fetchImpl).toHaveBeenCalledWith(
      "https://example.test/dbwebexport.zip",
      expect.objectContaining({
        method: "HEAD",
        signal: expect.any(AbortSignal),
      })
    );
  });

  it("downloads a remote ZIP to an explicit output path", async () => {
    const zip = makeStoredZip([{ fileName: "CalAccess/DBEXPORT/FILER.TSV", content: "ID\tNAME\n1\tExample\n" }]);
    const outputPath = await tempOutputPath();
    const fetchImpl = vi.fn().mockResolvedValue(zipResponse(zip)) as unknown as typeof fetch;

    await expect(
      downloadCalAccessRawDataZip({
        url: "https://example.test/dbwebexport.zip",
        outputPath,
        fetchImpl,
        timeoutMs: 1000,
      })
    ).resolves.toMatchObject({
      url: "https://example.test/dbwebexport.zip",
      outputPath,
      bytesWritten: zip.length,
      contentLength: zip.length,
    });
  });

  it("runs a local ZIP probe and returns entry samples", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "CalAccess/DBEXPORT/FILER.TSV",
        content: "FILER_ID\tFILER_NAME\n1\tExample Committee\n",
      },
    ]);
    const options = parseProbeCaliforniaCampaignFinanceRawDataScriptArgs([
      `--local-zip=${zipPath}`,
      "--sample-file=CalAccess/DBEXPORT/FILER.TSV",
    ]);

    const output = await runProbeCaliforniaCampaignFinanceRawDataScript({ options });

    expect(output).toMatchObject({
      type: "cal_access_raw_data_probe",
      input_kind: "local_zip",
      head_only: false,
      remote: null,
      download: null,
      probe: {
        zip_path: zipPath,
        entry_count: 1,
        samples: [
          {
            fileName: "CalAccess/DBEXPORT/FILER.TSV",
            headers: ["FILER_ID", "FILER_NAME"],
            rowObjects: [{ FILER_ID: "1", FILER_NAME: "Example Committee" }],
          },
        ],
      },
      manifest_validation: null,
    });
  });

  it("runs manifest validation against a local ZIP probe", async () => {
    const zipPath = await writeFixtureZip([
      {
        fileName: "CalAccess/DATA/FILERNAME_CD.TSV",
        content: "FILER_ID\tFILER_TYPE\tSTATUS\tNAML\tNAMF\tCITY\tST\n1\tCTL\tACTIVE\tNEWSOM\tGAVIN\tSACRAMENTO\tCA\n",
      },
      {
        fileName: "CalAccess/DATA/CVR_CAMPAIGN_DISCLOSURE_CD.TSV",
        content:
          "FILING_ID\tFORM_TYPE\tFILER_ID\tFILER_NAML\tRPT_DATE\tFROM_DATE\tTHRU_DATE\tELECT_DATE\tCMTTE_ID\tCMTTE_TYPE\tCONTROL_YN\tCAND_NAML\tCAND_NAMF\tOFFICE_CD\tOFFIC_DSCR\tJURIS_CD\tDIST_NO\tSUP_OPP_CD\n1\tF460\t1\tNEWSOM FOR GOVERNOR\t\t\t\t\t1\tC\tY\tNEWSOM\tGAVIN\tGOV\tGovernor\tSTW\t\t\n",
      },
      {
        fileName: "CalAccess/DATA/RCPT_CD.TSV",
        content:
          "FILING_ID\tFORM_TYPE\tTRAN_ID\tENTITY_CD\tCTRIB_NAML\tCTRIB_NAMF\tCTRIB_CITY\tCTRIB_ST\tCTRIB_EMP\tCTRIB_OCC\tRCPT_DATE\tAMOUNT\tCMTE_ID\tCAND_NAML\tCAND_NAMF\tOFFICE_CD\tOFFIC_DSCR\tSUP_OPP_CD\n1\tA\tT1\tIND\tDOE\tJANE\tLOS ANGELES\tCA\tACME\tENGINEER\t1/1/2026\t100\t1\tNEWSOM\tGAVIN\tGOV\tGovernor\tS\n",
      },
      {
        fileName: "CalAccess/DATA/S496_CD.TSV",
        content: "FILING_ID\tFORM_TYPE\tTRAN_ID\tAMOUNT\tEXP_DATE\tEXPN_DSCR\n1\tF496\tE1\t50\t1/1/2026\tMailer\n",
      },
      {
        fileName: "CalAccess/DATA/S497_CD.TSV",
        content:
          "FILING_ID\tFORM_TYPE\tTRAN_ID\tENTITY_CD\tENTY_NAML\tENTY_NAMF\tCTRIB_EMP\tCTRIB_OCC\tCTRIB_DATE\tAMOUNT\tCMTE_ID\tCAND_NAML\tCAND_NAMF\tOFFICE_CD\tOFFIC_DSCR\tSUP_OPP_CD\n1\tF497P1\tL1\tIND\tDOE\tJANE\tACME\tENGINEER\t1/1/2026\t1000\t1\tNEWSOM\tGAVIN\tGOV\tGovernor\tS\n",
      },
      {
        fileName: "CalAccess/DATA/SMRY_CD.TSV",
        content: "FILING_ID\tLINE_ITEM\tFORM_TYPE\tAMOUNT_A\tAMOUNT_B\tAMOUNT_C\n1\t12\tF460\t100\t\t\n",
      },
    ]);
    const options = parseProbeCaliforniaCampaignFinanceRawDataScriptArgs([`--local-zip=${zipPath}`, "--manifest"]);

    const output = await runProbeCaliforniaCampaignFinanceRawDataScript({ options });

    expect(output).toMatchObject({
      probe: {
        entry_count: 6,
        missing_file_names: [],
      },
      manifest_validation: {
        ok: true,
        missingFiles: [],
      },
    });
  });

  it("runs a remote head-only probe without downloading", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(zipResponse(Buffer.from("zip"))) as unknown as typeof fetch;
    const options = parseProbeCaliforniaCampaignFinanceRawDataScriptArgs([
      "--url=https://example.test/dbwebexport.zip",
      "--head-only",
    ]);

    const output = await runProbeCaliforniaCampaignFinanceRawDataScript({ options, fetchImpl });

    expect(output).toMatchObject({
      type: "cal_access_raw_data_probe",
      input_kind: "url",
      head_only: true,
      remote: {
        url: "https://example.test/dbwebexport.zip",
        contentLength: 3,
      },
      download: null,
      probe: null,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });
});

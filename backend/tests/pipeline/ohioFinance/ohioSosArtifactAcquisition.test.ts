import { describe, expect, it } from "vitest";

import {
  ohioSos31uDetailCachePath,
  ohioSosFileDownloadUrl,
  ohioSosFileListUrl,
  planOhioSosCycleDownloads,
  type OhioSosListedFile,
} from "../../../src/pipeline/ohioFinance/ohioSosArtifactAcquisition.js";

function listed(fileName: string, downloadId: string, overrides: Partial<OhioSosListedFile> = {}): OhioSosListedFile {
  return {
    listType: "CAN",
    fileName,
    downloadId,
    dateModified: "08/04/2026 10:30 AM",
    ...overrides,
  };
}

describe("Ohio SoS portal URLs", () => {
  it("builds session-less listing URLs; APEX mints its own session", () => {
    expect(ohioSosFileListUrl("CAN")).toBe(
      "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73:::::P73_TYPE:CAN"
    );
    expect(ohioSosFileListUrl("PARTY")).toBe(
      "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73:::::P73_TYPE:PARTY"
    );
  });

  it("builds the file-download URL from a discovered id", () => {
    expect(ohioSosFileDownloadUrl("6768")).toBe(
      "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:72:::NO::P72_GETID:6768"
    );
  });

  it("rejects a non-numeric download id", () => {
    expect(() => ohioSosFileDownloadUrl("6768; drop")).toThrow(/Invalid Ohio SoS download id/);
  });
});

describe("planOhioSosCycleDownloads", () => {
  // The observed ids are non-sequential and get reissued when the portal
  // regenerates files, so the plan always comes from the live listing.
  const fullListing: OhioSosListedFile[] = [
    listed("ACT_CAN_LIST.CSV", "120"),
    listed("CAN_COVER.CSV", "123"),
    listed("CAC_CON_2026.CSV", "6768"),
    listed("CAC_EXP_2026.CSV", "6769"),
    listed("CAC_CON_2025.CSV", "6130"),
    listed("CAC_EXP_2025.CSV", "6131"),
    listed("ACT_PAC_LIST.CSV", "3", { listType: "PAC" }),
    listed("PAC_COV.CSV", "3431", { listType: "PAC" }),
    listed("PAC_CON_2026.CSV", "6770", { listType: "PAC" }),
    listed("PAC_EXP_2026.CSV", "6771", { listType: "PAC" }),
    listed("PAC_CON_2025.CSV", "6132", { listType: "PAC" }),
    listed("PAC_EXP_2025.CSV", "6133", { listType: "PAC" }),
    listed("PAR_COVER.CSV", "122", { listType: "PARTY" }),
    listed("PPC_CON_2026.CSV", "6772", { listType: "PARTY" }),
    listed("PPC_EXP_2026.CSV", "6773", { listType: "PARTY" }),
    listed("PPC_CON_2025.CSV", "6134", { listType: "PARTY" }),
    listed("PPC_EXP_2025.CSV", "6135", { listType: "PARTY" }),
  ];

  it("resolves every required file of the cycle to its discovered id", () => {
    const plan = planOhioSosCycleDownloads({ cycleYear: 2026, listedFiles: fullListing });

    expect(plan.missingFileNames).toEqual([]);
    expect(plan.entries).toHaveLength(17);
    expect(plan.entries.find((entry) => entry.fileName === "CAC_CON_2026.CSV")).toMatchObject({
      productKey: "candidate_contributions",
      transactionYear: 2026,
      downloadId: "6768",
      dateModified: "08/04/2026 10:30 AM",
    });
    expect(plan.entries.find((entry) => entry.fileName === "PPC_EXP_2025.CSV")).toMatchObject({
      productKey: "party_expenditures",
      transactionYear: 2025,
      downloadId: "6135",
    });
  });

  it("reports a required file the portal did not list instead of guessing an id", () => {
    const plan = planOhioSosCycleDownloads({
      cycleYear: 2026,
      listedFiles: fullListing.filter((file) => file.fileName !== "PAC_EXP_2026.CSV"),
    });

    expect(plan.missingFileNames).toEqual(["PAC_EXP_2026.CSV"]);
    expect(plan.entries).toHaveLength(16);
  });

  it("keeps the first listing when a file appears under more than one tab", () => {
    const plan = planOhioSosCycleDownloads({
      cycleYear: 2026,
      listedFiles: [
        listed("ACT_CAN_LIST.CSV", "120", { listType: "NEW" }),
        listed("ACT_CAN_LIST.CSV", "999", { listType: "CAN" }),
        ...fullListing.filter((file) => file.fileName !== "ACT_CAN_LIST.CSV"),
      ],
    });

    expect(plan.entries.find((entry) => entry.fileName === "ACT_CAN_LIST.CSV")?.downloadId).toBe("120");
  });

  it("shifts the annual files with the cycle year", () => {
    const plan = planOhioSosCycleDownloads({
      cycleYear: 2028,
      listedFiles: [listed("CAC_CON_2028.CSV", "7000"), listed("CAC_CON_2027.CSV", "6900")],
    });

    expect(plan.entries.map((entry) => entry.fileName)).toEqual(["CAC_CON_2027.CSV", "CAC_CON_2028.CSV"]);
    expect(plan.missingFileNames).toContain("ACT_CAN_LIST.CSV");
  });
});

describe("ohioSos31uDetailCachePath", () => {
  it("names the detail bundle per cycle", () => {
    expect(ohioSos31uDetailCachePath({ cacheDir: "/tmp/oh", cycleYear: 2026 })).toBe("/tmp/oh/31U_DETAIL_2026.json");
  });
});

import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  fetchOhioSos31uDetails,
  ohioSos31uDetailCachePath,
  readOhioSos31uDetailBundle,
  ohioSosFileDownloadUrl,
  ohioSosFileListUrl,
  planOhioSosCycleDownloads,
  watchOhioSosDownload,
  type OhioSosListedFile,
} from "../../../src/pipeline/ohioFinance/ohioSosArtifactAcquisition.js";
import { OHIO_SOS_31U_DETAIL_HEADER } from "../../../src/pipeline/ohioFinance/ohioSos31uDetail.js";
import type {
  OhioSosChromeSession,
  OhioSosChromeTab,
} from "../../../src/pipeline/ohioFinance/ohioSosChromeClient.js";

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

type FakeEvent = { method: string; params: Record<string, unknown>; sessionId?: string };

// Enough of OhioSosChromeSession for the watcher and the detail fetcher: the
// event fan-out plus a canned response per CDP method.
class FakeChromeSession {
  private readonly listeners = new Set<(event: FakeEvent) => void>();

  constructor(private readonly evaluate: () => unknown = () => null) {}

  on(listener: (event: FakeEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event: FakeEvent): void {
    for (const listener of [...this.listeners]) {
      listener(event);
    }
  }

  async send(method: string, _params?: Record<string, unknown>, sessionId?: string): Promise<Record<string, unknown>> {
    if (method === "Page.navigate") {
      queueMicrotask(() => this.emit({ method: "Page.loadEventFired", params: {}, sessionId }));
      return {};
    }
    if (method === "Runtime.evaluate") {
      return { result: { value: this.evaluate() } };
    }
    return {};
  }

  asSession(): OhioSosChromeSession {
    return this as unknown as OhioSosChromeSession;
  }
}

const TAB: OhioSosChromeTab = { targetId: "target-1", sessionId: "session-1" };

describe("watchOhioSosDownload", () => {
  it("replays a completion that arrived before wait() was armed", async () => {
    const session = new FakeChromeSession();
    const watcher = watchOhioSosDownload(session.asSession(), "/staging", TAB);
    session.emit({ method: "Browser.downloadWillBegin", params: { frameId: "target-1", guid: "g1" } });
    session.emit({ method: "Browser.downloadProgress", params: { guid: "g1", state: "completed" } });

    await expect(watcher.wait({ timeoutMs: 50 })).resolves.toEqual({ filePath: join("/staging", "g1") });
    watcher.dispose();
  });

  it("replays a cancellation that arrived before wait() was armed", async () => {
    const session = new FakeChromeSession();
    const watcher = watchOhioSosDownload(session.asSession(), "/staging", TAB);
    session.emit({ method: "Browser.downloadWillBegin", params: { frameId: "target-1", guid: "g1" } });
    session.emit({ method: "Browser.downloadProgress", params: { guid: "g1", state: "canceled" } });

    await expect(watcher.wait({ timeoutMs: 50 })).rejects.toThrow(/canceled the download/);
    watcher.dispose();
  });

  // The attached Chrome is the user's own profile: a download they start
  // themselves must not be claimed (and later deleted) by the watcher.
  it("ignores downloads from other frames", async () => {
    const session = new FakeChromeSession();
    const watcher = watchOhioSosDownload(session.asSession(), "/staging", TAB);
    session.emit({ method: "Browser.downloadWillBegin", params: { frameId: "user-tab", guid: "foreign" } });
    session.emit({ method: "Browser.downloadProgress", params: { guid: "foreign", state: "completed" } });

    await expect(watcher.wait({ timeoutMs: 20 })).rejects.toThrow(/Timed out/);
    watcher.dispose();
  });

  it("keeps the first claimed download when a second one begins", async () => {
    const session = new FakeChromeSession();
    const watcher = watchOhioSosDownload(session.asSession(), "/staging", TAB);
    session.emit({ method: "Browser.downloadWillBegin", params: { frameId: "target-1", guid: "g1" } });
    session.emit({ method: "Browser.downloadWillBegin", params: { frameId: "target-1", guid: "g2" } });
    session.emit({ method: "Browser.downloadProgress", params: { guid: "g2", state: "completed" } });
    session.emit({ method: "Browser.downloadProgress", params: { guid: "g1", state: "completed" } });

    await expect(watcher.wait({ timeoutMs: 50 })).resolves.toEqual({ filePath: join("/staging", "g1") });
    watcher.dispose();
  });
});

describe("fetchOhioSos31uDetails bundle persistence", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  async function makeCacheDir(): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), "ohio-31u-test-"));
    tempDirs.push(dir);
    return dir;
  }

  const noSleep = async () => {};
  const annualTotals = new Map([["100", { totalCents: 10_000, rowCount: 1 }]]);

  function detailTable(): { headers: string[]; rows: string[][] } {
    const row = new Array<string>(OHIO_SOS_31U_DETAIL_HEADER.length).fill("-");
    row[0] = "ACME MEDIA";
    row[7] = "$100.00";
    row[12] = "PAC X";
    row[15] = "SUPPORT";
    return { headers: [...OHIO_SOS_31U_DETAIL_HEADER], rows: [row] };
  }

  it("writes the bundle atomically on success, creating the cache dir", async () => {
    const cacheDir = join(await makeCacheDir(), "nested-does-not-exist-yet");
    const session = new FakeChromeSession(detailTable);

    const result = await fetchOhioSos31uDetails({
      session: session.asSession(),
      tab: TAB,
      cacheDir,
      cycleYear: 2026,
      annualTotals,
      sleep: noSleep,
    });

    expect(result.failures).toEqual([]);
    expect(result.written).toBe(true);
    const written = JSON.parse(await readFile(result.detailPath, "utf8")) as {
      reports: Array<{ reportKey: string; reconciled: boolean }>;
    };
    expect(written.reports).toHaveLength(1);
    expect(written.reports[0]).toMatchObject({ reportKey: "100", reconciled: true });
    // No .tmp remnants.
    expect((await readdir(cacheDir)).filter((name) => name.includes(".tmp-"))).toEqual([]);
  });

  it("preserves an existing bundle when any report fails", async () => {
    const cacheDir = await makeCacheDir();
    const detailPath = ohioSos31uDetailCachePath({ cacheDir, cycleYear: 2026 });
    const priorBundle = '{"version":1,"reports":[{"reportKey":"100"}]}\n';
    await writeFile(detailPath, priorBundle, "utf8");
    const session = new FakeChromeSession(() => {
      throw new Error("portal exploded");
    });

    const result = await fetchOhioSos31uDetails({
      session: session.asSession(),
      tab: TAB,
      cacheDir,
      cycleYear: 2026,
      annualTotals,
      sleep: noSleep,
    });

    expect(result.failures).toHaveLength(1);
    expect(result.written).toBe(false);
    expect(await readFile(detailPath, "utf8")).toBe(priorBundle);
  });

  it("still writes a partial bundle when nothing was cached before", async () => {
    const cacheDir = await makeCacheDir();
    const session = new FakeChromeSession(() => {
      throw new Error("portal exploded");
    });

    const result = await fetchOhioSos31uDetails({
      session: session.asSession(),
      tab: TAB,
      cacheDir,
      cycleYear: 2026,
      annualTotals,
      sleep: noSleep,
    });

    expect(result.failures).toHaveLength(1);
    expect(result.written).toBe(true);
    const written = JSON.parse(await readFile(result.detailPath, "utf8")) as {
      reports: unknown[];
      failures: Array<{ reportKey: string }>;
    };
    expect(written.reports).toEqual([]);
    expect(written.failures[0]).toMatchObject({ reportKey: "100" });
  });
});

describe("readOhioSos31uDetailBundle", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  async function makeCacheDir(): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), "ohio-31u-read-test-"));
    tempDirs.push(dir);
    return dir;
  }

  async function writeBundle(cacheDir: string, payload: unknown): Promise<void> {
    await writeFile(ohioSos31uDetailCachePath({ cacheDir, cycleYear: 2026 }), JSON.stringify(payload), "utf8");
  }

  function bundleRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
      reportKey: "100",
      spenderCommitteeName: "PAC X",
      amountCents: 10_000,
      direction: "support",
      candidateNameOrBallotIssue: "AMY ACTON",
      office: null,
      ...overrides,
    };
  }

  function versionOneBundle(rows: Array<Record<string, unknown>>): Record<string, unknown> {
    return {
      version: 1,
      cycleYear: 2026,
      retrievedAt: "2026-08-04T00:00:00.000Z",
      header: [...OHIO_SOS_31U_DETAIL_HEADER],
      reports: [
        {
          reportKey: "100",
          annualTotalCents: 10_000,
          detailTotalCents: 10_000,
          reconciled: true,
          rows,
        },
      ],
      failures: [],
    };
  }

  it("reads the version-1 payload with already-parsed rows", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, versionOneBundle([bundleRow()]));

    const reports = await readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 });
    expect(reports).toHaveLength(1);
    expect(reports[0]?.reportKey).toBe("100");
    expect(reports[0]?.rows[0]).toMatchObject({ amountCents: 10_000, direction: "support" });
  });

  it("rejects a version-1 row whose direction is outside the pinned vocabulary", async () => {
    // The aggregator's attribution branch treats any non-null direction
    // that is not "support" as opposition, so a bogus value must throw
    // here instead of becoming oppose money.
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, versionOneBundle([bundleRow({ direction: "unknown" })]));
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /row 1 has a malformed direction/
    );
  });

  it("rejects a version-1 row whose amount is not an integer cents value", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, versionOneBundle([bundleRow({ amountCents: "10000" })]));
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /row 1 has a malformed amountCents/
    );
  });

  it("rejects a version-1 row whose reportKey does not match its report", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, versionOneBundle([bundleRow({ reportKey: "999" })]));
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /row 1 has a malformed reportKey/
    );
  });

  it("rejects a version-1 report entry without a string reportKey", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, { version: 1, cycleYear: 2026, reports: [{ reportKey: 100, rows: [] }] });
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /malformed report entry/
    );
  });

  it("rejects a legacy entry without a string key", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, { rows: [{ key: 512315395, rows: [] }] });
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /malformed legacy report entry/
    );
  });

  it("rejects a version-1 payload without a reports array", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, { version: 1, cycleYear: 2026 });
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /unrecognized format/
    );
  });

  it("parses the legacy spike checkpoint through the pinned-header table parser", async () => {
    const cacheDir = await makeCacheDir();
    const row = new Array<string>(OHIO_SOS_31U_DETAIL_HEADER.length).fill("-");
    row[7] = "$150,000.00";
    row[12] = "V-PAC";
    row[13] = "GOVERNOR";
    row[14] = "AMY ACTON";
    row[15] = "OPPOSED";
    await writeBundle(cacheDir, { rows: [{ key: "512315395", n: 1, rows: [row] }], err: [], running: false });

    const reports = await readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 });
    expect(reports).toHaveLength(1);
    expect(reports[0]?.reportKey).toBe("512315395");
    expect(reports[0]?.rows[0]).toMatchObject({
      reportKey: "512315395",
      spenderCommitteeName: "V-PAC",
      amountCents: 15_000_000,
      office: "GOVERNOR",
      candidateNameOrBallotIssue: "AMY ACTON",
      direction: "oppose",
    });
  });

  it("rejects a version-1 bundle for a different cycle", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, { version: 1, cycleYear: 2024, reports: [] });
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /for cycle 2024, expected 2026/
    );
  });

  it("rejects an unrecognized payload shape", async () => {
    const cacheDir = await makeCacheDir();
    await writeBundle(cacheDir, { something: "else" });
    await expect(readOhioSos31uDetailBundle({ cacheDir, cycleYear: 2026 })).rejects.toThrow(
      /unrecognized format/
    );
  });
});

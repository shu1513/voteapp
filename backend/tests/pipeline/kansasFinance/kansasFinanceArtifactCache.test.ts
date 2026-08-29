import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  readKansasFinanceArtifact,
  readKansasFinanceArtifactVersion,
  storeKansasFinanceArtifact,
  type KansasFinanceArtifactKey,
} from "../../../src/pipeline/kansasFinance/kansasFinanceArtifactCache.js";

const KEY: KansasFinanceArtifactKey = {
  kind: "kpdc_pdf",
  id: "House/2026ElecCycle/202607/H001DH_202607.pdf",
};

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "ks-artifact-cache-"));
});

afterEach(async () => {
  await rm(cacheDir, { recursive: true, force: true });
});

describe("kansasFinanceArtifactCache", () => {
  it("round-trips a binary body with a verified manifest", async () => {
    const body = Buffer.from("%PDF-1.4 fake body");
    const stored = await storeKansasFinanceArtifact({
      cacheDir,
      key: KEY,
      sourceUrl: "https://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/202607/H001DH_202607.pdf",
      body,
      retrievedAt: new Date("2026-08-28T12:00:00Z"),
    });
    expect(stored.changed).toBe(true);
    expect(stored.manifest.supersedes).toBeNull();
    expect(stored.manifest.byteSize).toBe(body.byteLength);

    const read = await readKansasFinanceArtifact({ cacheDir, key: KEY });
    expect(read.body.equals(body)).toBe(true);
    expect(read.manifest).toEqual(stored.manifest);
  });

  it("treats byte-identical re-fetches as unchanged, keeping the original manifest", async () => {
    const body = "export table";
    const first = await storeKansasFinanceArtifact({
      cacheDir,
      key: { kind: "viewer_export", id: "contribution/Holscher/2026-01-01_2026-07-23" },
      sourceUrl: "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_contribution_results.aspx",
      body,
      retrievedAt: new Date("2026-08-28T12:00:00Z"),
    });
    const second = await storeKansasFinanceArtifact({
      cacheDir,
      key: { kind: "viewer_export", id: "contribution/Holscher/2026-01-01_2026-07-23" },
      sourceUrl: "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_contribution_results.aspx",
      body,
      retrievedAt: new Date("2026-08-29T12:00:00Z"),
    });
    expect(second.changed).toBe(false);
    expect(second.manifest.retrievedAt).toBe(first.manifest.retrievedAt);
  });

  it("keeps prior versions immutable and records supersession on changed bytes", async () => {
    const store = (body: string, retrievedAt: string) =>
      storeKansasFinanceArtifact({
        cacheDir,
        key: KEY,
        sourceUrl: "https://www.kansas.gov/x.pdf",
        body,
        retrievedAt: new Date(retrievedAt),
      });
    const v1 = await store("version one", "2026-08-28T12:00:00Z");
    const v2 = await store("version two", "2026-08-29T12:00:00Z");

    expect(v2.changed).toBe(true);
    expect(v2.manifest.supersedes).toBe(v1.manifest.sha256);

    const latest = await readKansasFinanceArtifact({ cacheDir, key: KEY });
    expect(latest.body.toString("utf8")).toBe("version two");

    const prior = await readKansasFinanceArtifactVersion({
      cacheDir,
      key: KEY,
      sha256: v1.manifest.sha256,
    });
    expect(prior.body.toString("utf8")).toBe("version one");
    expect(prior.manifest.supersedes).toBeNull();
  });

  it("fails closed on tampered bytes", async () => {
    const stored = await storeKansasFinanceArtifact({
      cacheDir,
      key: KEY,
      sourceUrl: "https://www.kansas.gov/x.pdf",
      body: "original",
    });
    await writeFile(
      resolve(cacheDir, KEY.kind, KEY.id, `v-${stored.manifest.sha256}.bin`),
      "tampered!"
    );
    await expect(readKansasFinanceArtifact({ cacheDir, key: KEY })).rejects.toThrow(
      "Corrupt Kansas finance artifact"
    );
  });

  it("rejects traversal-shaped and malformed ids, kinds, and bodies", async () => {
    const store = (key: KansasFinanceArtifactKey) =>
      storeKansasFinanceArtifact({ cacheDir, key, sourceUrl: "https://x", body: "b" });
    await expect(store({ kind: "kpdc_pdf", id: "../escape.pdf" })).rejects.toThrow(
      "Invalid Kansas finance artifact id"
    );
    await expect(store({ kind: "kpdc_pdf", id: "/absolute.pdf" })).rejects.toThrow(
      "Invalid Kansas finance artifact id"
    );
    await expect(store({ kind: "nope" as never, id: "x.pdf" })).rejects.toThrow(
      "Invalid Kansas finance artifact kind"
    );
    await expect(
      storeKansasFinanceArtifact({ cacheDir, key: KEY, sourceUrl: "https://x", body: "" })
    ).rejects.toThrow("Refusing to store empty");
  });

  it("reports a missing artifact by key", async () => {
    await expect(
      readKansasFinanceArtifact({ cacheDir, key: { kind: "kpdc_index", id: "nope.htm" } })
    ).rejects.toThrow("Missing Kansas finance artifact: kpdc_index nope.htm");
  });

  it("stores artifacts with restricted file modes", async () => {
    const stored = await storeKansasFinanceArtifact({
      cacheDir,
      key: KEY,
      sourceUrl: "https://www.kansas.gov/x.pdf",
      body: "pii-bearing",
    });
    const { statSync } = await import("node:fs");
    const mode = statSync(resolve(cacheDir, KEY.kind, KEY.id, `v-${stored.manifest.sha256}.bin`)).mode;
    expect(mode & 0o077).toBe(0);
  });

  it("round-trips through the manifest file on disk", async () => {
    const stored = await storeKansasFinanceArtifact({
      cacheDir,
      key: KEY,
      sourceUrl: "https://www.kansas.gov/x.pdf",
      body: "body",
    });
    const onDisk = JSON.parse(
      await readFile(
        resolve(cacheDir, KEY.kind, KEY.id, `v-${stored.manifest.sha256}.manifest.json`),
        "utf8"
      )
    );
    expect(onDisk).toEqual(stored.manifest);
  });
});

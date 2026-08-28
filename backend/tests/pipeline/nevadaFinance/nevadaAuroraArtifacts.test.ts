import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { readNevadaMonthlyContributions } from "../../../src/pipeline/nevadaFinance/nevadaAuroraArtifacts.js";

const HEADER = '"Contributor","Date","Amount","Type","Recipient","Report"\r\n';
const ROW = '"A Donor","6/5/2026","$1.00","Monetary Contribution","A B","2026 CE Report 2"\r\n';

describe("readNevadaMonthlyContributions", () => {
  let artifactDir: string;
  let contributionsDir: string;

  beforeEach(async () => {
    artifactDir = await mkdtemp(join(tmpdir(), "nv-aurora-"));
    contributionsDir = join(artifactDir, "contributions");
    await mkdir(contributionsDir);
  });

  afterEach(async () => {
    await rm(artifactDir, { recursive: true, force: true });
  });

  async function write(name: string): Promise<void> {
    await writeFile(join(contributionsDir, name), HEADER + ROW, "utf8");
  }

  it("accepts one full-month file or a contiguous split run", async () => {
    await write("2026-05.csv");
    await write("2026-06-a.csv");
    await write("2026-06-b.csv");
    await write("2026-06-c.csv");
    const load = await readNevadaMonthlyContributions(artifactDir, "2026-05", "2026-06");
    expect(load.fileCount).toBe(4);
    expect(load.rows).toHaveLength(4);
    expect(load.monthsLoaded).toEqual(["2026-05", "2026-06"]);
  });

  it("rejects a missing month, a lone split file, a split gap, and mixed sets", async () => {
    await expect(readNevadaMonthlyContributions(artifactDir, "2026-06", "2026-06")).rejects.toThrow(
      /missing/
    );

    await write("2026-06-a.csv");
    await expect(readNevadaMonthlyContributions(artifactDir, "2026-06", "2026-06")).rejects.toThrow(
      /incomplete split set/
    );

    await write("2026-06-c.csv");
    await expect(readNevadaMonthlyContributions(artifactDir, "2026-06", "2026-06")).rejects.toThrow(
      /incomplete split set/
    );

    await write("2026-06-b.csv");
    await write("2026-06.csv");
    await expect(readNevadaMonthlyContributions(artifactDir, "2026-06", "2026-06")).rejects.toThrow(
      /mix a full-month file/
    );
  });
});

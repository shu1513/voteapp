import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  normalizeConnecticutEcrisHeader,
  parseConnecticutEcrisCsvRows,
  readConnecticutEcrisArtifactRows,
} from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactReader.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-ct-ecris-reader-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("connecticutEcrisArtifactReader", () => {
  it("normalizes headers", () => {
    expect(normalizeConnecticutEcrisHeader("\uFEFF Committee Name ")).toBe("Committee Name");
  });

  it("parses eCRIS CSV rows with quoted fields", () => {
    const rows = parseConnecticutEcrisCsvRows(
      'Committee ID,Committee Name,Amount,Contributor Occupation\nC1,"Smith, Jane for CT",100.50,"Attorney"\nC2,Another Committee,,""\n'
    );

    expect(rows).toEqual([
      {
        "Committee ID": "C1",
        "Committee Name": "Smith, Jane for CT",
        Amount: "100.50",
        "Contributor Occupation": "Attorney",
      },
      {
        "Committee ID": "C2",
        "Committee Name": "Another Committee",
        Amount: "",
        "Contributor Occupation": "",
      },
    ]);
  });

  it("streams CSV rows from disk with predicate and maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "receipts.csv");
    await writeFile(
      filePath,
      [
        "Committee ID,Committee Name,Amount,Contributor Occupation",
        "C1,One,10,Attorney",
        "C2,Two,20,Teacher",
        "C3,Three,30,Attorney",
        "C4,Four,40,Attorney",
      ].join("\n"),
      "utf8"
    );

    await expect(
      readConnecticutEcrisArtifactRows({
        filePath,
        format: "csv",
        predicate: (row) => row["Contributor Occupation"] === "Attorney",
        maxRows: 2,
      })
    ).resolves.toEqual([
      {
        "Committee ID": "C1",
        "Committee Name": "One",
        Amount: "10",
        "Contributor Occupation": "Attorney",
      },
      {
        "Committee ID": "C3",
        "Committee Name": "Three",
        Amount: "30",
        "Contributor Occupation": "Attorney",
      },
    ]);
  });

  it("rejects duplicate headers", () => {
    expect(() => parseConnecticutEcrisCsvRows("Committee ID,Committee ID\nC1,C2\n")).toThrow(
      "Duplicate Connecticut eCRIS CSV header"
    );
  });

  it("rejects malformed quoted CSV", () => {
    expect(() => parseConnecticutEcrisCsvRows('Committee ID,Name\nC1,"unterminated\n')).toThrow(
      "unterminated quoted field"
    );
  });

  it("rejects spreadsheet artifacts until an explicit parser is added", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "receipts.xlsx");
    await writeFile(filePath, "fake", "utf8");

    await expect(readConnecticutEcrisArtifactRows({ filePath, format: "xlsx" })).rejects.toThrow(
      "not supported by the CSV reader"
    );
  });

  it("rejects invalid maxRows", async () => {
    const dir = await makeTempDir();
    const filePath = join(dir, "receipts.csv");
    await writeFile(filePath, "A\n1\n", "utf8");

    await expect(readConnecticutEcrisArtifactRows({ filePath, format: "csv", maxRows: 0 })).rejects.toThrow(
      "Invalid Connecticut eCRIS maxRows"
    );
  });
});

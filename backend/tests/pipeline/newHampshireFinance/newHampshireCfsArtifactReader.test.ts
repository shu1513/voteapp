import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  readNewHampshireExpenditureCsvArtifact,
  readNewHampshireReceiptCsvArtifact,
  validateNewHampshireExpenditureCsvArtifact,
  validateNewHampshireReceiptCsvArtifact,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsArtifactReader.js";

const fixtures = new URL("../../fixtures/newHampshireFinance/", import.meta.url);
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function temporaryFile(body: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "nh-cfs-reader-test-"));
  tempDirs.push(dir);
  const path = join(dir, "artifact.csv");
  await writeFile(path, body, "utf8");
  return path;
}

describe("New Hampshire CFS artifact reader", () => {
  it("streams malformed vendor CSV using quote-aware numeric boundaries", async () => {
    const rows = await readNewHampshireReceiptCsvArtifact({
      filePath: new URL("receipts-sanitized.csv", fixtures).pathname,
    });
    expect(rows).toHaveLength(5);
    expect(rows[2]?.Description).toBe("First line, extra comma\n123, Main Street\nsecond line");
    expect(rows[3]?.Description).toBe('"Malformed "INNER")');

    const expenditures = await readNewHampshireExpenditureCsvArtifact({
      filePath: new URL("expenditures-sanitized.csv", fixtures).pathname,
    });
    expect(expenditures).toHaveLength(2);
    expect(expenditures[1]?.["Transaction Description"]).toBe('Vendor purpose with "INNER"');
  });

  it("applies predicates before maxRows", async () => {
    const rows = await readNewHampshireReceiptCsvArtifact({
      filePath: new URL("receipts-sanitized.csv", fixtures).pathname,
      predicate: (row) => row["Filing Entity ID"] === "50450",
      maxRows: 2,
    });
    expect(rows).toHaveLength(2);
    expect(rows.every((row) => row["Filing Entity ID"] === "50450")).toBe(true);
  });

  it("validates complete artifacts without collecting their rows", async () => {
    await expect(
      validateNewHampshireReceiptCsvArtifact({
        filePath: new URL("receipts-sanitized.csv", fixtures).pathname,
      })
    ).resolves.toEqual({ rowCount: 5 });
    await expect(
      validateNewHampshireExpenditureCsvArtifact({
        filePath: new URL("expenditures-sanitized.csv", fixtures).pathname,
      })
    ).resolves.toEqual({ rowCount: 2 });
  });

  it("fails closed on changed headers, stray physical lines, and invalid limits", async () => {
    const wrongHeader = await temporaryFile("wrong,header\n1,value\n");
    await expect(readNewHampshireReceiptCsvArtifact({ filePath: wrongHeader })).rejects.toThrow(
      "header changed"
    );

    const receiptHeader =
      "Filing Entity ID,Candidate Name,Committee Name,Committee Subtype,Transaction Type,Transaction Sub Type,Election Period,Election year,Date of Receipt,Amount of receipt,Contributor Type,Contributor Name,Contributor Address Line 1,Contributor Address Line 2,Contributor City,Contributor State,Contributor Zip Code,Contributor occupation,Contributor Employer,Contributor Principle place of Business,Description,Timed Report\n";
    const strayLine = await temporaryFile(`${receiptHeader}not-a-record\n`);
    await expect(readNewHampshireReceiptCsvArtifact({ filePath: strayLine })).rejects.toThrow(
      "does not start with a numeric Filing Entity ID"
    );
    await expect(
      readNewHampshireReceiptCsvArtifact({ filePath: strayLine, maxRows: 0 })
    ).rejects.toThrow("Invalid New Hampshire CFS maxRows");
  });
});

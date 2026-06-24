import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS,
  MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS,
  listMichiganMitnLegacyContributionCsvFileNames,
  listMichiganMitnLegacyExtractedFileNames,
  michiganMitnLegacyExpendituresCsvFileName,
  michiganMitnLegacyReceiptsCsvFileName,
  parseMichiganMitnLegacyCsvRows,
  readMichiganMitnLegacyContributionRows,
  readMichiganMitnLegacyCsvTableRows,
  readMichiganMitnLegacyExpenditureRows,
} from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "voteapp-mi-mitn-reader-"));
  tempDirs.push(dir);
  return dir;
}

function csvCell(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function csvRow(headers: readonly string[], values: Record<string, string>): string {
  return headers.map((header) => csvCell(values[header] ?? "")).join(",");
}

function csv(headers: readonly string[], rows: readonly Record<string, string>[]): string {
  return [headers.join(","), ...rows.map((row) => csvRow(headers, row))].join("\n");
}

async function writeCsv(dir: string, fileName: string, headers: readonly string[], rows: readonly Record<string, string>[]): Promise<void> {
  await writeFile(join(dir, fileName), `${csv(headers, rows)}\n`, "utf8");
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Michigan MiTN legacy archive reader", () => {
  it("builds known yearly table file names", () => {
    expect(michiganMitnLegacyReceiptsCsvFileName(2022)).toBe("2022_mi_cfr_receipts.csv");
    expect(michiganMitnLegacyExpendituresCsvFileName(2022)).toBe("2022_mi_cfr_expenditures.csv");
    expect(() => michiganMitnLegacyReceiptsCsvFileName(2019)).toThrow("Invalid Michigan MiTN legacy archive year");
  });

  it("parses quoted CSV cells and trims headers and values", () => {
    const rows = parseMichiganMitnLegacyCsvRows({
      csv: "\uFEFFname,amount,notes\n\"Smith, Jane\", \"100.00\" ,\"line 1\nline 2\"\n",
      requiredColumns: ["name", "amount"],
      tableLabel: "fixture",
    });

    expect(rows).toEqual([
      {
        name: "Smith, Jane",
        amount: "100.00",
        notes: "line 1\nline 2",
      },
    ]);
  });

  it("lists extracted files and contribution split files for the selected year", async () => {
    const dir = await makeTempDir();
    await writeFile(join(dir, "2022_mi_cfr_contributions 10 of 12.csv"), "a\n", "utf8");
    await writeFile(join(dir, "2022_mi_cfr_contributions 2 of 2.csv"), "a\n", "utf8");
    await writeFile(join(dir, "2022_mi_cfr_contributions 1 of 2.csv"), "a\n", "utf8");
    await writeFile(join(dir, "2021_mi_cfr_contributions 1 of 1.csv"), "a\n", "utf8");
    await writeFile(join(dir, "2022_mi_cfr_expenditures.csv"), "a\n", "utf8");
    await mkdir(join(dir, "ignored-directory"));

    await expect(listMichiganMitnLegacyExtractedFileNames(dir)).resolves.toEqual([
      "2021_mi_cfr_contributions 1 of 1.csv",
      "2022_mi_cfr_contributions 1 of 2.csv",
      "2022_mi_cfr_contributions 2 of 2.csv",
      "2022_mi_cfr_contributions 10 of 12.csv",
      "2022_mi_cfr_expenditures.csv",
    ]);
    await expect(listMichiganMitnLegacyContributionCsvFileNames({ extractedDir: dir, year: 2022 })).resolves.toEqual([
      "2022_mi_cfr_contributions 1 of 2.csv",
      "2022_mi_cfr_contributions 2 of 2.csv",
      "2022_mi_cfr_contributions 10 of 12.csv",
    ]);
  });

  it("streams a selected CSV table with predicate and maxRows", async () => {
    const dir = await makeTempDir();
    await writeCsv(dir, "custom.csv", ["committee", "amount", "note"], [
      { committee: "A", amount: "10", note: "skip" },
      { committee: "B", amount: "20", note: "keep" },
      { committee: "B", amount: "30", note: "keep again" },
    ]);

    await expect(
      readMichiganMitnLegacyCsvTableRows({
        extractedDir: dir,
        fileName: "custom.csv",
        predicate: (row) => row.committee === "B",
        maxRows: 1,
        requiredColumns: ["committee", "amount"],
      })
    ).resolves.toEqual([
      {
        committee: "B",
        amount: "20",
        note: "keep",
      },
    ]);
  });

  it("reads contribution rows across split files with a global maxRows", async () => {
    const dir = await makeTempDir();
    await writeCsv(dir, "2022_mi_cfr_contributions 1 of 2.csv", MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS, [
      {
        cfr_com_id: "520012",
        com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
        l_name_or_org: "PETROPLEX ENERGY",
        occupation: "",
        employer: "",
        amount: "125000.00",
      },
      {
        cfr_com_id: "999999",
        com_legal_name: "OTHER COMMITTEE",
        l_name_or_org: "OTHER DONOR",
        amount: "10.00",
      },
    ]);
    await writeCsv(dir, "2022_mi_cfr_contributions 2 of 2.csv", MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS, [
      {
        cfr_com_id: "520012",
        com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
        l_name_or_org: "OTIS EASTERN SERVICE LLC",
        amount: "100000.00",
      },
    ]);

    await expect(
      readMichiganMitnLegacyContributionRows({
        extractedDir: dir,
        year: 2022,
        predicate: (row) => row.cfr_com_id === "520012",
        maxRows: 2,
      })
    ).resolves.toMatchObject([
      {
        cfr_com_id: "520012",
        l_name_or_org: "PETROPLEX ENERGY",
        amount: "125000.00",
      },
      {
        cfr_com_id: "520012",
        l_name_or_org: "OTIS EASTERN SERVICE LLC",
        amount: "100000.00",
      },
    ]);
  });

  it("reads expenditure rows with strict required columns", async () => {
    const dir = await makeTempDir();
    await writeCsv(dir, "2022_mi_cfr_expenditures.csv", MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS, [
      {
        cfr_com_id: "520012",
        com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
        schedule_desc: "INDEPENDENT EXPENDITURE",
        supp_opp: "2",
        can_or_ballot: "GRETCHEN WHITMER",
        amount: "863076.75",
      },
    ]);

    await expect(
      readMichiganMitnLegacyExpenditureRows({
        extractedDir: dir,
        year: 2022,
        predicate: (row) => row.schedule_desc.includes("INDEPENDENT"),
      })
    ).resolves.toMatchObject([
      {
        cfr_com_id: "520012",
        supp_opp: "2",
        can_or_ballot: "GRETCHEN WHITMER",
        amount: "863076.75",
      },
    ]);
  });

  it("fails loudly when a required typed column is missing", async () => {
    const dir = await makeTempDir();
    await writeCsv(dir, "2022_mi_cfr_expenditures.csv", ["cfr_com_id", "amount"], [
      {
        cfr_com_id: "520012",
        amount: "100",
      },
    ]);

    await expect(
      readMichiganMitnLegacyExpenditureRows({
        extractedDir: dir,
        year: 2022,
      })
    ).rejects.toThrow("Missing required Michigan MiTN legacy expenditures CSV column: doc_seq_no");
  });
});

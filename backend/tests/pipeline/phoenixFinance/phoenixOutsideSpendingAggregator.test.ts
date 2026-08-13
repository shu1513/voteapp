import { describe, expect, it } from "vitest";
import {
  aggregatePhoenixOutsideSpending,
  type PhoenixOutsidePoolEntry,
} from "../../../src/pipeline/phoenixFinance/phoenixOutsideSpendingAggregator.js";
import type { PhoenixB6Entry } from "../../../src/pipeline/phoenixFinance/phoenixReportPdfParser.js";
import {
  validatePhoenixOutsideSupplements,
  type PhoenixOutsideSupplement,
} from "../../../src/pipeline/phoenixFinance/phoenixOutsideSpendingSupplements.js";

const hermes = {
  displayName: "Ed Hermes",
  officeName: "City Council Member" as const,
  districtNumber: 4,
  electionDate: "2026-11-03",
};

function poolEntry(over: Partial<PhoenixB6Entry> = {}): PhoenixOutsidePoolEntry {
  const entry: PhoenixB6Entry = {
    amountCents: 650_000,
    supportedNames: ["Ed", "Hermes"],
    supportedPercents: [100],
    opposedNames: [],
    opposedPercents: [],
    electionText: "2026",
    officeText: "City Council",
    ...over,
  };
  return {
    spenderCopId: "PAC-22-14",
    spenderName: "Some IE PAC",
    reportPackageId: "d7118529-0000-0000-0000-000000000000",
    entry,
  };
}

function supplement(over: Partial<PhoenixOutsideSupplement> = {}): PhoenixOutsideSupplement {
  return {
    electionYear: 2026,
    channel: "standing_pac",
    spenderFilerId: "PAC-19-5",
    spenderName: "UNITE HERE Arizona",
    candidateName: "Ed Hermes",
    officeSought: "City Council",
    districtNumber: 4,
    direction: "oppose",
    amountCents: 120_000,
    expenditureDate: "2026-09-01",
    filingReference: "sos-report-123",
    sourceNote: "Transcribed from the SOS filing 2026-09-05.",
    ...over,
  };
}

describe("aggregatePhoenixOutsideSpending", () => {
  it("books a single-name 100% supported entry to the candidate", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [poolEntry()],
    });
    expect(result.supportTotalCents).toBe(650_000);
    expect(result.opposeTotalCents).toBe(0);
    expect(result.groups).toEqual([
      {
        spenderFilerId: "PAC-22-14",
        spenderName: "Some IE PAC",
        direction: "support",
        amountCents: 650_000,
        expenditureCount: 1,
      },
    ]);
  });

  it("accepts a blank % for a single disclosed name (reads as 100)", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [poolEntry({ supportedPercents: [] })],
    });
    expect(result.supportTotalCents).toBe(650_000);
  });

  it("drops prior-cycle entries before the name checks (the live pool is all 2023/2024)", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        // Blank names AND a 2024 election: not this contest's money, so it
        // must not surface as an unattributable gap in THIS race.
        poolEntry({
          electionText: "2024",
          supportedNames: [],
          supportedPercents: [],
          opposedPercents: [100],
        }),
        poolEntry({ electionText: "03/2023", opposedNames: ["Sam", "Stone"] }),
      ],
    });
    expect(result.diagnostics.outOfCycleEntries).toBe(2);
    expect(result.diagnostics.unattributableEntries).toBe(0);
    expect(result.diagnostics.vetoedRows).toBe(0);
    expect(result.diagnostics.otherCandidateRows).toBe(0);
  });

  it("fails closed on the pinned live entry: blank names on both blocks", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({
          supportedNames: [],
          supportedPercents: [],
          opposedNames: [],
          opposedPercents: [100],
        }),
      ],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.unattributableEntries).toBe(1);
    expect(result.diagnostics.unattributableCents).toBe(650_000);
  });

  it("excludes partial percentages and multi-candidate splits un-pro-rated", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({ supportedPercents: [50] }),
        poolEntry({ supportedNames: ["Ed Hermes & Ashley Harder"], supportedPercents: [] }),
      ],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.partialAttributionRows).toBe(2);
  });

  it("books a 'Last, First' inverted single name — a comma alone is not a split", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({ supportedNames: ["Hermes, Ed"] }),
        // Trailing separator from cell fragmentation is punctuation, not a split.
        poolEntry({ supportedNames: ["Ed Hermes,"] }),
      ],
    });
    expect(result.supportTotalCents).toBe(1_300_000);
    expect(result.diagnostics.partialAttributionRows).toBe(0);
  });

  it("counts a multi-candidate split as another candidate's row when this candidate is not named", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({ supportedNames: ["Ashley Harder & Sam Stone"], supportedPercents: [] }),
      ],
    });
    expect(result.supportTotalCents).toBe(0);
    // Hermes is not in the split, so his coverage note must not disclose it
    // as partial attribution.
    expect(result.diagnostics.partialAttributionRows).toBe(0);
    expect(result.diagnostics.otherCandidateRows).toBe(1);
  });

  it("vetoes office, district digits, and cycle mismatches on name-matched rows", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({ officeText: "Mayor" }),
        poolEntry({ officeText: "City Council District 6" }),
        poolEntry({ officeText: null }),
      ],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.vetoedRows).toBe(3);
  });

  it("counts an entry with no usable election as unplaceable, not as a veto", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [poolEntry({ electionText: null })],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.undatedEntries).toBe(1);
    expect(result.diagnostics.unattributableCents).toBe(650_000);
    expect(result.diagnostics.vetoedRows).toBe(0);
  });

  it("keeps a March-2027 runoff disclosure in the 2026 cycle", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: { ...hermes, electionDate: "2027-03-09" },
      pool: [poolEntry({ electionText: "3/2027" })],
    });
    expect(result.supportTotalCents).toBe(650_000);
  });

  it("counts a row naming another candidate as other-candidate, not vetoed", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [poolEntry({ supportedNames: ["Ashley", "Harder"] })],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.otherCandidateRows).toBe(1);
  });

  it("books both blocks of one entry independently (support A, oppose B)", () => {
    const opposed = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [
        poolEntry({
          supportedNames: ["Ashley", "Harder"],
          opposedNames: ["Ed", "Hermes"],
          opposedPercents: [100],
        }),
      ],
    });
    expect(opposed.opposeTotalCents).toBe(650_000);
    expect(opposed.diagnostics.otherCandidateRows).toBe(1);
  });

  it("runs supplements through the same gates and groups by spender", () => {
    const result = aggregatePhoenixOutsideSpending({
      candidate: hermes,
      pool: [],
      supplements: [
        supplement(),
        supplement({ filingReference: "sos-report-124", amountCents: 80_000 }),
        supplement({ filingReference: "sos-report-125", districtNumber: 6 }),
        supplement({ filingReference: "sos-report-126", candidateName: "Ashley Harder" }),
        supplement({ filingReference: "sos-report-127", officeSought: "Mayor" }),
      ],
    });
    expect(result.opposeTotalCents).toBe(200_000);
    expect(result.groups).toEqual([
      expect.objectContaining({
        spenderFilerId: "PAC-19-5",
        direction: "oppose",
        amountCents: 200_000,
        expenditureCount: 2,
      }),
    ]);
    expect(result.diagnostics.supplementRowsIncluded).toBe(2);
    expect(result.diagnostics.vetoedRows).toBe(2);
    expect(result.diagnostics.otherCandidateRows).toBe(1);
  });

  it("requires a district for council candidates", () => {
    expect(() =>
      aggregatePhoenixOutsideSpending({
        candidate: { ...hermes, districtNumber: null },
        pool: [],
      }),
    ).toThrow(/needs a district/);
  });
});

describe("validatePhoenixOutsideSupplements", () => {
  it("rejects defective curated entries loudly", () => {
    expect(() =>
      validatePhoenixOutsideSupplements([supplement({ amountCents: 0 })]),
    ).toThrow(/amountCents/);
    expect(() =>
      validatePhoenixOutsideSupplements([supplement({ expenditureDate: "2026-02-29" })]),
    ).toThrow(/not a calendar date/);
    expect(() =>
      validatePhoenixOutsideSupplements([
        supplement({ channel: "spotlight" as never }),
      ]),
    ).toThrow(/unknown channel/);
    expect(() =>
      validatePhoenixOutsideSupplements([supplement(), supplement()]),
    ).toThrow(/duplicate entry/);
    expect(() =>
      validatePhoenixOutsideSupplements([supplement({ districtNumber: 9 })]),
    ).toThrow(/districtNumber/);
  });
});

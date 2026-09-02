import { describe, expect, it } from "vitest";

import {
  aggregateIdahoOutsideSpending,
  IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL,
  idahoIeTargetName,
  idahoOutsideFilerKey,
} from "../../../src/pipeline/idahoFinance/idahoOutsideSpendingAggregator.js";
import { GUID_A, GUID_B, GUID_C, independentExpenditure, registration } from "./idahoTestFixtures.js";

const GUID_D = "11111111-1111-4111-8111-111111111104";
const PAC_GUID = "55555555-5555-4555-8555-555555555501";

// The linked 2026 Senate registration, its 2024 Senate registration, its 2025
// city-council registration, and another person's 2026 Senate registration.
const linked = registration({ registrationGuid: GUID_A });
const prior = registration({ registrationGuid: GUID_B, filerRegistrationId: 900, electionYear: 2024, filingCycleId: 5 });
const cityCouncil = registration({
  registrationGuid: GUID_C,
  filerRegistrationId: 950,
  electionYear: 2025,
  office: "City Council",
  district: "Post Falls",
});
const otherPerson = registration({
  registrationGuid: GUID_D,
  entityGuid: "22222222-2222-4222-8222-222222222299",
  filerEntityId: 999,
  filerRegistrationId: 990,
  filerName: "Other, Person",
  firstName: "Person",
  middleName: null,
  lastName: "Other",
});
const grid = [otherPerson, cityCouncil, prior, linked];

function row(n: number, overrides: Parameters<typeof independentExpenditure>[0] = {}) {
  return independentExpenditure({ guid: `44444444-4444-4444-8444-${String(n).padStart(12, "0")}`, ...overrides });
}

function nonRegisteredFiler(filerName: string) {
  return { filerName, filerRegistrationGuid: null, isNonRegisteredEntity: true, transactionTypeCode: "TEXP" } as const;
}

function nameOnly(candidateMeasure: string, overrides: Parameters<typeof independentExpenditure>[0] = {}) {
  return {
    candidateMeasure,
    candidateMeasureFilerRegistrationGuid: null,
    isCandidateNonRegisteredEntity: true,
    ...nonRegisteredFiler("Rebecca Lee Crea"),
    ...overrides,
  };
}

describe("aggregateIdahoOutsideSpending", () => {
  it("counts rows on every same-office registration of the entity inside the cycle window and groups them by filer", () => {
    const rows = [
      row(1, { amountApplied: 250 }),
      // Filed against the 2024 registration but dated in the 2026 cycle.
      row(2, { candidateMeasureFilerRegistrationGuid: GUID_B, amountApplied: 100.1, transactionDate: "2026-05-01T00:00:00" }),
      // The 2024 race itself.
      row(3, { candidateMeasureFilerRegistrationGuid: GUID_B, amountApplied: 5000, transactionDate: "2024-10-01T00:00:00" }),
      // Declared for the city-council registration.
      row(4, { candidateMeasureFilerRegistrationGuid: GUID_C, amountApplied: 40, transactionDate: "2026-05-18T00:00:00" }),
      // One federal PAC under two spellings of its FEC id.
      row(5, { amountApplied: 75.25, stance: "Oppose", ...nonRegisteredFiler("Make Liberty Win (FEC ID: C00731133)"), transactionDate: "2026-06-01T00:00:00" }),
      row(6, { amountApplied: 24.75, stance: "Oppose", ...nonRegisteredFiler("Make Liberty Win (C00731133)"), transactionDate: "2026-06-02T00:00:00" }),
      row(7, { amountApplied: 0 }),
      // Another person's race.
      row(8, { candidateMeasureFilerRegistrationGuid: GUID_D, amountApplied: 999 }),
      // After the election year.
      row(9, { amountApplied: 10, transactionDate: "2027-01-05T00:00:00" }),
      row(10, nameOnly("Achilles, Todd", { amountApplied: 150, transactionDate: "2026-05-05T00:00:00" })),
      // A measure row and an identical allocation row (the state counts both).
      row(11, { candidateMeasure: "Some Levy", officeSought: null, candidateMeasureFilerRegistrationGuid: null, isCandidateNonRegisteredEntity: true, amountApplied: 777 }),
      row(1, { amountApplied: 250 }),
    ];

    const result = aggregateIdahoOutsideSpending({ registration: linked, registrations: grid, expenditureRows: rows });

    expect(result.summary).toEqual({
      supportTotal: 750.1,
      opposeTotal: 100,
      groups: [
        { filerKey: PAC_GUID, filerName: "Sample PAC", supportOppose: "support", amount: 600.1, sourceUrl: IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL },
        { filerKey: "name:REBECCA LEE CREA", filerName: "Rebecca Lee Crea", supportOppose: "support", amount: 150, sourceUrl: IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL },
        { filerKey: "fec:C00731133", filerName: "Make Liberty Win (C00731133)", supportOppose: "oppose", amount: 100, sourceUrl: IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL },
      ],
      sourceUrl: IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL,
    });
    expect(result).toMatchObject({
      windowStartYear: 2024,
      windowEndYear: 2026,
      raceRegistrationGuids: [GUID_A, GUID_B],
      sourceRowCount: 12,
      includedRowCount: 6,
      guidResolvedRowCount: 5,
      priorRegistrationRowCount: 1,
      nameResolvedRowCount: 1,
      outOfWindowRowCount: 2,
      otherOfficeRowCount: 1,
      ambiguousNameRowCount: 0,
      nonPositiveRowCount: 1,
    });
  });

  it("uses a two-year window without a previous same-office registration and a custom source url", () => {
    const result = aggregateIdahoOutsideSpending({
      registration: linked,
      registrations: [linked, otherPerson],
      sourceUrl: " https://example.test/ie ",
      expenditureRows: [
        row(1, { amountApplied: 10, transactionDate: "2025-01-01T00:00:00" }),
        row(2, { amountApplied: 20, transactionDate: "2024-12-31T00:00:00" }),
      ],
    });
    expect(result).toMatchObject({ windowStartYear: 2024, raceRegistrationGuids: [GUID_A], includedRowCount: 1, outOfWindowRowCount: 1 });
    expect(result.summary).toMatchObject({ supportTotal: 10, sourceUrl: "https://example.test/ie" });
    expect(result.summary.groups[0]?.sourceUrl).toBe("https://example.test/ie");

    const empty = aggregateIdahoOutsideSpending({ registration: linked, registrations: [linked], expenditureRows: [] });
    expect(empty.summary).toEqual({ supportTotal: 0, opposeTotal: 0, groups: [], sourceUrl: IDAHO_INDEPENDENT_EXPENDITURE_PAGE_URL });
  });

  it("matches name-only targets with nickname expansion, the office guard, and other-entity ambiguity", () => {
    const foreman = registration({
      registrationGuid: GUID_A,
      filerName: "Foreman, Daniel David",
      firstName: "Daniel",
      middleName: "David",
      lastName: "Foreman",
    });
    const namesake = registration({
      registrationGuid: GUID_D,
      filerEntityId: 999,
      filerRegistrationId: 990,
      electionYear: 2024,
      filerName: "Foreman, Dan Edward",
      firstName: "Dan",
      middleName: "Edward",
      lastName: "Foreman",
    });
    const rows = [
      row(1, nameOnly("Foreman, Dan", { amountApplied: 150 })),
      row(2, nameOnly("Foreman, Daniel David", { amountApplied: 50, stance: "Oppose" })),
      row(3, nameOnly("Foreman, Dan", { amountApplied: 30, officeSought: "State Representative" })),
      row(4, nameOnly("Foreman, Dan", { amountApplied: 20, transactionDate: "2024-03-19T00:00:00" })),
      // Middle-name conflict and a different surname never match.
      row(5, nameOnly("Foreman, Daniel Edward", { amountApplied: 70 })),
      row(6, nameOnly("Forman, Dan", { amountApplied: 80 })),
    ];

    const alone = aggregateIdahoOutsideSpending({ registration: foreman, registrations: [foreman], expenditureRows: rows });
    expect(alone.summary).toMatchObject({ supportTotal: 150, opposeTotal: 50 });
    expect(alone).toMatchObject({
      includedRowCount: 2,
      nameResolvedRowCount: 2,
      otherOfficeRowCount: 1,
      outOfWindowRowCount: 1,
      ambiguousNameRowCount: 0,
    });

    // A same-office registration of another entity with the same first and
    // last name makes the bare-name row ambiguous; the full-name row still
    // resolves because its middle name contradicts the namesake's.
    const shared = aggregateIdahoOutsideSpending({ registration: foreman, registrations: [foreman, namesake], expenditureRows: rows });
    expect(shared.summary).toMatchObject({ supportTotal: 0, opposeTotal: 50 });
    expect(shared).toMatchObject({ includedRowCount: 1, ambiguousNameRowCount: 1 });
  });

  it("limits groups per direction without changing totals", () => {
    const result = aggregateIdahoOutsideSpending({
      registration: linked,
      registrations: [linked],
      maxGroups: 1,
      expenditureRows: [
        row(1, { amountApplied: 100 }),
        row(2, { amountApplied: 250, filerRegistrationGuid: "55555555-5555-4555-8555-555555555502", filerName: "Bigger PAC" }),
        row(3, { amountApplied: 200, stance: "Oppose", ...nonRegisteredFiler("Opposing Person") }),
      ],
    });
    expect(result.summary).toMatchObject({
      supportTotal: 350,
      opposeTotal: 200,
      groups: [
        { filerKey: "55555555-5555-4555-8555-555555555502", supportOppose: "support", amount: 250 },
        { filerKey: "name:OPPOSING PERSON", supportOppose: "oppose", amount: 200 },
      ],
    });
  });

  it("fails closed on rows and inputs it cannot trust", () => {
    const aggregate = (rows: ReturnType<typeof row>[], registrations = [linked]) => () =>
      aggregateIdahoOutsideSpending({ registration: linked, registrations, expenditureRows: rows });

    expect(aggregate([row(1, { stance: "N/A" })])).toThrow('unknown stance "N/A"');
    expect(aggregate([row(1, { transactionTypeCode: "TIE" })])).toThrow('unknown transaction type "TIE"');
    expect(aggregate([row(1, { filerRegistrationGuid: null })])).toThrow("without a filer registration guid");
    expect(aggregate([row(1, { transactionDate: "08/24/2026" })])).toThrow("unexpected date");
    expect(aggregate([row(1, { amountApplied: Number.NaN })])).toThrow("Invalid Idaho IE amount");
    expect(aggregate([row(1, nameOnly("Achilles, Todd", { filerName: "  " }))])).toThrow("blank filer name");
    expect(aggregate([], [otherPerson])).toThrow("is not in the candidate grid");
    expect(() =>
      aggregateIdahoOutsideSpending({ registration: registration({ registrationGuid: GUID_A, office: null }), registrations: [linked], expenditureRows: [] })
    ).toThrow("has no office");
    expect(() =>
      aggregateIdahoOutsideSpending({ registration: linked, registrations: [linked], expenditureRows: [], maxGroups: 0 })
    ).toThrow("Invalid Idaho outside spending maxGroups");
    // Rows of other races are never validated.
    expect(aggregate([row(1, { candidateMeasureFilerRegistrationGuid: GUID_D, stance: "N/A" })])).not.toThrow();
  });

  it("derives filer keys and target names", () => {
    expect(idahoOutsideFilerKey({ filerName: "Sample PAC", filerRegistrationGuid: PAC_GUID.toUpperCase() })).toBe(PAC_GUID);
    expect(idahoOutsideFilerKey({ filerName: "Make Liberty Win (FEC ID: C00731133", filerRegistrationGuid: null })).toBe("fec:C00731133");
    expect(idahoOutsideFilerKey({ filerName: " Brent F. Regan ", filerRegistrationGuid: null })).toBe("name:BRENT F REGAN");
    expect(() => idahoOutsideFilerKey({ filerName: "--", filerRegistrationGuid: null })).toThrow("blank filer name");
    expect(idahoIeTargetName("Corbus, Franklin 'Bud\"")).toBe("Corbus, Franklin (Bud)");
    expect(idahoIeTargetName("Chapman,  ada,")).toBe("Chapman, ada");
    expect(idahoIeTargetName("Blaylock , Camille")).toBe("Blaylock , Camille");
  });
});

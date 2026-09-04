import type {
  NewHampshireElectionCycle,
  NewHampshireFilingEntityRow,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";

export const CYCLE_2026_ID = 110;
export const CYCLE_2024_ID = 27;

export const ELECTION_CYCLES: NewHampshireElectionCycle[] = [
  { value: CYCLE_2026_ID, name: "2026 Election Cycle", dueDate: "2026-11-03T00:00:00" },
  { value: CYCLE_2024_ID, name: "2024 Election Cycle", dueDate: "2024-11-05T00:00:00" },
];

export function filingEntity(
  overrides: Partial<NewHampshireFilingEntityRow> = {}
): NewHampshireFilingEntityRow {
  return {
    registrationGuid: "00000000-0000-4000-8000-000000000001",
    filingEntityId: 50_450,
    filerName: "Sample Candidate Committee",
    candidateName: "Sample Candidate",
    firstName: "Sample",
    lastName: "Candidate",
    committeeName: "Sample Candidate Committee",
    filerTypeCode: "PAC",
    filerSubTypeCode: "PACCC",
    filerSubTypeName: "Candidate Committee",
    officeName: "State Senate",
    county: null,
    district: "1",
    electionCycleId: CYCLE_2026_ID,
    electionYear: 2026,
    electionCycle: "2026 Election Cycle",
    status: "Active",
    ...overrides,
  };
}

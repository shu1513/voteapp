import type {
  IdahoCandidateRegistrationRow,
  IdahoContributionRow,
  IdahoIndependentExpenditureRow,
} from "../../../src/pipeline/idahoFinance/idahoCfsClient.js";

export const GUID_A = "11111111-1111-4111-8111-111111111101";
export const GUID_B = "11111111-1111-4111-8111-111111111102";
export const GUID_C = "11111111-1111-4111-8111-111111111103";

export function registration(
  overrides: Partial<IdahoCandidateRegistrationRow> & { registrationGuid: string }
): IdahoCandidateRegistrationRow {
  return {
    entityGuid: "22222222-2222-4222-8222-222222222201",
    filerEntityId: 257,
    filerRegistrationId: 1698,
    filerName: "Achilles, Todd Baker",
    firstName: "Todd",
    middleName: "Baker",
    lastName: "Achilles",
    committeeName: "Todd Achilles for Idaho",
    office: "State Senator",
    districtType: "Legislative",
    district: "Legislative District 16",
    jurisdiction: "Idaho State",
    seatZone: null,
    party: "Democratic Party",
    partyCode: "DEM",
    electionYear: 2026,
    filingCycleId: 6,
    status: "Active",
    statusCode: "ACTV",
    totalRaised: 1500,
    totalSpent: 50,
    balanceOfFunds: 1450,
    isLegacyRecord: false,
    ...overrides,
  };
}

export function contribution(overrides: Partial<IdahoContributionRow> = {}): IdahoContributionRow {
  return {
    guid: "33333333-3333-4333-8333-333333333301",
    transactionId: 313559,
    transactionVersionId: 1,
    filerReportId: 9001,
    filerReportVersionId: 1,
    filerReportGuid: null,
    filerRegistrationGuid: GUID_A,
    filerEntityId: 257,
    filerName: "Todd Baker Achilles",
    transactionAmount: 1000,
    transactionDate: "03/03/2025",
    transactionTypeCode: "TCON",
    transactionSubTypeCode: "ITMY",
    sourceTypeCode: "TIND",
    sourceName: "Sample Donor",
    contributorCity: "Boise",
    contributorState: "ID",
    stateType: "INST",
    electionYear: 2026,
    electionTypeCode: "PRMELEC",
    reportName: "2025 Annual Report",
    timedReport: null,
    filedDate: "01/10/2026",
    ...overrides,
  };
}

export function independentExpenditure(
  overrides: Partial<IdahoIndependentExpenditureRow> = {}
): IdahoIndependentExpenditureRow {
  return {
    guid: "44444444-4444-4444-8444-444444444401",
    candidateMeasure: "Achilles, Todd",
    officeSought: "State Senator",
    amountApplied: 250,
    transactionDate: "2026-08-24T00:00:00",
    filerName: "Sample PAC",
    filerRegistrationGuid: "55555555-5555-4555-8555-555555555501",
    candidateMeasureFilerRegistrationGuid: GUID_A,
    reportName: "48 Hour Notice",
    timedReport: null,
    purpose: "Mailer",
    stance: "Support",
    sourceName: null,
    isNonRegisteredEntity: false,
    isCandidateNonRegisteredEntity: false,
    transactionTypeCode: "TIECOM",
    ...overrides,
  };
}

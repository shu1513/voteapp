export type HoustonFinanceSourceSystem = "legacy_webforms" | "ethics_efile";

export type HoustonFinanceReportIndexRecord = {
  sourceSystem: HoustonFinanceSourceSystem;
  reportId: string;
  filerId: string;
  filerName: string;
  filerType: string;
  reportType: string;
  receivedDate: string;
  filedAt: string;
  periodStart: string | null;
  periodEnd: string | null;
  officeDescription: string | null;
  campaignYear: number | null;
  pdfUrl: string | null;
  legacySelectionIndex?: number;
};

export type HoustonFinanceContribution = {
  contributionDate: string;
  contributorName: string;
  amount: number;
  occupation: string | null;
  sourceUrl: string;
};

export type HoustonFinanceParsedReport = {
  index: HoustonFinanceReportIndexRecord;
  candidateName: string;
  electionDate: string;
  officeSought: string;
  periodStart: string;
  periodEnd: string;
  directContributionTotal: number | null;
  contributions: HoustonFinanceContribution[];
};

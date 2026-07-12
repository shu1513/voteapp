import { normalizeTexasCandidateNameKeys } from "../texasFinance/texasCandidateCommitteeResolver.js";
import type { HoustonFinanceParsedReport } from "./houstonFinanceTypes.js";

function namesMatch(left: string, right: string): boolean {
  const rightKeys = normalizeTexasCandidateNameKeys(right);
  for (const key of normalizeTexasCandidateNameKeys(left)) if (rightKeys.has(key)) return true;
  return false;
}

export type HoustonCandidateCommitteeResolution =
  | { status: "matched"; committeeId: string; committeeName: string; sourceUrl: string | null; reports: HoustonFinanceParsedReport[] }
  | { status: "not_found" | "ambiguous"; reason: string };

export function resolveHoustonCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  reports: readonly HoustonFinanceParsedReport[];
}): HoustonCandidateCommitteeResolution {
  const reports = input.reports.filter((report) =>
    report.officeSought === "Mayor" &&
    report.electionDate.startsWith(`${input.electionYear}-`) &&
    namesMatch(input.candidateName, report.candidateName)
  );
  if (reports.length === 0) return { status: "not_found", reason: "no exact Houston Mayor report" };
  const names = new Set(reports.map((report) => [...normalizeTexasCandidateNameKeys(report.candidateName)][0]).filter(Boolean));
  if (names.size !== 1) return { status: "ambiguous", reason: "multiple candidate identities matched Houston reports" };
  const efileIds = new Set(
    reports.filter((report) => report.index.sourceSystem === "ethics_efile").map((report) => report.index.filerId.trim()).filter(Boolean)
  );
  if (efileIds.size > 1) return { status: "ambiguous", reason: "multiple current Houston filer IDs matched" };
  const normalizedName = [...normalizeTexasCandidateNameKeys(input.candidateName)].sort()[0] ?? input.candidateName.trim().toUpperCase();
  const efileId = [...efileIds][0];
  return {
    status: "matched",
    committeeId: efileId ? `efile:${efileId}` : `legacy:${normalizedName}:${input.electionYear}`,
    committeeName: reports[0]!.candidateName,
    sourceUrl: reports.find((report) => report.index.pdfUrl)?.index.pdfUrl ??
      "https://cohweb.houstontx.gov/CampaignFinanceWeb/CFRwebsiteSimpleSearch.aspx",
    reports,
  };
}

import type { Pool } from "pg";
import { classifyFinanceLabel, normalizeFinanceLabel, type FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  financeClassificationKey,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import { aggregateTexasOutsideGroupContributions } from "../texasFinance/texasOutsideGroupContributionAggregator.js";
import type { TexasTecCandidateRow, TexasTecContributionRow, TexasTecExpenditureRow, TexasTecPurposeRow } from "../texasFinance/texasTecCsvDatabaseReader.js";
import { normalizeTexasCandidateNameKeys } from "../texasFinance/texasCandidateCommitteeResolver.js";
import { selectEffectiveHoustonCandidateReports } from "./houstonCampaignFinancePdfParser.js";
import { aggregateHoustonDirectContributions } from "./houstonDirectContributionAggregator.js";
import type { HoustonFinanceParsedReport } from "./houstonFinanceTypes.js";
import {
  houstonFinanceOfficeTargetsEqual,
  parseStoredHoustonFinanceOfficeTarget,
} from "./houstonFinanceOfficeTargets.js";
import { aggregateHoustonTexasGpacOutsideSpending } from "./houstonTexasGpacOutsideSpendingAggregator.js";
import { replaceHoustonCandidateFinanceSnapshot, type HoustonFinanceOutsideGroupBreakdownInput } from "./houstonFinanceWriter.js";

type ConnectableDb = Pick<Pool, "query" | "connect">;
const DEFAULT_AI_MIN_AMOUNT = 25_000;

function required(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new Error(`${label} is required`);
  return trimmed;
}

function namesMatch(left: string, right: string): boolean {
  const leftKeys = normalizeTexasCandidateNameKeys(left);
  const rightKeys = normalizeTexasCandidateNameKeys(right);
  for (const value of leftKeys) if (rightKeys.has(value)) return true;
  return false;
}

export function isHoustonIndustryEligibleOrganizationName(value: string): boolean {
  const normalized = value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return false;
  return !(
    /\bPAC\b/.test(normalized) ||
    /\bPOLITICAL ACTION (COMMITTEE|FUND)\b/.test(normalized) ||
    /\bCAMPAIGN(S| COMMITTEE| FUND)?\b/.test(normalized) ||
    /\b(FRIENDS|CITIZENS) (OF|FOR)\b/.test(normalized) ||
    /\bFOR (STATE |US |U S )?(REPRESENTATIVE|SENATE|SENATOR|CONGRESS|GOVERNOR|MAYOR)\b/.test(normalized)
  );
}

function organizationKey(value: string): string {
  return value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}

export function isHoustonIndustryEligibleOrganization(
  value: string,
  excludedNames?: ReadonlySet<string>
): boolean {
  return isHoustonIndustryEligibleOrganizationName(value) && !excludedNames?.has(organizationKey(value));
}

export type HoustonCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
};

export function mergeHoustonOutsideIndustryBreakdowns(
  breakdowns: readonly HoustonFinanceOutsideGroupBreakdownInput[]
): HoustonFinanceOutsideGroupBreakdownInput[] {
  const merged = new Map<string, HoustonFinanceOutsideGroupBreakdownInput>();

  for (const breakdown of breakdowns) {
    const key = [
      breakdown.committeeId.trim().toUpperCase(),
      breakdown.supportOppose,
      breakdown.categoryName,
    ].join("\u0000");
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, { ...breakdown });
      continue;
    }

    const contributorCount =
      existing.contributorCount === null || existing.contributorCount === undefined
        ? breakdown.contributorCount ?? null
        : existing.contributorCount + (breakdown.contributorCount ?? 0);
    merged.set(key, {
      ...existing,
      amount: Math.round((existing.amount + breakdown.amount) * 100) / 100,
      contributorCount,
      sourceUrl: existing.sourceUrl ?? breakdown.sourceUrl ?? null,
    });
  }

  return [...merged.values()];
}

export async function syncHoustonCandidateFinance(input: {
  db: ConnectableDb;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeName?: string;
  district?: string | null;
  committeeId: string;
  committeeName: string;
  sourceUrl?: string | null;
  reports?: readonly HoustonFinanceParsedReport[];
  purposeRows?: readonly TexasTecPurposeRow[];
  candidateRows?: readonly TexasTecCandidateRow[];
  expenditureRows?: readonly TexasTecExpenditureRow[];
  outsideContributionRows?: readonly TexasTecContributionRow[];
  excludedIndustryOrganizationNames?: ReadonlySet<string>;
  tecSourceUrl?: string | null;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  dryRun?: boolean;
  now?: Date;
}): Promise<HoustonCandidateFinanceSyncResult> {
  const candidateId = required(input.candidateId, "candidate id");
  const electionId = required(input.electionId, "election id");
  const candidateName = required(input.candidateName, "candidate name");
  const committeeId = required(input.committeeId, "Houston filer id");
  const committeeName = required(input.committeeName, "Houston filer name");
  const officeTarget = parseStoredHoustonFinanceOfficeTarget({
    officeName: input.officeName ?? "Mayor",
    district: input.district ?? "Houston",
  });
  if (!officeTarget) throw new Error(`Unsupported Houston finance office target: ${input.officeName ?? ""} ${input.district ?? ""}`);
  if (!Number.isInteger(input.electionYear) || input.electionYear < 2014 || input.electionYear > 2100) {
    throw new Error(`Invalid Houston finance election year: ${input.electionYear}`);
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) throw new Error("Invalid Houston finance sync timestamp");
  const dryRun = input.dryRun === true;
  const minAmount = input.aiClassificationMinAmount ?? DEFAULT_AI_MIN_AMOUNT;
  if (!Number.isFinite(minAmount) || minAmount < 0) throw new Error("Invalid Houston finance AI threshold");

  const effectiveReports = input.reports
    ? selectEffectiveHoustonCandidateReports(
        input.reports.filter((report) =>
          report.electionDate.startsWith(String(input.electionYear)) &&
          houstonFinanceOfficeTargetsEqual(report.officeSought, officeTarget) &&
          namesMatch(candidateName, report.candidateName)
        )
      )
    : undefined;
  const direct = effectiveReports ? aggregateHoustonDirectContributions({ reports: effectiveReports }) : null;

  const hasOutsideSource =
    input.purposeRows !== undefined &&
    input.candidateRows !== undefined &&
    input.expenditureRows !== undefined &&
    input.outsideContributionRows !== undefined;
  const outside = hasOutsideSource
    ? aggregateHoustonTexasGpacOutsideSpending({
        candidateName,
        electionYear: input.electionYear,
        officeTarget,
        purposeRows: input.purposeRows!,
        candidateRows: input.candidateRows!,
        expenditureRows: input.expenditureRows!,
        sourceUrl: input.tecSourceUrl ?? null,
      })
    : null;
  const groups = outside?.summary?.groups;
  const baseBreakdowns = groups
    ? aggregateTexasOutsideGroupContributions({
        electionYear: input.electionYear,
        outsideGroups: groups,
        contributionRows: input.outsideContributionRows!.filter((row) =>
          isHoustonIndustryEligibleOrganization(row.contributorNameOrganization, input.excludedIndustryOrganizationNames)
        ),
        sourceUrl: input.tecSourceUrl ?? null,
        minIndustryAmount: 0,
      }).outsideGroupBreakdowns
    : hasOutsideSource
      ? []
      : undefined;

  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of baseBreakdowns ?? []) {
    if (breakdown.categoryType !== "donor") continue;
    mergeFinanceLabelClassification(classifications, classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType: "donor" }));
  }
  const donorBreakdowns = (baseBreakdowns ?? []).filter((breakdown) => breakdown.categoryType === "donor");
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: donorBreakdowns.map((breakdown) => ({ ...breakdown, categoryType: "donor" })),
    classifications,
    classifier: input.financeIndustryClassifier,
    minAmount,
    dryRun,
  });
  const industryBreakdowns = mergeHoustonOutsideIndustryBreakdowns(
    buildFinanceIndustryBreakdownsFromClassifications({
      directBreakdowns: [],
      outsideBreakdowns: donorBreakdowns.map((breakdown) => ({ ...breakdown, categoryType: "donor" })),
      classifications,
    }).outsideIndustryBreakdowns.map((breakdown) => ({ ...breakdown, categoryType: "industry" as const }))
  );
  const outsideBreakdowns: HoustonFinanceOutsideGroupBreakdownInput[] | undefined = baseBreakdowns === undefined
    ? undefined
    : [
        ...donorBreakdowns.map((breakdown) => ({ ...breakdown, categoryType: "donor" as const })),
        ...industryBreakdowns,
      ];

  if (!dryRun) {
    await replaceHoustonCandidateFinanceSnapshot({
      db: input.db,
      syncedAt: now,
      link: {
        candidateId,
        electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: [...normalizeTexasCandidateNameKeys(candidateName)].sort()[0] ?? candidateName.toUpperCase(),
        officeName: officeTarget.officeName,
        district: officeTarget.seat,
        committeeId,
        committeeName,
        linkSource: "houston_reports",
        sourceUrl: input.sourceUrl ?? effectiveReports?.[0]?.index.pdfUrl ?? null,
        lastVerifiedAt: now,
      },
      summary: {
        totalReceipts: direct?.totalReceipts,
        directContributionTotal: direct?.directContributionTotal,
        outsideSupportTotal: hasOutsideSource ? outside?.summary?.supportTotal ?? 0 : null,
        outsideOpposeTotal: hasOutsideSource ? outside?.summary?.opposeTotal ?? 0 : null,
        sourceUrl: input.sourceUrl ?? effectiveReports?.[0]?.index.pdfUrl ?? input.tecSourceUrl ?? null,
      },
      directBreakdowns: direct?.directBreakdowns,
      outsideGroups: groups?.map((group) => ({
        committeeId: group.committeeId,
        committeeName: group.committeeName,
        supportOppose: group.supportOppose,
        amount: group.amount,
        sourceUrl: group.sourceUrl,
      })) ?? (hasOutsideSource ? [] : undefined),
      outsideGroupBreakdowns: outsideBreakdowns,
      classifications: [...classifications.values()].filter((classification) =>
        Boolean(classification.normalizedLabel) &&
        (classification.industrySlug !== null || classification.classificationSource !== "unknown") &&
        financeClassificationKey(classification.labelType, classification.normalizedLabel).length > 1
      ),
    });
  }

  return {
    candidateId,
    electionId,
    electionYear: input.electionYear,
    dryRun,
    directContributionTotal: direct?.directContributionTotal ?? null,
    outsideSupportTotal: hasOutsideSource ? outside?.summary?.supportTotal ?? 0 : null,
    outsideOpposeTotal: hasOutsideSource ? outside?.summary?.opposeTotal ?? 0 : null,
    directBreakdownsWritten: direct?.directBreakdowns.length ?? 0,
    outsideGroupsWritten: groups?.length ?? 0,
    outsideGroupBreakdownsWritten: outsideBreakdowns?.length ?? 0,
  };
}

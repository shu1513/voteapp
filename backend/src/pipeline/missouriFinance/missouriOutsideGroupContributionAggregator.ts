import { normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import { normalizeMissouriMecText } from "./missouriFinanceEligibleOffices.js";
import { selectMissouriCanonicalReportRows, type MissouriReportSelectionDiagnostic } from "./missouriReportInventory.js";
import type { MissouriMecContributionRow, MissouriMecReportInventoryRow } from "./missouriMecParsers.js";
import type {
  MissouriFinanceOutsideGroupBreakdownInput,
  MissouriFinanceOutsideGroupInput,
} from "./missouriFinanceWriter.js";

export type MissouriOutsideSpenderContributionArtifacts = {
  inventory: readonly MissouriMecReportInventoryRow[];
  contributionRows: readonly MissouriMecContributionRow[];
  sourceUrl: string;
};

export type MissouriOutsideGroupReportDiagnostic = MissouriReportSelectionDiagnostic & { mecid: string };

export type MissouriOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: MissouriFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  individualContributionRowCount: number;
  outsideCycleContributionRowCount: number;
  nonPositiveContributionRowCount: number;
  ambiguousOrganizationRowCount: number;
  ambiguousOrganizationAmount: number;
  unrecognizedContributionKindRowCount: number;
  unrecognizedContributionKindAmount: number;
  reportDiagnostics: MissouriOutsideGroupReportDiagnostic[];
};

function clean(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ");
}

function contributionFingerprint(row: MissouriMecContributionRow): string {
  return [
    row.mecid,
    row.committeeName,
    row.contributorCommittee,
    row.contributorCompany,
    row.contributorLastName,
    row.contributorFirstName,
    row.employer,
    row.occupation,
    row.contributionDate,
    row.amountCents,
    row.contributionKind,
  ].map(String).join("\u0000");
}

function organizationName(row: MissouriMecContributionRow):
  | { status: "organization"; name: string }
  | { status: "individual" }
  | { status: "ambiguous" } {
  const committee = clean(row.contributorCommittee);
  const company = clean(row.contributorCompany);
  if (!committee && !company) return { status: "individual" };
  if (committee && company && normalizeMissouriMecText(committee) !== normalizeMissouriMecText(company)) {
    return { status: "ambiguous" };
  }
  return { status: "organization", name: committee || company };
}

export function aggregateMissouriOutsideGroupContributions(input: {
  outsideGroups: readonly MissouriFinanceOutsideGroupInput[];
  artifactsBySpender: ReadonlyMap<string, MissouriOutsideSpenderContributionArtifacts>;
  cycleStart: string;
  cycleEnd: string;
  sourceUrl?: string | null;
}): MissouriOutsideGroupContributionAggregationResult {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input.cycleStart) || !/^\d{4}-\d{2}-\d{2}$/.test(input.cycleEnd) || input.cycleStart > input.cycleEnd) {
    throw new Error(`Invalid Missouri outside-group contribution cycle: ${input.cycleStart}..${input.cycleEnd}`);
  }
  const groupsByMecid = new Map<string, MissouriFinanceOutsideGroupInput[]>();
  for (const group of input.outsideGroups) {
    const mecid = group.committeeId.trim().toUpperCase();
    const list = groupsByMecid.get(mecid) ?? [];
    list.push(group);
    groupsByMecid.set(mecid, list);
  }

  const donors = new Map<string, {
    committeeId: string;
    supportOppose: "support" | "oppose";
    displayName: string;
    normalizedName: string;
    amountCents: number;
    sourceUrl: string | null;
  }>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let individualContributionRowCount = 0;
  let outsideCycleContributionRowCount = 0;
  let nonPositiveContributionRowCount = 0;
  let ambiguousOrganizationRowCount = 0;
  let ambiguousOrganizationCents = 0;
  let unrecognizedContributionKindRowCount = 0;
  let unrecognizedContributionKindCents = 0;
  const reportDiagnostics: MissouriOutsideGroupReportDiagnostic[] = [];

  for (const [rawMecid, artifacts] of input.artifactsBySpender) {
    const mecid = rawMecid.trim().toUpperCase();
    const groups = groupsByMecid.get(mecid) ?? [];
    if (groups.length === 0) continue;
    for (const row of artifacts.contributionRows) {
      if (row.mecid !== mecid) throw new Error(`Missouri outside-spender export mismatch: expected ${mecid}, got ${row.mecid}`);
    }
    matchedContributionRowCount += artifacts.contributionRows.length;
    const cycleRows = artifacts.contributionRows.filter(
      (row) => row.contributionDate >= input.cycleStart && row.contributionDate <= input.cycleEnd
    );
    outsideCycleContributionRowCount += artifacts.contributionRows.length - cycleRows.length;
    const selection = selectMissouriCanonicalReportRows({
      inventory: artifacts.inventory,
      rows: cycleRows,
      reportName: (row) => row.report,
      amountCents: (row) => row.amountCents,
      safeFingerprint: contributionFingerprint,
    });
    reportDiagnostics.push(...selection.diagnostics.map((diagnostic) => ({ ...diagnostic, mecid })));
    for (const row of selection.rows) {
      const kind = normalizeMissouriMecText(row.contributionKind).replace(/[^A-Z]/g, "");
      if (kind !== "MONETARY" && kind !== "INKIND") {
        unrecognizedContributionKindRowCount += 1;
        unrecognizedContributionKindCents += row.amountCents;
        continue;
      }
      if (row.amountCents <= 0) {
        nonPositiveContributionRowCount += 1;
        continue;
      }
      const organization = organizationName(row);
      if (organization.status === "individual") {
        individualContributionRowCount += 1;
        continue;
      }
      if (organization.status === "ambiguous") {
        ambiguousOrganizationRowCount += 1;
        ambiguousOrganizationCents += row.amountCents;
        continue;
      }
      const normalizedName = normalizeFinanceLabel(organization.name, "donor");
      if (!normalizedName) {
        ambiguousOrganizationRowCount += 1;
        ambiguousOrganizationCents += row.amountCents;
        continue;
      }
      includedContributionRowCount += 1;
      for (const group of groups) {
        const key = `${mecid}\u0000${group.supportOppose}\u0000${normalizedName}`;
        const existing = donors.get(key);
        if (existing) existing.amountCents += row.amountCents;
        else donors.set(key, {
          committeeId: mecid,
          supportOppose: group.supportOppose,
          displayName: organization.name,
          normalizedName,
          amountCents: row.amountCents,
          sourceUrl: artifacts.sourceUrl || input.sourceUrl || null,
        });
      }
    }
  }

  const outsideGroupBreakdowns = [...donors.values()]
    .sort((left, right) => left.committeeId.localeCompare(right.committeeId) || left.supportOppose.localeCompare(right.supportOppose) || right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
    .map((donor) => ({
      committeeId: donor.committeeId,
      supportOppose: donor.supportOppose,
      categoryType: "donor" as const,
      categoryName: donor.displayName,
      amount: donor.amountCents / 100,
      contributorCount: 1,
      sourceUrl: donor.sourceUrl,
    }));
  return {
    outsideGroupBreakdowns,
    matchedContributionRowCount,
    includedContributionRowCount,
    individualContributionRowCount,
    outsideCycleContributionRowCount,
    nonPositiveContributionRowCount,
    ambiguousOrganizationRowCount,
    ambiguousOrganizationAmount: ambiguousOrganizationCents / 100,
    unrecognizedContributionKindRowCount,
    unrecognizedContributionKindAmount: unrecognizedContributionKindCents / 100,
    reportDiagnostics,
  };
}

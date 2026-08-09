// Direct-contribution formula for San Francisco controlled committees, as
// proven to the cent by the Phase 4 entry gate (plan-san-francisco-finance.md,
// verified live 2026-08-08 against all 15 committees of the 2024 Mayor and
// June 2026 D4 races; the probe re-validates it on every run):
//
//   itemized direct contributions =
//     non-memo Schedule A + non-memo Schedule C + unpaired non-memo F497P1
//
//   - Memo rows carry real amounts but are EXCLUDED from the official Form
//     460 line totals (proven: line 1 = non-memo A + F460ALine2 exactly on
//     memo-carrying committees).
//   - F496 plays no role for controlled committees (zero rows on all 15) and
//     no dollar thresholds apply — the dataset only contains what was filed.
//   - F497P1 late contributions are re-reported on the next Form 460
//     Schedule A under the SAME filer-assigned transaction_id, so Schedule A
//     is canonical and only UNPAIRED late rows are added. Two classes of
//     late rows are excluded rather than added: late LOANS (Schedule twin is
//     B1, same reused id) and PUBLIC-FINANCING disbursements reported as a
//     late contribution from the city (they exactly match a funds_approved
//     amount; counting them would double the public-funds figure).
//   - Refunds are negative Schedule A rows: they stay in the sums and are
//     excluded from size buckets by sign (no absolute-value bucketing — a
//     refund does not identify its original receipt's size).
//   - Occupation/employer breakdowns cover individual contributors only
//     (entity_code "IND"); occupations stay as disclosed, synonym merging is
//     out of scope. Industry classification happens at sync time, so the
//     category union carries "industry" but this aggregator never emits it.
import type { SanFranciscoItemizedTransactionRow } from "./sanFranciscoOpenDataClient.js";

// The fetch must include B1 (undated — needs includeUndatedTransactions) so
// the late-loan exclusion can fire, and both unitemized pseudo-row forms so
// reconciliation against the manifest funds figure stays possible.
export const SAN_FRANCISCO_DIRECT_CONTRIBUTION_FORM_TYPES = [
  "A",
  "C",
  "B1",
  "F497P1",
  "F460ALine2",
  "F460CLine2",
] as const;

export type SanFranciscoDirectBreakdown = {
  categoryType: "occupation" | "employer" | "industry" | "contribution_size";
  categoryName: string;
  amountCents: number;
  contributorCount: number;
};

export type SanFranciscoDirectContributionAggregate = {
  /** non-memo A + non-memo C + unpaired non-memo F497P1, cents. */
  itemizedCents: number;
  scheduleACents: number;
  scheduleCCents: number;
  unpairedLateCents: number;
  /** F460ALine2 pseudo-rows — per-filing unitemized (<$100) monetary totals. */
  unitemizedCents: number;
  /** F460CLine2 pseudo-rows — per-filing unitemized non-monetary totals. */
  unitemizedNonmonetaryCents: number;
  breakdowns: SanFranciscoDirectBreakdown[];
  diagnostics: {
    scheduleARows: number;
    scheduleCRows: number;
    refundRows: number;
    refundCents: number;
    memoRowsExcluded: number;
    memoCentsExcluded: number;
    lateRows: number;
    lateCents: number;
    latePairedById: number;
    latePairedByIdAmountMismatch: number;
    latePairedByAmountDate: number;
    lateLoanRowsExcluded: number;
    lateLoanCentsExcluded: number;
    latePublicFundsRowsExcluded: number;
    latePublicFundsCentsExcluded: number;
    unpairedLateRows: number;
  };
};

function sumCents(rows: readonly SanFranciscoItemizedTransactionRow[]): number {
  return rows.reduce((sum, row) => sum + row.calculatedAmountCents, 0);
}

function sizeBucket(cents: number): string {
  if (cents < 10_000) return "$1-$99";
  if (cents < 25_000) return "$100-$249";
  if (cents < 50_000) return "$250-$499";
  if (cents < 100_000) return "$500-$999";
  if (cents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

export function aggregateSanFranciscoDirectContributions(input: {
  /** Exactly the SAN_FRANCISCO_DIRECT_CONTRIBUTION_FORM_TYPES fetch result. */
  rows: readonly SanFranciscoItemizedTransactionRow[];
  /**
   * Individual public-financing approval amounts for this candidate, for
   * matching 497-reported disbursements ([] when the race has no program).
   */
  publicFundsApprovalCents: readonly number[];
  maxBreakdownsPerCategory?: number;
}): SanFranciscoDirectContributionAggregate {
  const limit = input.maxBreakdownsPerCategory ?? 20;
  if (!Number.isInteger(limit) || limit <= 0)
    throw new Error(`Invalid San Francisco direct breakdown limit: ${limit}`);
  // A form type outside the proven set means the caller fetched something
  // this formula was never proven against (e.g. F496) — fail loudly instead
  // of silently ignoring rows.
  const knownForms = new Set<string>(SAN_FRANCISCO_DIRECT_CONTRIBUTION_FORM_TYPES);
  for (const row of input.rows)
    if (!knownForms.has(row.formType))
      throw new Error(
        `San Francisco direct-contribution aggregation got unexpected form type ${row.formType}`,
      );
  const byForm = (formType: string) =>
    input.rows.filter((row) => row.formType === formType);
  const nonMemo = (rows: SanFranciscoItemizedTransactionRow[]) =>
    rows.filter((row) => row.memoCode !== true);
  const memoOnly = (rows: SanFranciscoItemizedTransactionRow[]) =>
    rows.filter((row) => row.memoCode === true);

  const scheduleA = nonMemo(byForm("A"));
  const scheduleC = nonMemo(byForm("C"));
  const scheduleB1 = byForm("B1");
  const lateRows = nonMemo(byForm("F497P1"));
  const unitemizedCents = sumCents(byForm("F460ALine2"));
  const unitemizedNonmonetaryCents = sumCents(byForm("F460CLine2"));
  const memoRows = [
    ...memoOnly(byForm("A")),
    ...memoOnly(byForm("C")),
    ...memoOnly(byForm("F497P1")),
  ];

  // Late-filing dedupe. transaction_id is filer-assigned and unique only
  // within a filing, so any Schedule A row sharing the id counts as the twin
  // and the late row is dropped; the amount only decides which counter it
  // lands in (latePairedById vs latePairedByIdAmountMismatch). A cross-filing
  // id collision would therefore drop a real late contribution, so the
  // mismatch counter is reported to keep that case visible.
  const scheduleAById = new Map<string, SanFranciscoItemizedTransactionRow[]>();
  for (const row of scheduleA) {
    if (row.transactionId === null) continue;
    const bucket = scheduleAById.get(row.transactionId) ?? [];
    bucket.push(row);
    scheduleAById.set(row.transactionId, bucket);
  }
  const loanIds = new Set(
    scheduleB1
      .map((row) => row.transactionId)
      .filter((id): id is string => id !== null),
  );
  const publicFundsApprovalSet = new Set(input.publicFundsApprovalCents);
  const unpairedLateRows: SanFranciscoItemizedTransactionRow[] = [];
  let latePairedById = 0;
  let latePairedByIdAmountMismatch = 0;
  let latePairedByAmountDate = 0;
  let lateLoanRowsExcluded = 0;
  let lateLoanCentsExcluded = 0;
  let latePublicFundsRowsExcluded = 0;
  let latePublicFundsCentsExcluded = 0;
  for (const lateRow of lateRows) {
    const idTwins =
      lateRow.transactionId === null
        ? []
        : (scheduleAById.get(lateRow.transactionId) ?? []);
    if (idTwins.length > 0) {
      if (
        idTwins.some(
          (twin) => twin.calculatedAmountCents === lateRow.calculatedAmountCents,
        )
      )
        latePairedById += 1;
      // Same id, different amount: almost certainly an amendment of the
      // same contribution — still reported on Schedule A, so still a
      // duplicate — but counted separately so drift is visible.
      else latePairedByIdAmountMismatch += 1;
      continue;
    }
    // Late-reported loan: the Schedule twin is B1, not A (same reused
    // transaction_id). Loans are excluded from direct contributions.
    if (lateRow.transactionId !== null && loanIds.has(lateRow.transactionId)) {
      lateLoanRowsExcluded += 1;
      lateLoanCentsExcluded += lateRow.calculatedAmountCents;
      continue;
    }
    // Public-financing disbursement reported as a late contribution from
    // the city ("City and Council of San Francisco" [sic] as disclosed);
    // already counted in the public-funds figure.
    if (
      (lateRow.contributorLastName ?? "")
        .toUpperCase()
        .includes("CITY AND COUN") &&
      publicFundsApprovalSet.has(lateRow.calculatedAmountCents)
    ) {
      latePublicFundsRowsExcluded += 1;
      latePublicFundsCentsExcluded += lateRow.calculatedAmountCents;
      continue;
    }
    // No-id fallback; names compare case-insensitively because one live
    // twin pair differs only by casing ("Lurie" vs "LURIE").
    const amountDateTwin = scheduleA.some(
      (row) =>
        row.calculatedAmountCents === lateRow.calculatedAmountCents &&
        row.transactionDate === lateRow.transactionDate &&
        (row.contributorLastName ?? "").toUpperCase() ===
          (lateRow.contributorLastName ?? "").toUpperCase(),
    );
    if (amountDateTwin) latePairedByAmountDate += 1;
    else unpairedLateRows.push(lateRow);
  }

  const includedRows = [...scheduleA, ...scheduleC, ...unpairedLateRows];
  const maps = new Map<
    "occupation" | "employer" | "contribution_size",
    Map<string, { name: string; cents: number; count: number }>
  >();
  const add = (
    type: "occupation" | "employer" | "contribution_size",
    rawName: string | null,
    cents: number,
  ): void => {
    const name = rawName?.trim().replace(/\s+/g, " ");
    if (!name) return;
    const values = maps.get(type) ?? new Map();
    const key = name.toUpperCase();
    const current = values.get(key) ?? { name, cents: 0, count: 0 };
    current.cents += cents;
    current.count += cents > 0 ? 1 : 0;
    values.set(key, current);
    maps.set(type, values);
  };
  for (const row of includedRows) {
    if (row.entityCode === "IND") {
      add("occupation", row.occupation, row.calculatedAmountCents);
      add("employer", row.employer, row.calculatedAmountCents);
    }
    if (row.calculatedAmountCents > 0)
      add(
        "contribution_size",
        sizeBucket(row.calculatedAmountCents),
        row.calculatedAmountCents,
      );
  }
  const breakdowns: SanFranciscoDirectBreakdown[] = [];
  for (const type of ["occupation", "employer", "contribution_size"] as const) {
    const categoryLimit =
      type === "contribution_size" ? Number.POSITIVE_INFINITY : limit;
    breakdowns.push(
      ...[...(maps.get(type)?.values() ?? [])]
        .filter((value) => value.cents > 0)
        .sort((a, b) => b.cents - a.cents || a.name.localeCompare(b.name))
        .slice(0, categoryLimit)
        .map((value) => ({
          categoryType: type,
          categoryName: value.name,
          amountCents: value.cents,
          contributorCount: value.count,
        })),
    );
  }

  const refundRows = scheduleA.filter((row) => row.calculatedAmountCents < 0);
  const scheduleACents = sumCents(scheduleA);
  const scheduleCCents = sumCents(scheduleC);
  const unpairedLateCents = sumCents(unpairedLateRows);
  return {
    itemizedCents: scheduleACents + scheduleCCents + unpairedLateCents,
    scheduleACents,
    scheduleCCents,
    unpairedLateCents,
    unitemizedCents,
    unitemizedNonmonetaryCents,
    breakdowns,
    diagnostics: {
      scheduleARows: scheduleA.length,
      scheduleCRows: scheduleC.length,
      refundRows: refundRows.length,
      refundCents: sumCents(refundRows),
      memoRowsExcluded: memoRows.length,
      memoCentsExcluded: sumCents(memoRows),
      lateRows: lateRows.length,
      lateCents: sumCents(lateRows),
      latePairedById,
      latePairedByIdAmountMismatch,
      latePairedByAmountDate,
      lateLoanRowsExcluded,
      lateLoanCentsExcluded,
      latePublicFundsRowsExcluded,
      latePublicFundsCentsExcluded,
      unpairedLateRows: unpairedLateRows.length,
    },
  };
}

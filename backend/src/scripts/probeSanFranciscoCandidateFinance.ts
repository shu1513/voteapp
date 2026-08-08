// Phase 0 probe (plan-san-francisco-finance.md): validates the SFEC
// dashboard-manifest path (0A) against the raw DataSF reconstruction (0B)
// for real contests, and quantifies every residual difference. Kept as a
// live smoke test after Phase 0: any candidate-level failure (fetch,
// schema, parse) exits non-zero so automation notices; reference-value
// mismatches are reported in the JSON but stay warning-only, because the
// dashboards legitimately move while committees keep filing.
import {
  defaultSanFranciscoDashboardManifestClientOptions,
  getSanFranciscoContestManifest,
  type SanFranciscoContestManifest,
} from "../pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";
import {
  defaultSanFranciscoOpenDataClientOptions,
  getSanFranciscoCandidateTargetedSpending,
  getSanFranciscoCommitteeItemizedTransactions,
  getSanFranciscoCommitteeSummaryRows,
  getSanFranciscoPublicFundsApproved,
  type SanFranciscoItemizedTransactionRow,
  type SanFranciscoOpenDataClientOptions,
  type SanFranciscoSummaryRow,
} from "../pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";

type ContestTarget = { electionDate: string; contestCode: string };

// Reference values captured from the rendered dashboards on 2026-08-06.
// Reported as booleans, not hard failures: a mismatch can mean genuine new
// filings (dashboards keep moving while committees file), so a human reads
// the diff — but the two canonical races below are the Phase 0 yardstick.
const REFERENCE_CENTS: Record<
  string,
  { funds?: number; expenses?: number; outsideSupport?: number; outsideOppose?: number }
> = {
  "2024-11-05/myr/DANIEL LURIE": {
    funds: 1091764252,
    expenses: 1081611285,
  },
  "2026-06-02/bos04/ALAN WONG": {
    funds: 41237100,
    expenses: 41072779,
    outsideSupport: 74440101,
    outsideOppose: 2475392,
  },
};

const DEFAULT_TARGETS: ContestTarget[] = [
  { electionDate: "2024-11-05", contestCode: "myr" },
  { electionDate: "2026-06-02", contestCode: "bos04" },
];

function parseTargets(argv: string[]): ContestTarget[] {
  const electionArg = argv.find((arg) => arg.startsWith("--election="));
  const contestArg = argv.find((arg) => arg.startsWith("--contest="));
  if (!electionArg && !contestArg) return DEFAULT_TARGETS;
  if (!electionArg || !contestArg)
    throw new Error("Pass both --election=YYYY-MM-DD and --contest=<code>");
  return [
    {
      electionDate: electionArg.slice("--election=".length),
      contestCode: contestArg.slice("--contest=".length),
    },
  ];
}

function centsToMoney(cents: number | null): string | null {
  return cents === null ? null : (cents / 100).toFixed(2);
}

// One key derivation for every name comparison in this probe, so map
// construction and lookup can never disagree.
function nameKey(name: string): string {
  return name.trim().replace(/\s+/g, " ").toUpperCase();
}

// Manifest names are "FIRST LAST"; split into the DataSF name-field filters.
// First AND last are both sent to the transactions query — last name alone
// mixes candidates who share a surname (verified live: David Lee's Schedule D
// picked up Barbara Lee's $2,600). The naive first/last token split can still
// miss compound names, so the probe reports the filter it used.
function splitCandidateName(candidateName: string): {
  firstName: string;
  lastName: string;
} {
  const parts = candidateName.trim().split(/\s+/);
  return {
    firstName: parts[0] ?? candidateName,
    lastName: parts[parts.length - 1] ?? candidateName,
  };
}

// Public-funds rows disclose "Last, First"; the manifest uses "FIRST LAST".
function normalizeCommaName(name: string): string {
  const [last, first] = name.split(",", 2);
  return nameKey(`${(first ?? "").trim()} ${(last ?? "").trim()}`);
}

// The public-financing program covers Mayor and Supervisor races only, and
// its rows carry a district ("Mayor" or a bare district number). Scope the
// lookup to this contest's district so a same-surname candidate in another
// contest on the same ballot can never be summed in.
function publicFundsDistrictFor(contestCode: string): string | null {
  if (contestCode === "myr") return "Mayor";
  const supervisorMatch = /^bos(\d{2})$/.exec(contestCode);
  return supervisorMatch ? String(Number(supervisorMatch[1])) : null;
}

function shiftDate(isoDate: string, days: number): string {
  const time = Date.parse(`${isoDate}T00:00:00.000Z`);
  if (Number.isNaN(time)) throw new Error(`Invalid date: ${isoDate}`);
  return new Date(time + days * 86_400_000).toISOString().slice(0, 10);
}

function sumCents(rows: SanFranciscoItemizedTransactionRow[]): number {
  return rows.reduce((sum, row) => sum + row.calculatedAmountCents, 0);
}

// Phase 4 entry gate: prove the itemized contributor formula against the
// committee's own Form 460 summary lines, to the cent. Everything below was
// first established by live exploration (2026-08-08) and is re-derived here
// on every run:
//   - form_type "A"/"C" rows are the itemized Schedule A/C contributions;
//     memo rows carry real amounts but are EXCLUDED from the official line
//     totals (proven: line 1 = non-memo A + F460ALine2 on committees that
//     do file memo rows).
//   - form_type "F460ALine2" pseudo-rows are the per-filing unitemized
//     (<$100) totals, dated at period end.
//   - Form 460 line 1 = non-memo Schedule A + F460ALine2 pseudo-rows, and
//     line 5 = line 1 + line 2 + line 4, where line 4 = non-memo Schedule C
//     + F460CLine2 pseudo-rows. Schedule B loan principal is NOT in line 5
//     (proven: a committee with $200,000 of B1 loans shows line 2 = $29.38),
//     so the dashboard funds figure never includes loans.
//   - F497P1 late contributions are re-reported on the next Form 460
//     Schedule A under the SAME filer-assigned transaction_id (proven on
//     the 2024 Lurie committee: all 13 ids reappear on Schedule A with
//     identical amounts — one re-reported with different name casing,
//     "Lurie" vs "LURIE", which is why the no-id fallback below compares
//     names case-insensitively). The formula therefore takes A as
//     canonical and adds only UNPAIRED F497P1 rows.
//   - Two classes of F497P1 rows are late-reported money that is NOT a
//     direct contribution and must be excluded rather than added: late
//     LOANS, whose Schedule twin is B1 instead of A (same reused
//     transaction_id, proven live), and PUBLIC-FINANCING disbursements,
//     which one filer reported as a $60,000 F497P1 row from "City and
//     Council of San Francisco" [sic] that exactly matches a funds_approved
//     row — counting it would double the public-funds figure.
//   - F496 plays no role in a controlled committee's direct contributions
//     (zero F496 rows on both canonical committees).
//   - Refunds are negative Schedule A rows; they stay in the sum.
async function proveContributorFormula(input: {
  fppcId: string;
  summaryRows: SanFranciscoSummaryRow[];
  publicFundsCents: number;
  /** Individual approval amounts, for matching 497-reported disbursements. */
  publicFundsApprovalCents: number[];
  manifestFundsCents: number;
  sodaOptions: SanFranciscoOpenDataClientOptions;
}): Promise<unknown> {
  // Full-history window derived from the committee's own filing periods:
  // the summary lines cover every filing, so the identity checks must too.
  // SF committees are per-election, so full history stays one contest.
  const periodDates = input.summaryRows
    .flatMap((row) => [row.periodStart, row.periodEnd])
    .filter((value): value is string => value !== null)
    .map((value) => value.slice(0, 10))
    .sort();
  if (periodDates.length === 0)
    return { skipped: "committee has no filing periods" };
  const rows = await getSanFranciscoCommitteeItemizedTransactions(
    {
      fppcId: input.fppcId,
      formTypes: ["A", "C", "B1", "F497P1", "F460ALine2", "F460CLine2"],
      transactionDateFrom: shiftDate(periodDates[0]!, -31),
      transactionDateTo: shiftDate(periodDates[periodDates.length - 1]!, 31),
      // Schedule B1 loan rows carry no transaction_date; without this the
      // window would silently drop the whole loan schedule and the
      // late-loan exclusion below would never fire.
      includeUndatedTransactions: true,
    },
    input.sodaOptions,
  );
  const byForm = (formType: string) =>
    rows.filter((row) => row.formType === formType);
  const nonMemo = (formRows: SanFranciscoItemizedTransactionRow[]) =>
    formRows.filter((row) => row.memoCode !== true);
  const memoOnly = (formRows: SanFranciscoItemizedTransactionRow[]) =>
    formRows.filter((row) => row.memoCode === true);

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
  // within a filing, so an id hit is confirmed by amount before it counts.
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
  let pairedById = 0;
  let pairedByIdAmountMismatch = 0;
  let pairedByAmountDate = 0;
  let loanRowsExcluded = 0;
  let loanCentsExcluded = 0;
  let publicFundsRowsExcluded = 0;
  let publicFundsCentsExcluded = 0;
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
        pairedById += 1;
      // Same id, different amount: almost certainly an amendment of the
      // same contribution — still reported on Schedule A, so still a
      // duplicate — but counted separately so drift is visible.
      else pairedByIdAmountMismatch += 1;
      continue;
    }
    // Late-reported loan: the Schedule twin is B1, not A (same reused
    // transaction_id). Loans are excluded from direct contributions.
    if (lateRow.transactionId !== null && loanIds.has(lateRow.transactionId)) {
      loanRowsExcluded += 1;
      loanCentsExcluded += lateRow.calculatedAmountCents;
      continue;
    }
    // Public-financing disbursement reported as a late contribution from
    // the city; already counted in the public-funds figure.
    if (
      (lateRow.contributorLastName ?? "")
        .toUpperCase()
        .includes("CITY AND COUN") &&
      publicFundsApprovalSet.has(lateRow.calculatedAmountCents)
    ) {
      publicFundsRowsExcluded += 1;
      publicFundsCentsExcluded += lateRow.calculatedAmountCents;
      continue;
    }
    const amountDateTwin = scheduleA.some(
      (row) =>
        row.calculatedAmountCents === lateRow.calculatedAmountCents &&
        row.transactionDate === lateRow.transactionDate &&
        (row.contributorLastName ?? "").toUpperCase() ===
          (lateRow.contributorLastName ?? "").toUpperCase(),
    );
    if (amountDateTwin) pairedByAmountDate += 1;
    else unpairedLateRows.push(lateRow);
  }

  const refundRows = scheduleA.filter((row) => row.calculatedAmountCents < 0);
  const entityCentsByCode = new Map<string, { rows: number; cents: number }>();
  for (const row of scheduleA) {
    const code = row.entityCode ?? "(none)";
    const bucket = entityCentsByCode.get(code) ?? { rows: 0, cents: 0 };
    bucket.rows += 1;
    bucket.cents += row.calculatedAmountCents;
    entityCentsByCode.set(code, bucket);
  }
  const entityBreakdown = Object.fromEntries(
    [...entityCentsByCode].map(([code, bucket]) => [
      code,
      { rows: bucket.rows, amount: centsToMoney(bucket.cents) },
    ]),
  );
  const individualRows = scheduleA.filter((row) => row.entityCode === "IND");

  const scheduleACents = sumCents(scheduleA);
  const scheduleCCents = sumCents(scheduleC);
  const unpairedLateCents = sumCents(unpairedLateRows);
  const itemizedCents = scheduleACents + scheduleCCents + unpairedLateCents;
  const line1Cents = input.summaryRows.reduce(
    (sum, row) => sum + (row.monetaryContributionsCents ?? 0),
    0,
  );
  const line2Cents = input.summaryRows.reduce(
    (sum, row) => sum + (row.line2Cents ?? 0),
    0,
  );
  const line5Cents = input.summaryRows.reduce(
    (sum, row) => sum + (row.contributionsCents ?? 0),
    0,
  );
  // line 5 covers only 460-reported money, so unpaired late rows are
  // deliberately absent from this identity.
  const line5ReconstructedCents =
    scheduleACents +
    unitemizedCents +
    line2Cents +
    scheduleCCents +
    unitemizedNonmonetaryCents;
  return {
    transaction_window: {
      from: shiftDate(periodDates[0]!, -31),
      to: shiftDate(periodDates[periodDates.length - 1]!, 31),
    },
    schedule_a: {
      rows: scheduleA.length,
      amount: centsToMoney(scheduleACents),
      refund_rows: refundRows.length,
      refund_amount: centsToMoney(sumCents(refundRows)),
    },
    schedule_c: { rows: scheduleC.length, amount: centsToMoney(scheduleCCents) },
    unitemized_line_amount: centsToMoney(unitemizedCents),
    unitemized_nonmonetary_line_amount: centsToMoney(
      unitemizedNonmonetaryCents,
    ),
    memo_rows_excluded: {
      rows: memoRows.length,
      amount: centsToMoney(sumCents(memoRows)),
    },
    late_f497p1: {
      rows: lateRows.length,
      amount: centsToMoney(sumCents(lateRows)),
      paired_with_schedule_a_by_id: pairedById,
      paired_by_id_amount_mismatch: pairedByIdAmountMismatch,
      paired_by_amount_date: pairedByAmountDate,
      loan_rows_excluded: loanRowsExcluded,
      loan_amount_excluded: centsToMoney(loanCentsExcluded),
      public_funds_rows_excluded: publicFundsRowsExcluded,
      public_funds_amount_excluded: centsToMoney(publicFundsCentsExcluded),
      unpaired_rows: unpairedLateRows.length,
      unpaired_amount: centsToMoney(unpairedLateCents),
    },
    itemized_total: centsToMoney(itemizedCents),
    entity_codes: entityBreakdown,
    individual_disclosure_coverage: {
      individual_rows: individualRows.length,
      with_occupation: individualRows.filter((row) => row.occupation !== null)
        .length,
      with_employer: individualRows.filter((row) => row.employer !== null)
        .length,
    },
    monetary_line1_identity: {
      matches: line1Cents === scheduleACents + unitemizedCents,
      line_1_total: centsToMoney(line1Cents),
      difference: centsToMoney(line1Cents - scheduleACents - unitemizedCents),
    },
    contributions_line5_identity: {
      // line 5 = line 1 + line 2 + line 4; a nonzero difference here means
      // the composition identity itself broke.
      matches: line5Cents === line5ReconstructedCents,
      line_5_total: centsToMoney(line5Cents),
      line_2_total: centsToMoney(line2Cents),
      difference: centsToMoney(line5Cents - line5ReconstructedCents),
    },
    manifest_reconciliation: {
      // manifest funds = line-5 prefix at the dashboard cutoff + public
      // funds, so the residual below is post-cutoff timing minus the
      // 497P1-only money the 460 lines never see; every fully closed race
      // reconciles to exactly 0.00.
      manifest_funds: centsToMoney(input.manifestFundsCents),
      formula_total: centsToMoney(
        itemizedCents +
          unitemizedCents +
          unitemizedNonmonetaryCents +
          line2Cents +
          input.publicFundsCents,
      ),
      residual: centsToMoney(
        input.manifestFundsCents -
          itemizedCents -
          unitemizedCents -
          unitemizedNonmonetaryCents -
          line2Cents -
          input.publicFundsCents,
      ),
    },
  };
}

async function probeContest(
  target: ContestTarget,
): Promise<{ report: unknown; errorCount: number }> {
  const manifest: SanFranciscoContestManifest =
    await getSanFranciscoContestManifest(target, {
      ...defaultSanFranciscoDashboardManifestClientOptions(),
    });
  const sodaOptions = defaultSanFranciscoOpenDataClientOptions();
  // Public financing explains the gap between raw Form 460 contribution
  // sums and the dashboard "funds" figure (verified to the cent for the
  // 2024 Mayor and June 2026 D4 races): funds = line-5 sum + public funds.
  const publicFundsDistrict = publicFundsDistrictFor(target.contestCode);
  const publicFundsRows = publicFundsDistrict
    ? await getSanFranciscoPublicFundsApproved(
        { electionDate: target.electionDate },
        sodaOptions,
      )
    : [];
  const publicFundsCentsByCandidate = new Map<string, number>();
  const publicFundsApprovalsByCandidate = new Map<string, number[]>();
  for (const row of publicFundsRows) {
    if (row.district !== publicFundsDistrict) continue;
    const key = normalizeCommaName(row.candidateName);
    publicFundsCentsByCandidate.set(
      key,
      (publicFundsCentsByCandidate.get(key) ?? 0) + row.fundsApprovedCents,
    );
    const approvals = publicFundsApprovalsByCandidate.get(key) ?? [];
    approvals.push(row.fundsApprovedCents);
    publicFundsApprovalsByCandidate.set(key, approvals);
  }
  let errorCount = 0;
  const candidates = [];
  for (const candidate of manifest.candidates) {
    const referenceKey = `${target.electionDate}/${target.contestCode}/${candidate.candidateName}`;
    const reference = REFERENCE_CENTS[referenceKey];
    try {
      const summaryRows = await getSanFranciscoCommitteeSummaryRows(
        { fppcId: candidate.fppcId },
        sodaOptions,
      );
      // 0B funds oracle: the dashboard's "funds" figure equals public funds
      // approved plus the sum of Form 460 line-5 periods up to some cutoff
      // filing. Walk period prefixes and report where — or whether — the
      // manifest total is reproduced exactly.
      const publicFundsCents =
        publicFundsCentsByCandidate.get(nameKey(candidate.candidateName)) ?? 0;
      let runningContributions = publicFundsCents;
      let runningExpenditures = 0;
      let matchedCutoff: {
        periodEnd: string | null;
        expendituresCents: number;
      } | null = null;
      for (const row of summaryRows) {
        runningContributions += row.contributionsCents ?? 0;
        runningExpenditures += row.expendituresCents ?? 0;
        if (runningContributions === candidate.fundsCents)
          matchedCutoff = {
            periodEnd: row.periodEnd,
            expendituresCents: runningExpenditures,
          };
      }
      const manifestOutside = manifest.outsideRelations.filter(
        (relation) => relation.candidateName === candidate.candidateName,
      );
      const manifestSupportCents = manifestOutside
        .filter((relation) => relation.position === "support")
        .reduce((sum, relation) => sum + relation.amountCents, 0);
      const manifestOpposeCents = manifestOutside
        .filter((relation) => relation.position === "oppose")
        .reduce((sum, relation) => sum + relation.amountCents, 0);
      const contributorFormula = await proveContributorFormula({
        fppcId: candidate.fppcId,
        summaryRows,
        publicFundsCents,
        publicFundsApprovalCents:
          publicFundsApprovalsByCandidate.get(
            nameKey(candidate.candidateName),
          ) ?? [],
        manifestFundsCents: candidate.fundsCents,
        sodaOptions,
      });
      const nameFilter = splitCandidateName(candidate.candidateName);
      const targeted = await getSanFranciscoCandidateTargetedSpending(
        {
          candidateLastName: nameFilter.lastName,
          candidateFirstName: nameFilter.firstName,
          // F496 rows carry no election_date; bound the contest by a
          // two-year cycle window ending shortly after election day.
          transactionDateFrom: shiftDate(target.electionDate, -730),
          transactionDateTo: shiftDate(target.electionDate, 30),
        },
        sodaOptions,
      );
      const taggedF496SupportCents = targeted
        .filter((row) => row.formType === "F496" && row.supportOpposeCode === "S")
        .reduce((sum, row) => sum + row.amountCents, 0);
      const taggedF496OpposeCents = targeted
        .filter((row) => row.formType === "F496" && row.supportOpposeCode === "O")
        .reduce((sum, row) => sum + row.amountCents, 0);
      const taggedScheduleDCents = targeted
        .filter((row) => row.formType === "D")
        .reduce((sum, row) => sum + row.amountCents, 0);
      // Per-relation view: how much of each manifest relation is visible in
      // candidate-tagged F496 rows. Differences quantify the money that only
      // flows through the spender's own untagged filings (primarily-formed
      // committees) — the reason the manifest is the primary source.
      const relations = manifestOutside.map((relation) => {
        const taggedCents = targeted
          .filter(
            (row) =>
              row.formType === "F496" &&
              relation.spenderFppcId !== null &&
              row.spenderFppcId === relation.spenderFppcId,
          )
          .reduce((sum, row) => sum + row.amountCents, 0);
        return {
          spender: relation.spenderName,
          spender_fppc_id: relation.spenderFppcId,
          position: relation.position,
          manifest_amount: centsToMoney(relation.amountCents),
          candidate_tagged_f496_amount: centsToMoney(taggedCents),
        };
      });
      candidates.push({
        candidate: candidate.candidateName,
        fppc_committee_id: candidate.fppcId,
        committee: candidate.committeeName,
        manifest_funds: centsToMoney(candidate.fundsCents),
        manifest_expenses: centsToMoney(candidate.expensesCents),
        public_funds_approved: centsToMoney(publicFundsCents),
        summary_filings: summaryRows.length,
        raw_contributions_all_periods: centsToMoney(runningContributions),
        raw_expenditures_all_periods: centsToMoney(runningExpenditures),
        funds_prefix_match: matchedCutoff
          ? {
              matched: true,
              cutoff_period_end: matchedCutoff.periodEnd,
              raw_expenditures_at_cutoff: centsToMoney(
                matchedCutoff.expendituresCents,
              ),
              expenses_difference_at_cutoff: centsToMoney(
                matchedCutoff.expendituresCents - candidate.expensesCents,
              ),
            }
          : {
              matched: false,
              all_periods_difference: centsToMoney(
                runningContributions - candidate.fundsCents,
              ),
            },
        contributor_formula: contributorFormula,
        outside: {
          manifest_support: centsToMoney(manifestSupportCents),
          manifest_oppose: centsToMoney(manifestOpposeCents),
          targeted_name_filter: nameFilter,
          candidate_tagged_f496_support: centsToMoney(taggedF496SupportCents),
          candidate_tagged_f496_oppose: centsToMoney(taggedF496OpposeCents),
          candidate_tagged_schedule_d: centsToMoney(taggedScheduleDCents),
          relations,
        },
        ...(reference
          ? {
              reference_check: {
                funds_matches_reference:
                  reference.funds === undefined ||
                  reference.funds === candidate.fundsCents,
                expenses_matches_reference:
                  reference.expenses === undefined ||
                  reference.expenses === candidate.expensesCents,
                outside_support_matches_reference:
                  reference.outsideSupport === undefined ||
                  reference.outsideSupport === manifestSupportCents,
                outside_oppose_matches_reference:
                  reference.outsideOppose === undefined ||
                  reference.outsideOppose === manifestOpposeCents,
              },
            }
          : {}),
      });
    } catch (error) {
      errorCount += 1;
      candidates.push({
        candidate: candidate.candidateName,
        fppc_committee_id: candidate.fppcId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return {
    errorCount,
    report: {
      election_date: target.electionDate,
      contest_code: target.contestCode,
      title: manifest.title,
      source_url: manifest.sourceUrl,
      candidate_count: manifest.candidates.length,
      outside_relation_count: manifest.outsideRelations.length,
      candidates,
    },
  };
}

const targets = parseTargets(process.argv);
const contests = [];
let candidateErrorCount = 0;
for (const target of targets) {
  const { report, errorCount } = await probeContest(target);
  contests.push(report);
  candidateErrorCount += errorCount;
}
console.log(
  JSON.stringify({ candidate_error_count: candidateErrorCount, contests }, null, 2),
);
if (candidateErrorCount > 0) process.exitCode = 1;

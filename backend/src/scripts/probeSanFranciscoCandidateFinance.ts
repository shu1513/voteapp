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
  type SanFranciscoOpenDataClientOptions,
  type SanFranciscoSummaryRow,
} from "../pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";
import {
  aggregateSanFranciscoDirectContributions,
  SAN_FRANCISCO_DIRECT_CONTRIBUTION_FORM_TYPES,
} from "../pipeline/sanFranciscoFinance/sanFranciscoDirectContributionAggregator.js";

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

// Phase 4 entry gate: prove the itemized contributor formula against the
// committee's own Form 460 summary lines, to the cent, on every run. The
// formula itself (composition, memo exclusion, F497P1 dedupe and its two
// exclusion classes, refund handling) lives in — and is documented on —
// sanFranciscoDirectContributionAggregator.ts; this probe calls it and
// checks the aggregate against the line identities and the manifest, so the
// production formula and the gate can never drift apart.
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
      formTypes: [...SAN_FRANCISCO_DIRECT_CONTRIBUTION_FORM_TYPES],
      transactionDateFrom: shiftDate(periodDates[0]!, -31),
      transactionDateTo: shiftDate(periodDates[periodDates.length - 1]!, 31),
      // Schedule B1 loan rows carry no transaction_date; without this the
      // window would silently drop the whole loan schedule and the
      // aggregator's late-loan exclusion would never fire.
      includeUndatedTransactions: true,
    },
    input.sodaOptions,
  );
  const aggregate = aggregateSanFranciscoDirectContributions({
    rows,
    publicFundsApprovalCents: input.publicFundsApprovalCents,
  });
  const {
    scheduleACents,
    scheduleCCents,
    unitemizedCents,
    unitemizedNonmonetaryCents,
    itemizedCents,
    unpairedLateCents,
    diagnostics,
  } = aggregate;

  // Disclosure census, not formula: entity-code composition and
  // occupation/employer coverage over the same non-memo Schedule A rows the
  // aggregator counts.
  const scheduleA = rows.filter(
    (row) => row.formType === "A" && row.memoCode !== true,
  );
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
      rows: diagnostics.scheduleARows,
      amount: centsToMoney(scheduleACents),
      refund_rows: diagnostics.refundRows,
      refund_amount: centsToMoney(diagnostics.refundCents),
    },
    schedule_c: {
      rows: diagnostics.scheduleCRows,
      amount: centsToMoney(scheduleCCents),
    },
    unitemized_line_amount: centsToMoney(unitemizedCents),
    unitemized_nonmonetary_line_amount: centsToMoney(
      unitemizedNonmonetaryCents,
    ),
    memo_rows_excluded: {
      rows: diagnostics.memoRowsExcluded,
      amount: centsToMoney(diagnostics.memoCentsExcluded),
    },
    late_f497p1: {
      rows: diagnostics.lateRows,
      amount: centsToMoney(diagnostics.lateCents),
      paired_with_schedule_a_by_id: diagnostics.latePairedById,
      paired_by_id_amount_mismatch: diagnostics.latePairedByIdAmountMismatch,
      paired_by_amount_date: diagnostics.latePairedByAmountDate,
      loan_rows_excluded: diagnostics.lateLoanRowsExcluded,
      loan_amount_excluded: centsToMoney(diagnostics.lateLoanCentsExcluded),
      public_funds_rows_excluded: diagnostics.latePublicFundsRowsExcluded,
      public_funds_amount_excluded: centsToMoney(
        diagnostics.latePublicFundsCentsExcluded,
      ),
      unpaired_rows: diagnostics.unpairedLateRows,
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

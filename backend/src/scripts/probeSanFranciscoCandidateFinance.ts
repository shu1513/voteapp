// Phase 0 probe (plan-san-francisco-finance.md): validates the SFEC
// dashboard-manifest path (0A) against the raw DataSF reconstruction (0B)
// for real contests, and quantifies every residual difference. Kept as a
// live smoke test after Phase 0 — if SFEC changes its methodology or
// manifest schema, this fails loudly instead of letting numbers drift.
import {
  defaultSanFranciscoDashboardManifestClientOptions,
  getSanFranciscoContestManifest,
  type SanFranciscoContestManifest,
} from "../pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";
import {
  defaultSanFranciscoOpenDataClientOptions,
  getSanFranciscoCandidateTargetedSpending,
  getSanFranciscoCommitteeSummaryRows,
  getSanFranciscoPublicFundsApproved,
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

function lastNameOf(candidateName: string): string {
  const parts = candidateName.trim().split(/\s+/);
  return parts[parts.length - 1] ?? candidateName;
}

// Public-funds rows disclose "Last, First"; the manifest uses "FIRST LAST".
function normalizeCommaName(name: string): string {
  const [last, first] = name.split(",", 2);
  return `${(first ?? "").trim()} ${(last ?? "").trim()}`.trim().toUpperCase();
}

function shiftDate(isoDate: string, days: number): string {
  const time = Date.parse(`${isoDate}T00:00:00.000Z`);
  if (Number.isNaN(time)) throw new Error(`Invalid date: ${isoDate}`);
  return new Date(time + days * 86_400_000).toISOString().slice(0, 10);
}

async function probeContest(target: ContestTarget): Promise<unknown> {
  const manifest: SanFranciscoContestManifest =
    await getSanFranciscoContestManifest(target, {
      ...defaultSanFranciscoDashboardManifestClientOptions(),
    });
  const sodaOptions = defaultSanFranciscoOpenDataClientOptions();
  // Public financing explains the gap between raw Form 460 contribution
  // sums and the dashboard "funds" figure (verified to the cent for the
  // 2024 Mayor and June 2026 D4 races): funds = line-5 sum + public funds.
  const publicFundsRows = await getSanFranciscoPublicFundsApproved(
    { electionDate: target.electionDate },
    sodaOptions,
  );
  const publicFundsCentsByCandidate = new Map<string, number>();
  for (const row of publicFundsRows) {
    const key = normalizeCommaName(row.candidateName);
    publicFundsCentsByCandidate.set(
      key,
      (publicFundsCentsByCandidate.get(key) ?? 0) + row.fundsApprovedCents,
    );
  }
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
        publicFundsCentsByCandidate.get(candidate.candidateName) ?? 0;
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
      const targeted = await getSanFranciscoCandidateTargetedSpending(
        {
          candidateLastName: lastNameOf(candidate.candidateName),
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
        outside: {
          manifest_support: centsToMoney(manifestSupportCents),
          manifest_oppose: centsToMoney(manifestOpposeCents),
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
      candidates.push({
        candidate: candidate.candidateName,
        fppc_committee_id: candidate.fppcId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return {
    election_date: target.electionDate,
    contest_code: target.contestCode,
    title: manifest.title,
    source_url: manifest.sourceUrl,
    candidate_count: manifest.candidates.length,
    outside_relation_count: manifest.outsideRelations.length,
    candidates,
  };
}

const targets = parseTargets(process.argv);
const contests = [];
for (const target of targets) contests.push(await probeContest(target));
console.log(JSON.stringify({ contests }, null, 2));

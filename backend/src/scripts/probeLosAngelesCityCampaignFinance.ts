import { aggregateLosAngelesDirectContributions } from "../pipeline/losAngelesCityFinance/losAngelesDirectContributionAggregator.js";
import {
  getLosAngelesEthicsCandidateTotals,
  getLosAngelesEthicsElections,
  getLosAngelesIndependentSpending,
} from "../pipeline/losAngelesCityFinance/losAngelesCityEthicsClient.js";
import { resolveLosAngelesEthicsElection } from "../pipeline/losAngelesCityFinance/losAngelesCandidateCommitteeResolver.js";
import { getLosAngelesCommitteeContributions } from "../pipeline/losAngelesCityFinance/losAngelesOpenDataClient.js";
const yearArg = process.argv.find((arg) => /^--year=\d{4}$/.test(arg));
const year = yearArg ? Number(yearArg.slice(7)) : new Date().getUTCFullYear();
const dateArg = process.argv.find((arg) =>
  /^--election-date=\d{4}-\d{2}-\d{2}$/.test(arg),
);
const electionDate =
  dateArg?.slice("--election-date=".length) ??
  (year === 2026 ? "2026-06-02" : null);
if (!electionDate)
  throw new Error("Pass --election-date=YYYY-MM-DD for non-2026 probes");
const elections = await getLosAngelesEthicsElections();
const election = resolveLosAngelesEthicsElection({
  elections,
  electionYear: year,
});
if (!election)
  throw new Error(`No unique Los Angeles City election for ${year}`);
const totals = await getLosAngelesEthicsCandidateTotals({
  electionId: election.electionId,
  officeName: "Mayor",
});
const candidates = [];
for (const total of totals) {
  try {
    const records = await getLosAngelesCommitteeContributions({
      committeeId: total.fppcCommitteeId,
      electionDate,
    });
    const direct = aggregateLosAngelesDirectContributions({ records });
    const [support, oppose] = await Promise.all([
      getLosAngelesIndependentSpending({
        electionSeatCandidateId: total.electionSeatCandidateId,
        supportOppose: "support",
      }),
      getLosAngelesIndependentSpending({
        electionSeatCandidateId: total.electionSeatCandidateId,
        supportOppose: "oppose",
      }),
    ]);
    candidates.push({
      candidate: total.candidateName,
      candidate_person_id: total.candidatePersonId,
      fppc_committee_id: total.fppcCommitteeId,
      headline_contributions: total.totalContributions,
      reconciled_line_items: direct.reconciledContributionTotal,
      difference:
        Math.round(
          (direct.reconciledContributionTotal - total.totalContributions) * 100,
        ) / 100,
      support_rows: support.length,
      oppose_rows: oppose.length,
    });
  } catch (error) {
    candidates.push({
      candidate: total.candidateName,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
console.log(
  JSON.stringify(
    { election, candidate_count: totals.length, candidates },
    null,
    2,
  ),
);

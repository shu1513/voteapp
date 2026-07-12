import { aggregateLosAngelesDirectContributions } from "../pipeline/losAngelesCityFinance/losAngelesDirectContributionAggregator.js";
import {
  getLosAngelesEthicsCandidateTotals,
  getLosAngelesEthicsElections,
  getLosAngelesIndependentSpending,
} from "../pipeline/losAngelesCityFinance/losAngelesCityEthicsClient.js";
import { resolveLosAngelesEthicsElection } from "../pipeline/losAngelesCityFinance/losAngelesCandidateCommitteeResolver.js";
import { getLosAngelesCommitteeContributions } from "../pipeline/losAngelesCityFinance/losAngelesOpenDataClient.js";
import { toLosAngelesEthicsOfficeName } from "../pipeline/losAngelesCityFinance/losAngelesCityFinanceEligibleOffices.js";
const yearArg = process.argv.find((arg) => /^--year=\d{4}$/.test(arg));
const year = yearArg ? Number(yearArg.slice(7)) : new Date().getUTCFullYear();
const officeArg = process.argv.find((arg) => arg.startsWith("--office="));
const officeCanonicalName = officeArg?.slice("--office=".length) || "Mayor";
const seatArg = process.argv.find((arg) => /^--seat=\d{1,2}$/.test(arg));
const seatNumber = seatArg ? Number(seatArg.slice("--seat=".length)) : null;
const ethicsOfficeName = toLosAngelesEthicsOfficeName({
  officeScope: "place",
  officeCanonicalName,
  seatNumber,
});
if (!ethicsOfficeName)
  throw new Error(
    `Unsupported Los Angeles City finance office: ${officeCanonicalName}`,
  );
const elections = await getLosAngelesEthicsElections();
const election = resolveLosAngelesEthicsElection({
  elections,
  electionYear: year,
});
if (!election)
  throw new Error(`No unique Los Angeles City election for ${year}`);
const totals = await getLosAngelesEthicsCandidateTotals({
  electionId: election.electionId,
  officeName: ethicsOfficeName,
});
const candidates = [];
for (const total of totals) {
  try {
    const records = await getLosAngelesCommitteeContributions({
      committeeId: total.fppcCommitteeId,
      electionYear: year,
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
    {
      election,
      office_canonical_name: officeCanonicalName,
      seat_number: seatNumber,
      ethics_office_name: ethicsOfficeName,
      candidate_count: totals.length,
      candidates,
    },
    null,
    2,
  ),
);

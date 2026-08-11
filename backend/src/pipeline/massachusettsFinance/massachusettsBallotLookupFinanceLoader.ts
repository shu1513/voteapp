import type { Pool, PoolClient } from "pg";

import { isMassachusettsCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isMassachusettsFinanceEligibleElectionRow } from "./massachusettsFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL = "https://www.ocpf.us/";

export async function loadMassachusettsCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "MA",
    source: "MASSACHUSETTS_OCPF",
    sourceUrl: GENERIC_MASSACHUSETTS_OCPF_SOURCE_URL,
    enabled: isMassachusettsCampaignFinanceEnabled,
    // Place offices additionally require an allowlisted municipal GEOID.
    isEligibleElection: (row) => isMassachusettsFinanceEligibleElectionRow(row),
    // Massachusetts renames both identities: candidate_cpf_id on the link
    // table, iepac_cpf_id/iepac_name on the outside tables.
    linkIdentityColumn: "candidate_cpf_id",
    outsideGroupIdentityColumns: { id: "iepac_cpf_id", name: "iepac_name" },
    evidenceLabelTypes: ["donor"],
    tables: {
      links: "ma_candidate_finance_links",
      summaries: "ma_candidate_finance_summaries",
      directBreakdowns: "ma_candidate_finance_direct_breakdowns",
      outsideGroups: "ma_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "ma_candidate_finance_outside_group_breakdowns",
    },
  });
}

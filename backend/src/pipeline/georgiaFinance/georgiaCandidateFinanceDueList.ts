import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
  type StandardStateFinanceDueRow,
} from "../finance/standardStateFinanceDueListQuery.js";
import { GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./georgiaFinanceEligibleOffices.js";
import type { GeorgiaFinanceLinkSource } from "./georgiaFinanceWriter.js";

// Georgia due-list query on the shared factory (georgia_plan.md PR 3):
// canonical link columns (committee_id = PeachFile filerEntityId), stalest
// first, bounded to the election window. PR 4's batch sync consumes this.
// link_source rides along so the sync writes the link back with its original
// provenance — a manual link must stay "manual" or it becomes eligible for
// auto-link supersession.
export type GeorgiaCandidateFinanceDueRow = StandardStateFinanceDueRow & {
  linkSource: GeorgiaFinanceLinkSource;
};

export const listDueGeorgiaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<GeorgiaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "GA",
    tables: {
      links: "ga_candidate_finance_links",
      summaries: "ga_candidate_finance_summaries",
    },
    eligibleOfficeKeys: GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
    linkColumns: ["committee_id", "committee_name", "link_source"],
    mapRow: (row): GeorgiaCandidateFinanceDueRow => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      committeeId: row.committee_id as string,
      committeeName: row.committee_name as string,
      linkSource: row.link_source as GeorgiaFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

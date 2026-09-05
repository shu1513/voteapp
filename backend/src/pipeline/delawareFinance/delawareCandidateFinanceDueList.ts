// Delaware finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first) narrowed
// to general-stage elections. election_date rides along for the window
// resolver; link_source is carried through so the sync writes the link back
// unchanged. The CFRS id is stored in committee_id and surfaces as cfId.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./delawareFinanceEligibleOffices.js";
import type { DelawareFinanceLinkSource } from "./delawareFinanceWriter.js";

export type DelawareCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeScope: string;
  officeName: string;
  district: string | null;
  cfId: string;
  committeeName: string;
  linkSource: DelawareFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueDelawareCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<DelawareCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "DE",
    tables: {
      links: "de_candidate_finance_links",
      summaries: "de_candidate_finance_summaries",
    },
    // The office-key constant is a Set; the builder binds a readonly array.
    eligibleOfficeKeys: [...DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS],
    electionStage: "general",
    selectElectionDate: true,
    linkColumns: ["committee_id", "committee_name", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      electionDate: (row.election_date as string).slice(0, 10),
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      cfId: row.committee_id as string,
      committeeName: row.committee_name as string,
      linkSource: row.link_source as DelawareFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

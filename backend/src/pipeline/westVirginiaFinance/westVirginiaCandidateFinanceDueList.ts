// West Virginia finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first), with
// link_source carried through so the sync writes the link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { WEST_VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./westVirginiaFinanceEligibleOffices.js";
import type { WestVirginiaFinanceLinkSource } from "./westVirginiaFinanceWriter.js";

export type WestVirginiaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  entityId: string;
  committeeName: string;
  linkSource: WestVirginiaFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueWestVirginiaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<WestVirginiaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "WV",
    tables: {
      links: "wv_candidate_finance_links",
      summaries: "wv_candidate_finance_summaries",
    },
    eligibleOfficeKeys: WEST_VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
    linkColumns: ["committee_id", "committee_name", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      entityId: row.committee_id as string,
      committeeName: row.committee_name as string,
      linkSource: row.link_source as WestVirginiaFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

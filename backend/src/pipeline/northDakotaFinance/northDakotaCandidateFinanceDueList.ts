// North Dakota finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first), with
// link_source carried through so the sync writes the link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { NORTH_DAKOTA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./northDakotaFinanceEligibleOffices.js";
import type { NorthDakotaFinanceLinkSource } from "./northDakotaFinanceWriter.js";

export type NorthDakotaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  entityId: string;
  committeeName: string;
  linkSource: NorthDakotaFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueNorthDakotaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<NorthDakotaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "ND",
    tables: {
      links: "nd_candidate_finance_links",
      summaries: "nd_candidate_finance_summaries",
    },
    eligibleOfficeKeys: NORTH_DAKOTA_FINANCE_ELIGIBLE_OFFICE_KEYS,
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
      linkSource: row.link_source as NorthDakotaFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

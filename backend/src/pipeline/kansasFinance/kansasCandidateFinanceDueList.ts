// Kansas finance due list: the shared link-gated staleness query (active
// links whose summary is missing or older than staleAfterDays). Kansas links
// carry the canonical committee_id/committee_name pair — committee_id is the
// viewer search recipe — plus link_source, carried through so the sync
// writes the link back unchanged (a manual link stays manual).

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./kansasFinanceEligibleOffices.js";
import type { KansasFinanceLinkSource } from "./kansasFinanceWriter.js";

export type KansasCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  /** The viewer search recipe "<officeCode>:<district>:<SURNAME>:<FIRST>". */
  committeeId: string;
  /** The filed spelling the link was verified against. */
  committeeName: string;
  linkSource: KansasFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueKansasCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<KansasCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "KS",
    tables: {
      links: "ks_candidate_finance_links",
      summaries: "ks_candidate_finance_summaries",
    },
    eligibleOfficeKeys: [...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS],
    linkColumns: ["committee_id", "committee_name", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      committeeId: row.committee_id as string,
      committeeName: row.committee_name as string,
      linkSource: row.link_source as KansasFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

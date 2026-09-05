// Missouri finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first) narrowed
// to general-stage elections. Only the DIRECT office subset is bound (place
// and school offices are excluded from the sync). election_date rides along
// for cycle boundaries; link_source is carried through so the sync writes
// the link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./missouriFinanceEligibleOffices.js";
import type { MissouriFinanceLinkSource } from "./missouriFinanceWriter.js";

export type MissouriCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeScope: string;
  officeName: string;
  district: string | null;
  committeeId: string;
  committeeName: string;
  linkSource: MissouriFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueMissouriCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<MissouriCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "MO",
    tables: {
      links: "mo_candidate_finance_links",
      summaries: "mo_candidate_finance_summaries",
    },
    // The office-key constant is a Set; the builder binds a readonly array.
    eligibleOfficeKeys: [...MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS],
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
      committeeId: row.committee_id as string,
      committeeName: row.committee_name as string,
      linkSource: row.link_source as MissouriFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

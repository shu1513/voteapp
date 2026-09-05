// Montana finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first) narrowed
// to general-stage elections, because Montana links FROM the Nov-2026
// general roster only. election_date rides along for the batch's per-year
// artifact sweeps; link_source is carried through so the sync writes the
// link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./montanaFinanceEligibleOffices.js";
import { normalizeMontanaCersEntityId, type MontanaFinanceLinkSource } from "./montanaFinanceWriter.js";

export type MontanaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeScope: string;
  officeName: string;
  district: string | null;
  /** Numeric CERS candidateId, stored as text in committee_id. */
  committeeId: string;
  committeeName: string;
  linkSource: MontanaFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueMontanaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<MontanaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "MT",
    tables: {
      links: "mt_candidate_finance_links",
      summaries: "mt_candidate_finance_summaries",
    },
    // The office-key constant is a Set; the builder binds a readonly array.
    eligibleOfficeKeys: [...MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
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
      committeeId: normalizeMontanaCersEntityId(row.committee_id as string),
      committeeName: row.committee_name as string,
      linkSource: row.link_source as MontanaFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

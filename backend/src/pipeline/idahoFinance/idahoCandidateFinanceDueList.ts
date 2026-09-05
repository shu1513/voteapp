// Idaho finance due list — the standard link-gated staleness query (active
// links whose summary is missing or older than staleAfterDays, for eligible
// offices inside the election window, never-synced first) narrowed to
// general-stage elections, because Idaho links FROM the Nov-2026 general
// roster only. The link identity is the Sunshine registration_guid plus
// filer_name; election_date rides along for the sync's cycle windows;
// link_source is carried through so the sync writes the link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { normalizeIdahoRegistrationGuid } from "./idahoCfsClient.js";
import { IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./idahoFinanceEligibleOffices.js";
import type { IdahoFinanceLinkSource } from "./idahoFinanceWriter.js";

export type IdahoCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeScope: string;
  officeName: string;
  district: string | null;
  /** Sunshine registration guid (lowercase uuid text). */
  registrationGuid: string;
  filerName: string;
  linkSource: IdahoFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueIdahoCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<IdahoCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "ID",
    tables: {
      links: "id_candidate_finance_links",
      summaries: "id_candidate_finance_summaries",
    },
    // The office-key constant is a Set; the builder binds a readonly array.
    eligibleOfficeKeys: [...IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS],
    electionStage: "general",
    selectElectionDate: true,
    linkColumns: ["registration_guid", "filer_name", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      electionDate: (row.election_date as string).slice(0, 10),
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      registrationGuid: normalizeIdahoRegistrationGuid(row.registration_guid as string),
      filerName: row.filer_name as string,
      linkSource: row.link_source as IdahoFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

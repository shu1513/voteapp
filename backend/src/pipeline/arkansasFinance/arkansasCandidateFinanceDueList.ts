// Arkansas finance due list — the standard link-gated staleness query
// (active links whose summary is missing or older than staleAfterDays, for
// eligible offices inside the election window, never-synced first), with
// the CFIS identity and link_source carried through so the sync writes the
// link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./arkansasFinanceEligibleOffices.js";
import type { ArkansasFinanceLinkSource } from "./arkansasFinanceWriter.js";

export type ArkansasCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  /** CFIS filer entity ID, stored as text (migration 266). */
  filingEntityId: number;
  filerName: string;
  linkSource: ArkansasFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

function parseFilingEntityId(value: unknown): number {
  const text = typeof value === "string" ? value : String(value);
  if (!/^[1-9]\d*$/.test(text) || !Number.isSafeInteger(Number(text))) {
    throw new Error(`Invalid Arkansas filing entity ID in due list: ${text}`);
  }
  return Number(text);
}

export const listDueArkansasCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<ArkansasCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "AR",
    tables: {
      links: "ar_candidate_finance_links",
      summaries: "ar_candidate_finance_summaries",
    },
    eligibleOfficeKeys: ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
    linkColumns: ["filing_entity_id", "filer_name", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeName: row.office_name,
      district: row.district,
      filingEntityId: parseFilingEntityId(row.filing_entity_id),
      filerName: row.filer_name as string,
      linkSource: row.link_source as ArkansasFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

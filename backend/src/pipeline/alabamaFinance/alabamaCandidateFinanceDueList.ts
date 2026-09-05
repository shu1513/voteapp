// Alabama finance due list — the standard link-gated staleness query (active
// links whose summary is missing or older than staleAfterDays, for eligible
// offices inside the election window, never-synced first) narrowed to
// general-stage elections. The ballot title rides along for judicial
// office-label routing (no election_date: the sync derives its cover window
// elsewhere); the extra fcpa_committee_number link column and link_source
// are carried through so the sync writes the link back unchanged.

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
} from "../finance/standardStateFinanceDueListQuery.js";
import { ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./alabamaFinanceEligibleOffices.js";
import type { AlabamaFinanceLinkSource } from "./alabamaFinanceWriter.js";

export type AlabamaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  ballotTitle: string;
  district: string | null;
  internalCommitteeId: number;
  committeeName: string;
  fcpaCommitteeNumber: string | null;
  linkSource: AlabamaFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

function parseStoredInternalCommitteeId(value: string): number {
  const parsed = Number(value);
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid stored Alabama internal committee id: ${value}`);
  }
  return parsed;
}

export const listDueAlabamaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<AlabamaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "AL",
    tables: {
      links: "al_candidate_finance_links",
      summaries: "al_candidate_finance_summaries",
    },
    eligibleOfficeKeys: ALABAMA_FINANCE_ELIGIBLE_OFFICE_KEYS,
    electionStage: "general",
    selectBallotTitle: true,
    linkColumns: ["committee_id", "committee_name", "fcpa_committee_number", "link_source"],
    mapRow: (row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeName: row.office_name,
      ballotTitle: row.ballot_title as string,
      district: row.district,
      internalCommitteeId: parseStoredInternalCommitteeId(row.committee_id as string),
      committeeName: row.committee_name as string,
      fcpaCommitteeNumber: row.fcpa_committee_number as string | null,
      linkSource: row.link_source as AlabamaFinanceLinkSource,
      sourceUrl: row.source_url,
      lastSyncedAt: row.last_synced_at,
    }),
  });

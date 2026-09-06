// Kansas ballot-lookup finance loader: thin config wrapper over the standard
// loader. Totals are report cover lines (receipts = line 2, spent = line 4,
// cash = latest line 5; "raised" adds in-kind line 6 and removes loans and
// refunds). Buckets come from Schedules A/B of e-filed reports, so a paper
// filer whose covers were transcribed has totals and no buckets. Outside
// totals come from transcribed independent-expenditure statements and stay
// null until a statement names the candidate. A transcribed paper cover has
// receipts but no donor total, and receipts include loans and refunds, so
// "raised" stays unknown for those rows instead of borrowing receipts.

import type { Pool, PoolClient } from "pg";

import { isKansasCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { buildKansasCfrUrl, KANSAS_CFR_VIEWER_PAGES } from "./kansasCfrViewerClient.js";
import { isKansasFinanceEligibleOffice } from "./kansasFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// The viewer keeps report identity in server session state, so there is no
// per-report deep link; the entry page is the only stable public URL.
export const KANSAS_CFR_PUBLIC_SITE_URL = buildKansasCfrUrl(KANSAS_CFR_VIEWER_PAGES.entry);

const KANSAS_DIRECT_COVERAGE_NOTE =
  "Totals are the cover-page figures of the reports filed with the Kansas Secretary of State. Contribution-size and occupation breakdowns come from the itemized schedules of electronically filed reports, so paper filers show totals only, and Kansas records an occupation only for individual donors who give more than $150.";

const KANSAS_OUTSIDE_COVERAGE_NOTE =
  "Outside spending counts independent-expenditure statements filed with the Kansas Public Disclosure Commission that name this candidate, using the stance each statement states. The groups' own funders are not shown.";

export async function loadKansasCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "KS",
    source: "KANSAS_SOS",
    sourceUrl: KANSAS_CFR_PUBLIC_SITE_URL,
    enabled: isKansasCampaignFinanceEnabled,
    isEligibleElection: (row) => isKansasFinanceEligibleOffice(officeInputFromElectionRow(row)),
    directCoverageNote: KANSAS_DIRECT_COVERAGE_NOTE,
    raisedFallsBackToReceipts: false,
    outsideCoverageNote: KANSAS_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "ks_candidate_finance_links",
      summaries: "ks_candidate_finance_summaries",
      directBreakdowns: "ks_candidate_finance_direct_breakdowns",
      outsideGroups: "ks_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "ks_candidate_finance_outside_group_breakdowns",
    },
  });
}

// Kansas finance due list: the shared link-gated staleness query (active
// links whose summary is missing or older than staleAfterDays). Kansas links
// carry the canonical committee_id/committee_name pair — committee_id is the
// viewer search recipe — so the standard row shape is used unchanged.

import { createStandardStateFinanceDueListQuery } from "../finance/standardStateFinanceDueListQuery.js";
import { KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./kansasFinanceEligibleOffices.js";

export const listDueKansasCandidateFinanceSyncRows = createStandardStateFinanceDueListQuery({
  state: "KS",
  tables: {
    links: "ks_candidate_finance_links",
    summaries: "ks_candidate_finance_summaries",
  },
  eligibleOfficeKeys: [...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS],
});

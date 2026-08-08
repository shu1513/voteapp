import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueListQuery,
  type StandardStateFinanceDueRow,
} from "../finance/standardStateFinanceDueListQuery.js";
import { GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./georgiaFinanceEligibleOffices.js";

// Georgia due-list query on the shared factory (georgia_plan.md PR 3):
// canonical link columns (committee_id = PeachFile filerEntityId), stalest
// first, bounded to the election window. PR 4's batch sync consumes this.
export type GeorgiaCandidateFinanceDueRow = StandardStateFinanceDueRow;

export const listDueGeorgiaCandidateFinanceSyncRows: StandardStateFinanceDueListQuery<GeorgiaCandidateFinanceDueRow> =
  createStandardStateFinanceDueListQuery({
    state: "GA",
    tables: {
      links: "ga_candidate_finance_links",
      summaries: "ga_candidate_finance_summaries",
    },
    eligibleOfficeKeys: GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  });

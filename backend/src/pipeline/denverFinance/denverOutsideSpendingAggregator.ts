// Outside-spending aggregation for Denver (plan Phase 3). SearchLight's
// GetSupportingorOpposingIndependentSpendersByCommittee lists are
// server-aggregated and direction-labeled with server-resolved targets, so
// there is no target-matching veto machinery here — but the rows carry NO id
// and the schema requires a stable one. Each spender name is resolved to a
// search uniqueId ("Ind787") via getAllCommitteesAndCandidate:
//   - type-3 (independent-expenditure) entries only;
//   - exact raw-name tier first — IE names collide on punctuation ("A Better
//     Denver" Ind808 vs "A Better Denver!" Ind678 are distinct entities whose
//     names normalize identically), so a normalized tier may only run when the
//     exact tier finds nothing;
//   - each tier must yield exactly one entity; zero or multiple in both tiers
//     fails the candidate closed.
// Cross-check (every sync): the support list must sum to the overview's
// independentExpendituresSupportingCandidate and the oppose list to its
// opposing figure, to the cent — a mismatch fails the candidate closed.

import {
  getDenverOutsideSpenders,
  searchDenverCommitteesAndCandidates,
  DENVER_SEARCHLIGHT_SEARCH_TYPE_INDEPENDENT_EXPENDITURE,
  type DenverFinancialOverview,
  type DenverSearchlightClientOptions,
} from "./denverSearchlightClient.js";
import { normalizeDenverTextKey } from "./denverCandidateCommitteeResolver.js";
import type { DenverOutsideGroupInput } from "./denverFinanceWriter.js";

export type DenverOutsideAggregation = {
  groups: DenverOutsideGroupInput[];
  supportTotalCents: number;
  opposeTotalCents: number;
};

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

/**
 * Resolves one spender name to its search uniqueId. Exported for the sync
 * script's diagnostics; the memo map lets one run resolve a spender that
 * appears in both directions (CWA-COPE supported AND opposed Johnston in
 * cycle 26) with a single search.
 */
export async function resolveDenverOutsideSpenderId(
  spenderName: string,
  options: DenverSearchlightClientOptions = {},
): Promise<string> {
  const entries = (
    await searchDenverCommitteesAndCandidates(spenderName, options)
  ).filter(
    (entry) =>
      entry.type === DENVER_SEARCHLIGHT_SEARCH_TYPE_INDEPENDENT_EXPENDITURE,
  );
  const exact = entries.filter((entry) => entry.name === spenderName);
  if (exact.length === 1) return exact[0]!.uniqueId;
  if (exact.length > 1)
    throw new Error(
      `Denver outside spender "${spenderName}" matches ${exact.length} IE entities exactly (${exact
        .map((entry) => entry.uniqueId)
        .join(", ")}); cannot resolve a stable id`,
    );
  const normalizedName = normalizeDenverTextKey(spenderName);
  const normalized = entries.filter(
    (entry) => normalizeDenverTextKey(entry.name) === normalizedName,
  );
  if (normalized.length === 1) return normalized[0]!.uniqueId;
  throw new Error(
    `Denver outside spender "${spenderName}" resolves to ${normalized.length} IE entities (${normalized
      .map((entry) => `${entry.uniqueId} "${entry.name}"`)
      .join(", ")}); cannot resolve a stable id`,
  );
}

export async function aggregateDenverOutsideSpending(input: {
  filerId: number;
  electionCycleId: number;
  /** The same-call overview — the cross-check target for both list sums. */
  overview: DenverFinancialOverview;
  options?: DenverSearchlightClientOptions;
}): Promise<DenverOutsideAggregation> {
  const options = input.options ?? {};
  const spenderIdsByName = new Map<string, string>();
  const groups: DenverOutsideGroupInput[] = [];
  const totals = { support: 0, oppose: 0 };
  for (const direction of ["support", "oppose"] as const) {
    const spenders = await getDenverOutsideSpenders(
      { filerId: input.filerId, electionCycleId: input.electionCycleId, direction },
      options,
    );
    const seenIds = new Set<string>();
    for (const spender of spenders) {
      let spenderId = spenderIdsByName.get(spender.name);
      if (spenderId === undefined) {
        spenderId = await resolveDenverOutsideSpenderId(spender.name, options);
        spenderIdsByName.set(spender.name, spenderId);
      }
      // Two list rows landing on one entity would collide on the writer's
      // (spender, direction) key — that is unresolvable double-reporting,
      // not something to sum over silently.
      if (seenIds.has(spenderId))
        throw new Error(
          `Denver ${direction} spender list resolves two rows to ${spenderId}; refusing to merge them`,
        );
      seenIds.add(spenderId);
      totals[direction] += spender.totalCents;
      groups.push({
        spenderId,
        spenderName: spender.name,
        supportOppose: direction,
        amountCents: spender.totalCents,
      });
    }
  }
  const expected = {
    support: input.overview.independentExpendituresSupportingCandidateCents,
    oppose: input.overview.independentExpendituresOpposingCandidateCents,
  };
  for (const direction of ["support", "oppose"] as const) {
    if (totals[direction] !== expected[direction])
      throw new Error(
        `Denver ${direction} spender list sums to ${usd(totals[direction])} but the overview reports ${usd(expected[direction])}; failing closed`,
      );
  }
  return {
    groups,
    supportTotalCents: totals.support,
    opposeTotalCents: totals.oppose,
  };
}

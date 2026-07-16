import {
  claimCandidateRecordsSearch,
  markCandidateRecordsSearchCompleted,
  releaseCandidateRecordsSearchClaim,
  type CandidateRecordsClaimInput,
  type CandidateRecordsSearchClaimQueryable,
} from "./candidateRecordsSearchClaim.js";
import {
  computeCandidateRecordsSearchWindow,
  readCandidateRecordsOverlapDaysFromEnv,
  type CandidateRecordsSearchWindow,
} from "./candidateRecordsSearchWindow.js";
import { usLatestLocalDateIso } from "../../utils/usLocalDate.js";

export type CandidateRecordsSearchMetrics = {
  discovered_count: number;
  inserted_count: number;
  deduped_count: number;
  tagged_specific_count: number;
  tagged_general_count: number;
};

export type CandidateRecordsSearchExecutionContext = {
  candidateId: string;
  window: CandidateRecordsSearchWindow;
};

export type CandidateRecordsSearchExecutor = (
  context: CandidateRecordsSearchExecutionContext
) => Promise<CandidateRecordsSearchMetrics>;

export type CandidateRecordsLifecycleOptions = CandidateRecordsClaimInput & {
  overlapDays?: number;
  researchedThrough?: Date | string;
};

export type CandidateRecordsLifecycleSkipped = {
  status: "skipped";
  reason: "cooldown_or_active_claim";
  metrics: CandidateRecordsSearchMetrics;
};

export type CandidateRecordsLifecycleCompleted = {
  status: "completed";
  metrics: CandidateRecordsSearchMetrics;
  window: CandidateRecordsSearchWindow;
  candidateId: string;
};

export type CandidateRecordsLifecycleResult =
  | CandidateRecordsLifecycleSkipped
  | CandidateRecordsLifecycleCompleted;

export type CandidateRecordsLifecycleSummary = {
  claimed_count: number;
  skipped_cooldown_or_claim_count: number;
  discovered_count: number;
  inserted_count: number;
  deduped_count: number;
  tagged_specific_count: number;
  tagged_general_count: number;
};

const ZERO_METRICS: CandidateRecordsSearchMetrics = {
  discovered_count: 0,
  inserted_count: 0,
  deduped_count: 0,
  tagged_specific_count: 0,
  tagged_general_count: 0,
};

export function summarizeCandidateRecordsLifecycleResults(
  results: readonly CandidateRecordsLifecycleResult[]
): CandidateRecordsLifecycleSummary {
  const summary: CandidateRecordsLifecycleSummary = {
    claimed_count: 0,
    skipped_cooldown_or_claim_count: 0,
    discovered_count: 0,
    inserted_count: 0,
    deduped_count: 0,
    tagged_specific_count: 0,
    tagged_general_count: 0,
  };

  for (const result of results) {
    if (result.status === "skipped") {
      summary.skipped_cooldown_or_claim_count += 1;
      continue;
    }

    summary.claimed_count += 1;
    summary.discovered_count += result.metrics.discovered_count;
    summary.inserted_count += result.metrics.inserted_count;
    summary.deduped_count += result.metrics.deduped_count;
    summary.tagged_specific_count += result.metrics.tagged_specific_count;
    summary.tagged_general_count += result.metrics.tagged_general_count;
  }

  return summary;
}

function toDateOnly(value: Date | string): string {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid researchedThrough date: ${value}`);
  }
  return parsed.toISOString().slice(0, 10);
}

export async function runCandidateRecordsSearchLifecycle(
  client: CandidateRecordsSearchClaimQueryable,
  options: CandidateRecordsLifecycleOptions,
  executeSearch: CandidateRecordsSearchExecutor
): Promise<CandidateRecordsLifecycleResult> {
  const claim = await claimCandidateRecordsSearch(client, {
    candidateId: options.candidateId,
    asOf: options.asOf,
    cooldownDays: options.cooldownDays,
    leaseHours: options.leaseHours,
    ignoreCooldown: options.ignoreCooldown,
  });

  if (!claim.claimed) {
    return {
      status: "skipped",
      reason: "cooldown_or_active_claim",
      metrics: { ...ZERO_METRICS },
    };
  }

  const overlapDays = options.overlapDays ?? readCandidateRecordsOverlapDaysFromEnv();
  const window = computeCandidateRecordsSearchWindow(claim.lastRecordsResearchedThrough, overlapDays);

  try {
    const metrics = await executeSearch({
      candidateId: claim.candidateId,
      window,
    });

    // Default checkpoint: the US-latest local date of the run instant, not
    // its UTC date. Converting the instant via toISOString stamped tomorrow's
    // date after 5pm Pacific, and later incremental windows starting at the
    // checkpoint skipped that local day forever. An explicit
    // researchedThrough date string still wins untouched.
    const researchedThrough = options.researchedThrough
      ? toDateOnly(options.researchedThrough)
      : usLatestLocalDateIso(options.asOf ?? new Date());
    await markCandidateRecordsSearchCompleted(client, claim.candidateId, researchedThrough);

    return {
      status: "completed",
      candidateId: claim.candidateId,
      metrics,
      window,
    };
  } catch (error) {
    await releaseCandidateRecordsSearchClaim(client, claim.candidateId);
    throw error;
  }
}

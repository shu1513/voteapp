export type CandidateRecordDropOutcome = {
  discoveredTotalCount: number;
  persistedCount: number;
  transientDropCount: number;
  permanentDropCount: number;
  repairCallFailedRetryable: boolean;
};

export type CandidateRecordOutcomeDecision = {
  shouldRetry: boolean;
  reason: string;
};

export function decideCandidateRecordOutcome(
  input: CandidateRecordDropOutcome
): CandidateRecordOutcomeDecision {
  if (input.persistedCount > 0) {
    return { shouldRetry: false, reason: "persisted_records_available" };
  }

  if (input.discoveredTotalCount === 0) {
    return { shouldRetry: false, reason: "no_records_discovered" };
  }

  if (input.permanentDropCount > 0) {
    return { shouldRetry: false, reason: "permanent_source_failures" };
  }

  if (input.repairCallFailedRetryable) {
    return { shouldRetry: true, reason: "repair_call_retryable_failure" };
  }

  if (input.transientDropCount > 0) {
    return { shouldRetry: true, reason: "transient_source_failures" };
  }

  return { shouldRetry: false, reason: "no_retry_condition_met" };
}

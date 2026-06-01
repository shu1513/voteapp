export const CANDIDATE_RECORD_RUN_PROCESSED_MARKER_PREFIX =
  "staging:candidate_record_run_processed:";

export const CANDIDATE_RECORD_RUN_PROCESSED_MARKER_TTL_SECONDS = 86_400;

export function buildCandidateRecordRunProcessedMarkerKey(runId: string): string {
  return `${CANDIDATE_RECORD_RUN_PROCESSED_MARKER_PREFIX}${runId}`;
}


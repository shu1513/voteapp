import type { ParsedElectionResultPayloadRow } from "../../contracts/electionResultPayloadContract.js";
import type { ElectionResultPassType } from "../../types/electionResults.js";

export const ELECTION_NIGHT_RESULT_MAX_ATTEMPTS = 3;
export const CERTIFIED_RESULT_MAX_ATTEMPTS = 3;
export const CERTIFIED_RESULT_RETRY_INTERVAL_DAYS = 7;

const CLEAR_OFFICE_OUTCOMES = new Set(["won", "advanced", "runoff"]);
const CLEAR_BALLOT_MEASURE_OUTCOMES = new Set(["passed", "failed"]);

export function isClearElectionNightResult(row: ParsedElectionResultPayloadRow): boolean {
  if (row.result_status === "not_found" || row.result_status === "not_final_yet") {
    return false;
  }
  if (row.outcome === "unknown" || row.outcome === "too_close") {
    return false;
  }
  if (row.match_status === "unmatched" || row.match_status === "partial" || row.match_status === "not_found") {
    return false;
  }
  if (row.match_status === "not_applicable") {
    return CLEAR_BALLOT_MEASURE_OUTCOMES.has(row.outcome);
  }
  return row.match_status === "matched" && row.winners.length > 0 && CLEAR_OFFICE_OUTCOMES.has(row.outcome);
}

export function shouldMarkElectionResultPassChecked(input: {
  passType: ElectionResultPassType;
  row: ParsedElectionResultPayloadRow;
  electionNightAttemptCountAfterThisRun: number;
  certifiedAttemptCountAfterThisRun?: number;
}): boolean {
  if (input.passType === "certified") {
    if ((input.certifiedAttemptCountAfterThisRun ?? 0) >= CERTIFIED_RESULT_MAX_ATTEMPTS) {
      return true;
    }
    if (input.row.result_status === "not_final_yet" || input.row.result_status === "not_found") {
      return false;
    }
    if (
      input.row.match_status === "unmatched" ||
      input.row.match_status === "partial" ||
      input.row.match_status === "not_found"
    ) {
      return false;
    }
    return true;
  }

  return (
    isClearElectionNightResult(input.row) ||
    input.electionNightAttemptCountAfterThisRun >= ELECTION_NIGHT_RESULT_MAX_ATTEMPTS
  );
}

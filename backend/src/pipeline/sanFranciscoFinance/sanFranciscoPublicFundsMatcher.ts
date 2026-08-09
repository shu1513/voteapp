// Public-financing matcher (plan Phase 4). The dbak-p2fq dataset carries no
// committee id — only election date, district, "Last, First" candidate name,
// and approved amount — so a candidate's public funds are matched by name
// within one district's rows, failing closed on ambiguity: a wrong
// public-funds figure is worse than none. Every published funds_approved row
// is an approval (the pending_completed column is dead upstream, verified
// live on 152 rows) so matched rows are summed directly.
import { sanFranciscoCandidateNameMatches } from "./sanFranciscoCandidateCommitteeResolver.js";
import type { SanFranciscoPublicFundsRow } from "./sanFranciscoOpenDataClient.js";

// The public-financing program covers Mayor and Supervisor races only; its
// rows carry "Mayor" or a bare district number. A null return means the
// contest has no program, which is a normal zero, not an error.
export function sanFranciscoPublicFundsDistrictForContest(
  contestCode: string,
): string | null {
  if (contestCode === "myr") return "Mayor";
  const supervisorMatch = /^bos(\d{2})$/.exec(contestCode);
  return supervisorMatch ? String(Number(supervisorMatch[1])) : null;
}

export type SanFranciscoPublicFundsMatch = {
  status: "matched" | "none" | "ambiguous";
  publicFundsCents: number;
  /**
   * Individual approval amounts, in disclosure order — the direct
   * aggregator needs them to recognize 497-reported disbursements.
   */
  approvalCents: number[];
  /** Distinct source names that matched (2+ of them means ambiguous). */
  matchedNames: string[];
};

export function matchSanFranciscoPublicFunds(input: {
  rows: readonly SanFranciscoPublicFundsRow[];
  /** Manifest display name ("FIRST LAST"). */
  candidateName: string;
  /** From sanFranciscoPublicFundsDistrictForContest. */
  district: string;
}): SanFranciscoPublicFundsMatch {
  // Re-guard the district even though the fetch can filter server-side: a
  // caller passing an unscoped row set must never sum another contest.
  const districtRows = input.rows.filter(
    (row) => row.district === input.district,
  );
  // Group by the exact disclosed name first: one person's rows share their
  // spelling, and the ambiguity test is between DIFFERENT disclosed names
  // both matching this candidate.
  const rowsByName = new Map<string, SanFranciscoPublicFundsRow[]>();
  for (const row of districtRows) {
    const bucket = rowsByName.get(row.candidateName) ?? [];
    bucket.push(row);
    rowsByName.set(row.candidateName, bucket);
  }
  const matchedNames = [...rowsByName.keys()].filter((sourceName) =>
    sanFranciscoCandidateNameMatches(sourceName, input.candidateName),
  );
  if (matchedNames.length === 0)
    return { status: "none", publicFundsCents: 0, approvalCents: [], matchedNames: [] };
  if (matchedNames.length > 1)
    return {
      status: "ambiguous",
      publicFundsCents: 0,
      approvalCents: [],
      matchedNames,
    };
  const matchedRows = rowsByName.get(matchedNames[0]!)!;
  const approvalCents = matchedRows.map((row) => row.fundsApprovedCents);
  return {
    status: "matched",
    publicFundsCents: approvalCents.reduce((sum, cents) => sum + cents, 0),
    approvalCents,
    matchedNames,
  };
}

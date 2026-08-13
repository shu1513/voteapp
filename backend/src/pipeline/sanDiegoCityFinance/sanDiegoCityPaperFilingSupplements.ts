// Operator-curated independent expenditures from PAPER 496 filings.
//
// The bulk export carries e-filed data only (confirmed live 2026-08-12): the
// portal's search index lists paper filings (`efiled: false`) as scanned PDFs
// with no text layer, and neither the S496 sheet nor the search index carries
// the 496's per-candidate cumulative, so a paper IE can never be detected or
// attributed mechanically. Each entry here is a human-verified transcription
// of one paper 496 expenditure line, fed through the outside-spending
// aggregator's normal target-matching and veto pipeline (a mistyped target
// simply matches no roster candidate).
//
// Maintenance contract:
// - Add an entry only after reading the scanned form on the portal AND
//   confirming the expenditure is absent from the bulk export (a later e-filed
//   amendment that re-reports it would double-count; re-verify entries when
//   re-auditing a cycle).
// - `electionYear` is the ELECTION year the expenditure targets (the cycle
//   key), not necessarily the calendar year of the expenditure. Sync applies
//   only entries whose electionYear matches the candidate's election.
// - One entry per candidate named on the form; a multi-candidate paper 496
//   yields several entries sharing an eFilingId.

export type SanDiegoCityPaper496Supplement = {
  /** Election year the expenditure targets — sync's cycle filter key. */
  electionYear: number;
  /** Spender's FPPC ID as printed on the form. */
  spenderFilerId: string;
  spenderName: string;
  /** Target exactly as the filed form names the candidate. */
  candidateLastName: string;
  candidateFirstName: string | null;
  /** CAL office code as printed: CCM/COU = City Council Member (both
   * observed in the San Diego export), MAY = Mayor. */
  officeCd: "CCM" | "COU" | "MAY";
  jurisDscr: string | null;
  /** District number as printed; null when the form leaves it blank. */
  distNo: string | null;
  direction: "SUPPORT" | "OPPOSE";
  amountCents: number;
  /** Expenditure date from the form (ISO). */
  expenditureDate: string;
  /** Portal e_filing_id of the paper filing (search API; NOT in the export). */
  eFilingId: string;
  /** How the entry was verified — cite the scan and any corroboration. */
  sourceNote: string;
};

const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** Throws on the first defective entry — curated data fails loud, not quiet. */
export function validateSanDiegoCityPaper496Supplements(
  entries: readonly SanDiegoCityPaper496Supplement[],
): void {
  const seen = new Set<string>();
  for (const entry of entries) {
    const label = `paper 496 supplement ${entry.eFilingId} (${entry.candidateLastName})`;
    if (
      !Number.isInteger(entry.electionYear) ||
      entry.electionYear < 2000 ||
      entry.electionYear > 2100
    )
      throw new Error(`${label}: implausible electionYear ${entry.electionYear}`);
    for (const [field, value] of [
      ["spenderFilerId", entry.spenderFilerId],
      ["spenderName", entry.spenderName],
      ["candidateLastName", entry.candidateLastName],
      ["eFilingId", entry.eFilingId],
      ["sourceNote", entry.sourceNote],
    ] as const) {
      if (value.trim() === "") throw new Error(`${label}: ${field} is blank`);
    }
    if (!Number.isSafeInteger(entry.amountCents) || entry.amountCents <= 0)
      throw new Error(`${label}: amountCents must be a positive integer`);
    if (!ISO_DATE_PATTERN.test(entry.expenditureDate))
      throw new Error(`${label}: expenditureDate is not an ISO date`);
    // Round-trip through UTC so calendar-invalid dates (2026-02-29) fail
    // too — same standard the workbook parser holds export dates to.
    const [year, month, day] = entry.expenditureDate.split("-").map(Number);
    const roundTrip = new Date(Date.UTC(year!, month! - 1, day!));
    if (
      roundTrip.getUTCFullYear() !== year ||
      roundTrip.getUTCMonth() !== month! - 1 ||
      roundTrip.getUTCDate() !== day
    )
      throw new Error(`${label}: expenditureDate is not a calendar date`);
    // One paper filing may name several candidates, but never the same
    // candidate twice — a duplicate is an operator copy-paste error.
    const key = JSON.stringify([
      entry.eFilingId,
      entry.candidateLastName.trim().toUpperCase(),
      (entry.candidateFirstName ?? "").trim().toUpperCase(),
    ]);
    if (seen.has(key)) throw new Error(`${label}: duplicate entry`);
    seen.add(key);
  }
}

export const SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS: readonly SanDiegoCityPaper496Supplement[] =
  [
    // Ships empty (plan Phase 3). Entries are curated by the Phase 4 live-run
    // paper-filing sweep: list every non-e-filed 460/496/497 on the portal
    // for the cycle, read the scans that touch rostered candidates, and add
    // an entry only per the maintenance contract above. San Diego's sub-$10k
    // paper allowance means a WHOLE committee can be paper-only; that stays
    // a coverage-note case, never a full-460 transcription.
  ];

validateSanDiegoCityPaper496Supplements(SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS);

// Eligibility for Denver city campaign finance (plan-denver-finance.md
// Phase 2), copy-adapted from sanDiegoCityFinanceEligibleOffices. v1 scope is
// the cycle-36 contest only: the Nov 3, 2026 City Council Vacancy Election
// (At-Large Seat B) on the Denver place row. The at-large seat letter lives
// in the ballot title (canonical-district policy: at-large seats are
// elections on the place district with the seat in the title), and the same
// parser reads SearchLight's officeSought strings ("City Council At-Large
// Seat B"), so the roster side and the registration side meet on one rule.
// District council seats, Mayor, and the other citywide offices wait for the
// 2027 cycle (cycle 33) — widening this whitelist is that phase's work, not
// speculative support here. Sync is additionally gated to roster candidates;
// eligibility here is the structural whitelist, not the per-election gate.

export const DENVER_CITY_GEOID = "0820000";

/** SearchLight cycle 36 = 2026 City Council Vacancy Election (2026-11-03). */
export const DENVER_2026_VACANCY_ELECTION_CYCLE_ID = 36;

/**
 * The cycle-36 election date. Auto-link binds a SearchLight cycle to its
 * election date: eligibility alone would accept any Denver at-large council
 * election (a future 2027 at-large contest included), and resolving one
 * against another cycle's registrants would hand a repeat candidate the
 * wrong cycle's committee.
 */
export const DENVER_2026_VACANCY_ELECTION_DATE = "2026-11-03";

export const DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "City Council Member",
] as const;

type DenverFinanceEligibleOfficeName =
  (typeof DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

function normalizeDenverOfficeText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extracts the at-large seat letter from an office designation — a roster
 * ballot title ("City Council Member, At-Large Seat B") or a SearchLight
 * officeSought ("City Council At-Large Seat B"). Null when the text names no
 * at-large seat (district seats, Mayor, blank).
 */
export function parseDenverAtLargeSeatLetter(
  text: string | null | undefined,
): string | null {
  if (!text) return null;
  const match = /\bAT LARGE SEAT ([A-Z])\b/.exec(normalizeDenverOfficeText(text));
  return match ? match[1]! : null;
}

function isDenverFinanceEligibleOfficeName(
  value: string,
): value is DenverFinanceEligibleOfficeName {
  return (DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]).includes(
    value,
  );
}

export function isDenverFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  if (input.state?.trim().toUpperCase() !== "CO") return false;
  if (
    input.districtType?.trim() !== "place" ||
    input.geoidCompact?.trim() !== DENVER_CITY_GEOID ||
    input.officeScope?.trim() !== "place"
  )
    return false;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isDenverFinanceEligibleOfficeName(officeName)) return false;
  // v1: at-large seats only (cycle 36 has no other contest). A council
  // election whose title carries no at-large seat letter is a district seat —
  // out of scope until the 2027 cycle work widens this gate.
  return parseDenverAtLargeSeatLetter(input.officialBallotTitle) !== null;
}

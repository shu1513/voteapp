// Eligibility for City of Austin campaign finance (plan-austin-finance.md
// Phase 2), copy-adapted from denverFinanceEligibleOffices. Scope guard =
// the Austin place row + an explicit election-date allowlist — never a
// state-wide office sweep. Structural eligibility is any Mayor or City
// Council Member election on the place row (v1 data scope is the Nov 2026
// council seats; Mayor is the 2028 cycle, admitted structurally so the
// allowlist is the only thing to widen then).
//
// One office-code vocabulary joins the two sides: a roster election
// (canonical office + ballot title "City Council Member District 1") and a
// Report Detail row (`office_sought` "COUNCIL_MBR_DISTRICT_01 District 1")
// both parse to `COUNCIL_MBR_DISTRICT_01`, so the resolver's office gate and
// Phase 3's report filter compare one string. The source side reads ONLY
// the leading code — every candidate row carries it (30 distinct
// office_sought spellings checked live 2026-08-18: the trailing "District
// 1" / "District One" / "District District 1" drift is decoration).

export const AUSTIN_CITY_GEOID = "4805000";

export const AUSTIN_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "City Council Member",
] as const;

type AustinFinanceEligibleOfficeName =
  (typeof AUSTIN_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

/**
 * v1 election-date allowlist. Report Detail is keyed by `election_date`,
 * so each entry is one auto-link/sync scope; the Nov 2026 council seats
 * first, the 2028 mayoral cycle when its rosters exist. Selection and
 * auto-link both refuse dates outside this list.
 */
export const AUSTIN_FINANCE_ELECTION_DATES = ["2026-11-03"] as const;

/** Austin's 10-1 council: single-member districts 1–10. */
export const AUSTIN_COUNCIL_DISTRICT_COUNT = 10;

/** `MAYOR` or `COUNCIL_MBR_DISTRICT_NN` (NN zero-padded, 01–10). */
export type AustinOfficeCode = "MAYOR" | `COUNCIL_MBR_DISTRICT_${string}`;

export function isAustinFinanceSupportedElectionDate(
  value: string | null | undefined,
): boolean {
  return (AUSTIN_FINANCE_ELECTION_DATES as readonly string[]).includes(
    value ?? "",
  );
}

function normalizeAustinOfficeText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function councilDistrictOfficeCode(district: number): AustinOfficeCode | null {
  if (
    !Number.isInteger(district) ||
    district < 1 ||
    district > AUSTIN_COUNCIL_DISTRICT_COUNT
  )
    return null;
  return `COUNCIL_MBR_DISTRICT_${String(district).padStart(2, "0")}`;
}

/**
 * Roster side: canonical office + ballot title → office code. Mayor needs
 * no title; a council election takes its district number from the title
 * ("City Council Member District 1"). Null fails closed: an ineligible
 * office, a council title with no district number, an out-of-range number,
 * or a title naming two different districts.
 */
export function austinOfficeCodeForElection(input: {
  officeCanonicalName: string | null | undefined;
  officialBallotTitle: string | null | undefined;
}): AustinOfficeCode | null {
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (officeName === "Mayor") return "MAYOR";
  if (officeName !== "City Council Member") return null;
  const districts = new Set<number>();
  for (const match of normalizeAustinOfficeText(
    input.officialBallotTitle ?? "",
  ).matchAll(/\bDISTRICT (\d{1,2})\b/g)) {
    districts.add(Number(match[1]));
  }
  if (districts.size !== 1) return null;
  return councilDistrictOfficeCode([...districts][0]!);
}

/**
 * Source side: Report Detail `office_sought` (or DCE `office_sought_info`)
 * → office code by its LEADING code token; anything else ("NONE", "OTHER",
 * blank, an out-of-range district) is null.
 */
export function parseAustinOfficeSoughtCode(
  value: string | null | undefined,
): AustinOfficeCode | null {
  const match = /^(MAYOR|COUNCIL_MBR_DISTRICT_(\d{2}))\b/.exec(
    (value ?? "").trim().toUpperCase(),
  );
  if (!match) return null;
  if (match[1] === "MAYOR") return "MAYOR";
  return councilDistrictOfficeCode(Number(match[2]));
}

/** The standard `district` link column for a code: "District 1", or null for Mayor. */
export function austinOfficeCodeDistrictLabel(code: AustinOfficeCode): string | null {
  const match = /^COUNCIL_MBR_DISTRICT_(\d{2})$/.exec(code);
  return match ? `District ${Number(match[1])}` : null;
}

function isAustinFinanceEligibleOfficeName(
  value: string,
): value is AustinFinanceEligibleOfficeName {
  return (AUSTIN_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]).includes(
    value,
  );
}

export function isAustinFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  if (input.state?.trim().toUpperCase() !== "TX") return false;
  if (
    input.districtType?.trim() !== "place" ||
    input.geoidCompact?.trim() !== AUSTIN_CITY_GEOID ||
    input.officeScope?.trim() !== "place"
  )
    return false;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isAustinFinanceEligibleOfficeName(officeName)) return false;
  // A council election whose title names no single district has no office
  // code, so neither the resolver's office gate nor Phase 3's report filter
  // could be verified — out of scope.
  return (
    austinOfficeCodeForElection({
      officeCanonicalName: officeName,
      officialBallotTitle: input.officialBallotTitle,
    }) !== null
  );
}

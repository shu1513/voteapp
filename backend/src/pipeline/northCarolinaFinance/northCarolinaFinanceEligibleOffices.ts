export type NorthCarolinaFinanceEligibleOfficeKey = `${string}::${string}`;

// The General Assembly — the offices whose campaign finance NC state law has
// filed with the State Board of Elections and that actually have NC 2026
// election rows (DB-grounded 2026-08-06: the only statewide NC row is United
// States Senator, which is federal/FEC money). Council of State offices
// (Governor, Attorney General, ...) mostly run in presidential years; they
// join this list when a cycle with real statewide rows enters scope, grounded
// against those rows then. Judicial offices and district attorneys are
// deferred like every other state's, and county/municipal candidates file
// with county boards; see north_carolina_plan.md decision 2.
export const NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly NorthCarolinaFinanceEligibleOfficeKey[];

const NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toNorthCarolinaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NorthCarolinaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNorthCarolinaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNorthCarolinaFinanceOfficeKey(input);
  return key !== null && NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

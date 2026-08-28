export type SouthCarolinaFinanceEligibleOfficeKey = `${string}::${string}`;

// V1 scope: statewide constitutional offices plus SC Senate and SC House
// (plan-south-carolina-finance.md, Phase 2). United States Senator and
// United States Representative are federal races filed with the FEC, not the
// State Ethics Commission. Lieutenant Governor runs on a joint ticket with
// Governor (since 2018) and has no separate campaign account. County and
// municipal filers use the same API and can be enabled later by widening
// this list only.
export const SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Attorney General",
  "statewide::Secretary of State",
  "statewide::State Treasurer",
  "statewide::Comptroller",
  "statewide::Superintendent of Public Instruction",
  "statewide::Commissioner of Agriculture",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly SouthCarolinaFinanceEligibleOfficeKey[];

const SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toSouthCarolinaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): SouthCarolinaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isSouthCarolinaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toSouthCarolinaFinanceOfficeKey(input);
  return key !== null && SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

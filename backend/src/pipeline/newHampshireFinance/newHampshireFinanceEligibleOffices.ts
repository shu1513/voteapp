export type NewHampshireFinanceEligibleOfficeKey = `${string}::${string}`;

// VoteApp canonical offices that the CFS registration resolver can verify by
// exact office + district. Executive Council is intentionally absent until it
// has a canonical VoteApp office; accepting a nearby office would be a guess.
export const NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
  "county::County Commissioner",
  "county::District Attorney",
  "county::County Treasurer",
  "county::Sheriff",
  "county::County Recorder",
  "county::Clerk of Court",
] as const satisfies readonly NewHampshireFinanceEligibleOfficeKey[];

const NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toNewHampshireFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NewHampshireFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNewHampshireFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNewHampshireFinanceOfficeKey(input);
  return key !== null && NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

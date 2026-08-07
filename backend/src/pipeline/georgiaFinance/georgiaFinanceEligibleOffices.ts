export type GeorgiaFinanceEligibleOfficeKey = `${string}::${string}`;

// Georgia state-filed offices with real GA 2026 election rows (DB-grounded
// 2026-08-07): every statewide constitutional office plus the Public Service
// Commission and both General Assembly chambers. United States Senator is
// GA 2026's only other statewide row — federal money belongs to the FEC,
// never this module. County offices (County Commissioner, District Attorney)
// and school boards file locally, largely as uploaded documents without
// structured transactions, so they stay out of v1; see georgia_plan.md D9.
export const GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::Commissioner of Agriculture",
  "statewide::Commissioner of Insurance",
  "statewide::Labor Commissioner",
  "statewide::Superintendent of Public Instruction",
  "statewide::Public Service Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly GeorgiaFinanceEligibleOfficeKey[];

const GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toGeorgiaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): GeorgiaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isGeorgiaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toGeorgiaFinanceOfficeKey(input);
  return key !== null && GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

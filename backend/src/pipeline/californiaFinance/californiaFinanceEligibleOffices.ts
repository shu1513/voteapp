export type CaliforniaFinanceEligibleOfficeKey = `${string}::${string}`;

export const CALIFORNIA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "statewide::State Treasurer",
  "statewide::Commissioner of Insurance",
  "statewide::Superintendent of Public Instruction",
  "statewide::State Board of Equalization Member",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly CaliforniaFinanceEligibleOfficeKey[];

const CALIFORNIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(CALIFORNIA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toCaliforniaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): CaliforniaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isCaliforniaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toCaliforniaFinanceOfficeKey(input);
  return key !== null && CALIFORNIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export type FloridaFinanceEligibleOfficeKey = `${string}::${string}`;

export type FloridaFinanceOfficeScope = "statewide" | "state_upper" | "state_lower";

export const FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Attorney General",
  "statewide::Chief Financial Officer",
  "statewide::Commissioner of Agriculture",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly FloridaFinanceEligibleOfficeKey[];

const FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toFloridaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): FloridaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isFloridaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toFloridaFinanceOfficeKey(input);
  return key !== null && FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

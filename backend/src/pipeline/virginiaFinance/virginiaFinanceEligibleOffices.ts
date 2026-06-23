export type VirginiaFinanceEligibleOfficeKey = `${string}::${string}`;

export const VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly VirginiaFinanceEligibleOfficeKey[];

const VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toVirginiaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): VirginiaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isVirginiaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toVirginiaFinanceOfficeKey(input);
  return key !== null && VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

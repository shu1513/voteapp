export type OregonFinanceEligibleOfficeKey = `${string}::${string}`;

export const OREGON_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly OregonFinanceEligibleOfficeKey[];

const OREGON_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(OREGON_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toOregonFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): OregonFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isOregonFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toOregonFinanceOfficeKey(input);
  return key !== null && OREGON_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

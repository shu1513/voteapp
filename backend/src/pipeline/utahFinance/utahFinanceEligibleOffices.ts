export type UtahFinanceEligibleOfficeKey = `${string}::${string}`;

export const UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::State Auditor",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly UtahFinanceEligibleOfficeKey[];

const UTAH_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toUtahFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): UtahFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isUtahFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toUtahFinanceOfficeKey(input);
  return key !== null && UTAH_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

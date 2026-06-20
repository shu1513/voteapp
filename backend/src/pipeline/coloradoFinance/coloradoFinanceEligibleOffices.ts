export type ColoradoFinanceEligibleOfficeKey = `${string}::${string}`;

export const COLORADO_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly ColoradoFinanceEligibleOfficeKey[];

const COLORADO_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(COLORADO_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toColoradoFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): ColoradoFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isColoradoFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toColoradoFinanceOfficeKey(input);
  return key !== null && COLORADO_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export type MaineFinanceEligibleOfficeKey = `${string}::${string}`;

export const MAINE_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly MaineFinanceEligibleOfficeKey[];

const MAINE_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(MAINE_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toMaineFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): MaineFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isMaineFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toMaineFinanceOfficeKey(input);
  return key !== null && MAINE_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

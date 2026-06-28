export type KentuckyFinanceEligibleOfficeKey = `${string}::${string}`;

export const KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly KentuckyFinanceEligibleOfficeKey[];

const KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toKentuckyFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): KentuckyFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isKentuckyFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toKentuckyFinanceOfficeKey(input);
  return key !== null && KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

export function normalizeKentuckyKrefLocation(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.replace(/\s+/g, " ");
}

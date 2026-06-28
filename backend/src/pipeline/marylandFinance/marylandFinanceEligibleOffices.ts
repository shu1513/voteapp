export type MarylandFinanceEligibleOfficeKey = `${string}::${string}`;

export const MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Comptroller",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly MarylandFinanceEligibleOfficeKey[];

const MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toMarylandFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): MarylandFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isMarylandFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toMarylandFinanceOfficeKey(input);
  return key !== null && MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

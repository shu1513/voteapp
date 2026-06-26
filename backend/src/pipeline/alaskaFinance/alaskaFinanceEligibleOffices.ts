export type AlaskaFinanceEligibleOfficeKey = `${string}::${string}`;

export const ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly AlaskaFinanceEligibleOfficeKey[];

const ALASKA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toAlaskaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): AlaskaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isAlaskaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toAlaskaFinanceOfficeKey(input);
  return key !== null && ALASKA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

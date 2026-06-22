export type NewMexicoFinanceEligibleOfficeKey = `${string}::${string}`;

export const NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "statewide::Land Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly NewMexicoFinanceEligibleOfficeKey[];

const NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toNewMexicoFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NewMexicoFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNewMexicoFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNewMexicoFinanceOfficeKey(input);
  return key !== null && NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

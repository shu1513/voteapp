export type OklahomaFinanceEligibleOfficeKey = `${string}::${string}`;

export const OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "statewide::Superintendent of Public Instruction",
  "statewide::Commissioner of Insurance",
  "statewide::Labor Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly OklahomaFinanceEligibleOfficeKey[];

const OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toOklahomaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): OklahomaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isOklahomaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toOklahomaFinanceOfficeKey(input);
  return key !== null && OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

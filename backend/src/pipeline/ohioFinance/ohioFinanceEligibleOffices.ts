export type OhioFinanceEligibleOfficeKey = `${string}::${string}`;

// Statewide executive offices plus the General Assembly — the offices Ohio
// requires to file campaign finance electronically with the Secretary of
// State. Lieutenant Governor is deliberately absent: Ohio elects it on a
// joint ticket with Governor, so no separate election rows exist. Judicial
// offices (canonical name "State Level Judge") and county/municipal offices
// are deferred; see ohio_plan.md decision 2.
export const OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Attorney General",
  "statewide::Secretary of State",
  "statewide::State Auditor",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly OhioFinanceEligibleOfficeKey[];

const OHIO_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toOhioFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): OhioFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isOhioFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toOhioFinanceOfficeKey(input);
  return key !== null && OHIO_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

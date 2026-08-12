export type RhodeIslandFinanceEligibleOfficeKey = `${string}::${string}`;

// Rhode Island v1 scope: statewide general offices + General Assembly
// (rhode_island_plan.md decision 9). Rhode Island's official title for its
// treasurer is "General Treasurer", but the VoteApp canonical office name is
// "State Treasurer" (seedOffices.ts; the RI 2026 statewide election row is
// DB-verified as State Treasurer) — the list must use the canonical name or
// the race is silently omitted. Municipal offices stay out of v1: smaller
// committees may lawfully file on paper, so electronic coverage there is
// unproven.
export const RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Secretary of State",
  "statewide::Attorney General",
  "statewide::State Treasurer",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly RhodeIslandFinanceEligibleOfficeKey[];

const RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS
);

export function toRhodeIslandFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): RhodeIslandFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isRhodeIslandFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toRhodeIslandFinanceOfficeKey(input);
  return key !== null && RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

// Montana finance office eligibility (docs/plans/montana-finance.md, Phase 2a).
//
// Phase 3 scope: statewide + judicial + PSC + legislative; county and local
// offices stay behind a second validated pass. Federal races (US Sen/House)
// are the FEC path and never appear here. The keys are VoteApp office-catalog
// "scope::canonical_name" pairs present for Montana's Nov-2026 ballot
// (Supreme Court = statewide::State Level Judge) plus the PSC catalog key so
// PSC races link as soon as they are seeded.

const ELIGIBLE_OFFICE_KEYS = [
  "statewide::State Level Judge",
  "statewide::Public Service Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const;

export const MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS: ReadonlySet<string> = new Set(ELIGIBLE_OFFICE_KEYS);

export function isMontanaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const scope = input.officeScope?.trim();
  const name = input.officeCanonicalName?.trim();
  return Boolean(scope && name && MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS.has(`${scope}::${name}`));
}

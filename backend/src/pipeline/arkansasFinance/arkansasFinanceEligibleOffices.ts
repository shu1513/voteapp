export type ArkansasFinanceEligibleOfficeKey = `${string}::${string}`;

// V1 auto-link scope: the statewide constitutional offices and both
// legislative chambers. These are the offices whose CFIS candidate
// registrations carry a cycle `electionYear`; county and municipal
// registrations (Mayor, Justice of the Peace, County Judge, ...) carry NO
// election year in the live registry (2,366 of 2,925 candidate rows,
// verified 2026-09-02), so the resolver cannot pin their cycle and they stay
// out until a cycle rule for local filers is proven. Appellate courts are
// elected in March and are not on the November roster. United States
// Senator is federal (FEC).
export const ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Secretary of State",
  "statewide::State Treasurer",
  "statewide::State Auditor",
  "statewide::Land Commissioner",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly ArkansasFinanceEligibleOfficeKey[];

const ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(
  ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS
);

// CFIS `PublicLookup/GetOfficeSoughtLookup` names, pinned live 2026-09-02
// (42 offices; registration rows carry the same strings in `office`).
const ARKANSAS_CFIS_OFFICE_NAMES: Record<string, string> = {
  "statewide::Governor": "Governor",
  "statewide::Lieutenant Governor": "Lieutenant Governor",
  "statewide::Attorney General": "Attorney General",
  "statewide::Secretary of State": "Secretary Of State",
  "statewide::State Treasurer": "State Treasurer",
  "statewide::State Auditor": "Auditor Of State",
  "statewide::Land Commissioner": "State Land Commissioner",
  "state_upper::State Senator": "State Senate",
  "state_lower::State Lower Chamber Legislator": "State Representative",
};

export function toArkansasFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): ArkansasFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isArkansasFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toArkansasFinanceOfficeKey(input);
  return key !== null && ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

/**
 * CFIS office name for a VoteApp office; null outside the v1 scope so
 * callers fail closed instead of guessing a nearby office.
 */
export function arkansasCfisOfficeNameForOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): string | null {
  const key = toArkansasFinanceOfficeKey(input);
  return key === null ? null : (ARKANSAS_CFIS_OFFICE_NAMES[key] ?? null);
}

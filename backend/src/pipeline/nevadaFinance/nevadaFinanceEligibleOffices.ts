export type NevadaFinanceEligibleOfficeKey = `${string}::${string}`;

// VoteApp canonical offices whose Nevada filers report under the NV SOS
// jurisdiction in AURORA (statewide, legislature, statewide judiciary).
// County/city/school offices file under their own AURORA jurisdictions and are
// out of scope for v1; US House goes through the FEC path.
export const NEVADA_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "statewide::Governor",
  "statewide::Lieutenant Governor",
  "statewide::Attorney General",
  "statewide::Secretary of State",
  "statewide::State Treasurer",
  "statewide::Comptroller",
  "statewide::State Level Judge",
  "state_upper::State Senator",
  "state_lower::State Lower Chamber Legislator",
] as const satisfies readonly NevadaFinanceEligibleOfficeKey[];

const NEVADA_FINANCE_ELIGIBLE_OFFICE_KEY_SET = new Set<string>(NEVADA_FINANCE_ELIGIBLE_OFFICE_KEYS);

export function toNevadaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NevadaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNevadaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNevadaFinanceOfficeKey(input);
  return key !== null && NEVADA_FINANCE_ELIGIBLE_OFFICE_KEY_SET.has(key);
}

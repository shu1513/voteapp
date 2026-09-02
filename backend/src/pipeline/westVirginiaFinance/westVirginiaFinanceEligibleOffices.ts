// West Virginia v1 finance scope (plan-west-virginia-finance.md, Phase 1):
// the two legislative chambers, the only state races on the November 2026
// ballot that CFRS covers. Statewide executive offices run in presidential
// years, judicial races are decided at the May primary, and county/municipal
// filings stay with local clerks until 2027 — none of them are linkable in
// this cycle, so none are modeled. Widening later means extending this map
// (and the resolver's office/district evidence) only.

export type WestVirginiaFinanceEligibleOfficeKey = `${string}::${string}`;

/**
 * Registry `office` labels, pinned live 2026-09-01 from the 429 "2026
 * Election" State Candidate committees ("House of Delegates" 316, "State
 * Senator" 77). Registry `district` for both is the plain seat number.
 */
export type WestVirginiaRegistryOffice = "House of Delegates" | "State Senator";

const WEST_VIRGINIA_REGISTRY_OFFICE_BY_KEY: Record<string, WestVirginiaRegistryOffice> = {
  "state_lower::State Lower Chamber Legislator": "House of Delegates",
  "state_upper::State Senator": "State Senator",
};

export const WEST_VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS = Object.keys(
  WEST_VIRGINIA_REGISTRY_OFFICE_BY_KEY
) as readonly WestVirginiaFinanceEligibleOfficeKey[];

export function toWestVirginiaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): WestVirginiaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isWestVirginiaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toWestVirginiaFinanceOfficeKey(input);
  return key !== null && key in WEST_VIRGINIA_REGISTRY_OFFICE_BY_KEY;
}

/** Registry office label for a VoteApp race; null outside the v1 map. */
export function westVirginiaRegistryOfficeForRace(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): WestVirginiaRegistryOffice | null {
  const key = toWestVirginiaFinanceOfficeKey(input);
  return key === null ? null : (WEST_VIRGINIA_REGISTRY_OFFICE_BY_KEY[key] ?? null);
}

/**
 * Seat number from a VoteApp district name ("Delegate District 12 (2024);
 * West Virginia", "State Senate District 3 (2024); West Virginia"); null
 * when the name carries no district number.
 */
export function westVirginiaDistrictNumberFromDistrictName(name: string | null | undefined): number | null {
  if (!name) {
    return null;
  }
  const match = /\bDistrict\s+(\d+)\b/i.exec(name);
  if (!match) {
    return null;
  }
  const parsed = Number.parseInt(match[1]!, 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

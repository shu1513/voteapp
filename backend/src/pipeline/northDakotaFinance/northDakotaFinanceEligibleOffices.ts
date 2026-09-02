// North Dakota v1 finance scope (plan-north-dakota-finance.md, Phase 1):
// the VoteApp races on the November 2026 ballot whose filers are in CFRS,
// mapped to the registry's exact `office` labels. Labels pinned live
// 2026-09-01 from the 376 "2026 Election - Statewide" candidate committees
// (State Representative 197, State Senator 81, District Court Judge 60,
// Public Service Commissioner 9, Superintendent of Public Instruction 5,
// Supreme Court Justice 5, Secretary of State 4, Tax Commissioner 3,
// Agriculture Commissioner 3, Governor and Lt. Governor 3, State Auditor 2,
// Attorney General 2, State Treasurer 1, Insurance Commissioner 1).
//
// Left out on purpose: Governor, Auditor, Treasurer and Insurance
// Commissioner (presidential-year offices — no 2026 race), and District
// Court Judge (registry district is a named judicial district such as
// "Northeast Central District"; VoteApp carries no such rows yet). County,
// city, school and park filers are not in CFRS at all. Widening later means
// extending this map (and, for judicial districts, the district evidence).

export type NorthDakotaFinanceEligibleOfficeKey = `${string}::${string}`;

export type NorthDakotaRegistryOffice =
  | "State Representative"
  | "State Senator"
  | "Attorney General"
  | "Secretary of State"
  | "Agriculture Commissioner"
  | "Tax Commissioner"
  | "Public Service Commissioner"
  | "Superintendent of Public Instruction"
  | "Supreme Court Justice";

export type NorthDakotaEligibleOffice = {
  registryOffice: NorthDakotaRegistryOffice;
  /**
   * Legislative registry rows carry "District N" and the roster district
   * name carries the same seat number; statewide rows carry no district and
   * the roster district is the state itself, so the column is ignored.
   */
  districted: boolean;
};

const NORTH_DAKOTA_ELIGIBLE_OFFICE_BY_KEY: Record<string, NorthDakotaEligibleOffice> = {
  "state_lower::State Lower Chamber Legislator": { registryOffice: "State Representative", districted: true },
  "state_upper::State Senator": { registryOffice: "State Senator", districted: true },
  "statewide::Attorney General": { registryOffice: "Attorney General", districted: false },
  "statewide::Secretary of State": { registryOffice: "Secretary of State", districted: false },
  "statewide::Commissioner of Agriculture": { registryOffice: "Agriculture Commissioner", districted: false },
  // VoteApp catalogs North Dakota's Tax Commissioner under the generic
  // Comptroller office (local roster: Kroshus / Nelson, 2026).
  "statewide::Comptroller": { registryOffice: "Tax Commissioner", districted: false },
  "statewide::Public Service Commissioner": { registryOffice: "Public Service Commissioner", districted: false },
  "statewide::Superintendent of Public Instruction": {
    registryOffice: "Superintendent of Public Instruction",
    districted: false,
  },
  // The only statewide judicial race; District Court Judges are scoped to
  // judicial districts and are not mapped (see the header).
  "statewide::State Level Judge": { registryOffice: "Supreme Court Justice", districted: false },
};

export const NORTH_DAKOTA_FINANCE_ELIGIBLE_OFFICE_KEYS = Object.keys(
  NORTH_DAKOTA_ELIGIBLE_OFFICE_BY_KEY
) as readonly NorthDakotaFinanceEligibleOfficeKey[];

export function toNorthDakotaFinanceOfficeKey(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NorthDakotaFinanceEligibleOfficeKey | null {
  const officeScope = input.officeScope?.trim();
  const officeCanonicalName = input.officeCanonicalName?.trim();
  if (!officeScope || !officeCanonicalName) {
    return null;
  }
  return `${officeScope}::${officeCanonicalName}`;
}

export function isNorthDakotaFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  const key = toNorthDakotaFinanceOfficeKey(input);
  return key !== null && key in NORTH_DAKOTA_ELIGIBLE_OFFICE_BY_KEY;
}

/** Registry office for a VoteApp race; null outside the v1 map (fail closed). */
export function northDakotaEligibleOfficeForRace(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): NorthDakotaEligibleOffice | null {
  const key = toNorthDakotaFinanceOfficeKey(input);
  return key === null ? null : (NORTH_DAKOTA_ELIGIBLE_OFFICE_BY_KEY[key] ?? null);
}

/**
 * Seat number from a VoteApp legislative district name ("State Senate
 * District 11 (2024); North Dakota"); null when the name carries none (the
 * statewide district is "North Dakota").
 */
export function northDakotaDistrictNumberFromDistrictName(name: string | null | undefined): number | null {
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

/** Registry `district` label for a legislative seat ("District 11"). */
export function northDakotaRegistryDistrictLabel(districtNumber: number): string {
  if (!Number.isSafeInteger(districtNumber) || districtNumber <= 0) {
    throw new Error(`Invalid North Dakota district number: ${districtNumber}`);
  }
  return `District ${districtNumber}`;
}

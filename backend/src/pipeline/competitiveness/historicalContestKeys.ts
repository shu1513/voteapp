import { getStateAbbreviationByFips } from "../../constants/usStates.js";
import type { ElectionDistrictType } from "../../types/election.js";

export const HISTORICAL_CONTEST_OFFICE_TYPES = [
  "US_PRESIDENT",
  "US_SENATE",
  "US_HOUSE",
  "GOVERNOR",
  "STATE_SENATE",
  "STATE_HOUSE",
] as const;

export type HistoricalContestOfficeType = (typeof HISTORICAL_CONTEST_OFFICE_TYPES)[number];

export type HistoricalContestDistrictType = Extract<
  ElectionDistrictType,
  "statewide" | "us_house" | "state_upper" | "state_lower"
>;

export type HistoricalContestLookupKey = {
  state: string;
  state_fips: string;
  office_type: HistoricalContestOfficeType;
  district_type: HistoricalContestDistrictType;
  district_key: string;
  mit_office: string;
  mit_district: string;
};

export type HistoricalContestLookupInput = {
  officeCanonicalName: string | null | undefined;
  districtType: ElectionDistrictType;
  geoidCompact: string;
  stateFips: string;
};

const OFFICE_TO_HISTORICAL_TYPE: Record<string, HistoricalContestOfficeType> = {
  "President of the United States": "US_PRESIDENT",
  "United States Senator": "US_SENATE",
  "United States Representative": "US_HOUSE",
  Governor: "GOVERNOR",
  "State Senator": "STATE_SENATE",
  "State Lower Chamber Legislator": "STATE_HOUSE",
};

const HISTORICAL_TYPE_TO_MIT_OFFICE: Record<HistoricalContestOfficeType, string> = {
  US_PRESIDENT: "US PRESIDENT",
  US_SENATE: "US SENATE",
  US_HOUSE: "US HOUSE",
  GOVERNOR: "GOVERNOR",
  STATE_SENATE: "STATE SENATE",
  STATE_HOUSE: "STATE HOUSE",
};

const HISTORICAL_TYPE_TO_DISTRICT_TYPE: Record<HistoricalContestOfficeType, HistoricalContestDistrictType> = {
  US_PRESIDENT: "statewide",
  US_SENATE: "statewide",
  US_HOUSE: "us_house",
  GOVERNOR: "statewide",
  STATE_SENATE: "state_upper",
  STATE_HOUSE: "state_lower",
};

export function mapOfficeCanonicalNameToHistoricalOfficeType(
  officeCanonicalName: string | null | undefined
): HistoricalContestOfficeType | null {
  const normalized = officeCanonicalName?.trim();
  if (!normalized) {
    return null;
  }
  return OFFICE_TO_HISTORICAL_TYPE[normalized] ?? null;
}

export function mapHistoricalOfficeTypeToMitOffice(officeType: HistoricalContestOfficeType): string {
  return HISTORICAL_TYPE_TO_MIT_OFFICE[officeType];
}

export function expectedDistrictTypeForHistoricalOffice(
  officeType: HistoricalContestOfficeType
): HistoricalContestDistrictType {
  return HISTORICAL_TYPE_TO_DISTRICT_TYPE[officeType];
}

export function toMitDistrict(input: {
  districtType: ElectionDistrictType;
  geoidCompact: string;
  stateFips: string;
}): string | null {
  const districtType = input.districtType;
  const geoidCompact = input.geoidCompact.trim();
  const stateFips = input.stateFips.trim().padStart(2, "0");

  if (districtType === "statewide") {
    return geoidCompact === stateFips ? "STATEWIDE" : null;
  }

  if (districtType !== "us_house" && districtType !== "state_upper" && districtType !== "state_lower") {
    return null;
  }

  if (!geoidCompact.startsWith(stateFips) || geoidCompact.length <= stateFips.length) {
    return null;
  }

  const districtCode = geoidCompact.slice(stateFips.length);
  return /^[0-9]+$/.test(districtCode) ? districtCode.padStart(3, "0") : null;
}

export function fromMitDistrict(input: {
  districtType: HistoricalContestDistrictType;
  mitDistrict: string;
  stateFips: string;
}): string | null {
  const stateFips = input.stateFips.trim().padStart(2, "0");
  const mitDistrict = input.mitDistrict.trim().toUpperCase();

  if (input.districtType === "statewide") {
    return mitDistrict === "STATEWIDE" ? stateFips : null;
  }

  if (!/^[0-9]+$/.test(mitDistrict)) {
    return null;
  }

  const districtNumber = Number.parseInt(mitDistrict, 10);
  if (!Number.isInteger(districtNumber) || districtNumber < 0) {
    return null;
  }

  const districtCode =
    input.districtType === "us_house"
      ? String(districtNumber).padStart(2, "0")
      : String(districtNumber).padStart(3, "0");

  return `${stateFips}${districtCode}`;
}

export function buildHistoricalContestLookupKey(
  input: HistoricalContestLookupInput
): HistoricalContestLookupKey | null {
  const officeType = mapOfficeCanonicalNameToHistoricalOfficeType(input.officeCanonicalName);
  if (!officeType) {
    return null;
  }

  const expectedDistrictType = expectedDistrictTypeForHistoricalOffice(officeType);
  if (input.districtType !== expectedDistrictType) {
    return null;
  }

  const mitDistrict = toMitDistrict({
    districtType: input.districtType,
    geoidCompact: input.geoidCompact,
    stateFips: input.stateFips,
  });
  if (!mitDistrict) {
    return null;
  }

  const stateFips = input.stateFips.trim().padStart(2, "0");

  return {
    state: getStateAbbreviationByFips(stateFips),
    state_fips: stateFips,
    office_type: officeType,
    district_type: expectedDistrictType,
    district_key: input.geoidCompact.trim(),
    mit_office: mapHistoricalOfficeTypeToMitOffice(officeType),
    mit_district: mitDistrict,
  };
}

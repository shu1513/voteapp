import { getStateAbbreviationByFips } from "../../constants/usStates.js";
import type { ElectionDistrictType } from "../../types/election.js";

export const HISTORICAL_CONTEST_OFFICE_TYPES = [
  "US_PRESIDENT",
  "US_SENATE",
  "US_HOUSE",
  "GOVERNOR",
  "LIEUTENANT_GOVERNOR",
  "SECRETARY_OF_STATE",
  "ATTORNEY_GENERAL",
  "STATE_TREASURER",
  "STATE_AUDITOR",
  "COMPTROLLER",
  "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION",
  "COMMISSIONER_OF_AGRICULTURE",
  "COMMISSIONER_OF_INSURANCE",
  "LABOR_COMMISSIONER",
  "LAND_COMMISSIONER",
  "STATE_SENATE",
  "STATE_HOUSE",
  "COUNTY_SHERIFF",
  "DISTRICT_ATTORNEY",
  "COUNTY_CLERK",
  "COUNTY_ASSESSOR",
  "COUNTY_AUDITOR",
  "COUNTY_TREASURER",
  "COUNTY_RECORDER",
  "COUNTY_CORONER",
] as const;

export type HistoricalContestOfficeType = (typeof HISTORICAL_CONTEST_OFFICE_TYPES)[number];

export type HistoricalContestDistrictType = Extract<
  ElectionDistrictType,
  "statewide" | "us_house" | "state_upper" | "state_lower" | "county"
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
  "Lieutenant Governor": "LIEUTENANT_GOVERNOR",
  "Secretary of State": "SECRETARY_OF_STATE",
  "Attorney General": "ATTORNEY_GENERAL",
  "State Treasurer": "STATE_TREASURER",
  "State Auditor": "STATE_AUDITOR",
  Comptroller: "COMPTROLLER",
  "Superintendent of Public Instruction": "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION",
  "Commissioner of Agriculture": "COMMISSIONER_OF_AGRICULTURE",
  "Commissioner of Insurance": "COMMISSIONER_OF_INSURANCE",
  "Labor Commissioner": "LABOR_COMMISSIONER",
  "Land Commissioner": "LAND_COMMISSIONER",
  "State Senator": "STATE_SENATE",
  "State Lower Chamber Legislator": "STATE_HOUSE",
  Sheriff: "COUNTY_SHERIFF",
  "District Attorney": "DISTRICT_ATTORNEY",
  "County Clerk": "COUNTY_CLERK",
  "County Assessor": "COUNTY_ASSESSOR",
  "County Auditor": "COUNTY_AUDITOR",
  "County Treasurer": "COUNTY_TREASURER",
  "County Recorder": "COUNTY_RECORDER",
  "County Coroner": "COUNTY_CORONER",
};

// Canonical MIT/MEDSL-style labels used for lookup keys and API context. These
// do not necessarily preserve the exact raw office label from the source row.
const HISTORICAL_TYPE_TO_MIT_OFFICE: Record<HistoricalContestOfficeType, string> = {
  US_PRESIDENT: "US PRESIDENT",
  US_SENATE: "US SENATE",
  US_HOUSE: "US HOUSE",
  GOVERNOR: "GOVERNOR",
  LIEUTENANT_GOVERNOR: "LIEUTENANT GOVERNOR",
  SECRETARY_OF_STATE: "SECRETARY OF STATE",
  ATTORNEY_GENERAL: "ATTORNEY GENERAL",
  STATE_TREASURER: "STATE TREASURER",
  STATE_AUDITOR: "STATE AUDITOR",
  COMPTROLLER: "STATE CONTROLLER",
  SUPERINTENDENT_OF_PUBLIC_INSTRUCTION: "SUPERINTENDENT OF PUBLIC INSTRUCTION",
  COMMISSIONER_OF_AGRICULTURE: "COMMISSIONER OF AGRICULTURE",
  COMMISSIONER_OF_INSURANCE: "COMMISSIONER OF INSURANCE",
  LABOR_COMMISSIONER: "LABOR COMMISSIONER",
  LAND_COMMISSIONER: "LAND COMMISSIONER",
  STATE_SENATE: "STATE SENATE",
  STATE_HOUSE: "STATE HOUSE",
  COUNTY_SHERIFF: "COUNTY SHERIFF",
  DISTRICT_ATTORNEY: "DISTRICT ATTORNEY",
  COUNTY_CLERK: "COUNTY CLERK",
  COUNTY_ASSESSOR: "COUNTY ASSESSOR",
  COUNTY_AUDITOR: "COUNTY AUDITOR",
  COUNTY_TREASURER: "COUNTY TREASURER",
  COUNTY_RECORDER: "COUNTY RECORDER",
  COUNTY_CORONER: "COUNTY CORONER",
};

const HISTORICAL_TYPE_TO_DISTRICT_TYPE: Record<HistoricalContestOfficeType, HistoricalContestDistrictType> = {
  US_PRESIDENT: "statewide",
  US_SENATE: "statewide",
  US_HOUSE: "us_house",
  GOVERNOR: "statewide",
  LIEUTENANT_GOVERNOR: "statewide",
  SECRETARY_OF_STATE: "statewide",
  ATTORNEY_GENERAL: "statewide",
  STATE_TREASURER: "statewide",
  STATE_AUDITOR: "statewide",
  COMPTROLLER: "statewide",
  SUPERINTENDENT_OF_PUBLIC_INSTRUCTION: "statewide",
  COMMISSIONER_OF_AGRICULTURE: "statewide",
  COMMISSIONER_OF_INSURANCE: "statewide",
  LABOR_COMMISSIONER: "statewide",
  LAND_COMMISSIONER: "statewide",
  STATE_SENATE: "state_upper",
  STATE_HOUSE: "state_lower",
  COUNTY_SHERIFF: "county",
  DISTRICT_ATTORNEY: "county",
  COUNTY_CLERK: "county",
  COUNTY_ASSESSOR: "county",
  COUNTY_AUDITOR: "county",
  COUNTY_TREASURER: "county",
  COUNTY_RECORDER: "county",
  COUNTY_CORONER: "county",
};

function normalizeStateFips(value: string): string | null {
  const stateFips = value.trim().padStart(2, "0");
  return /^[0-9]{2}$/.test(stateFips) ? stateFips : null;
}

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
  const stateFips = normalizeStateFips(input.stateFips);
  if (!stateFips) {
    return null;
  }

  if (districtType === "statewide") {
    return geoidCompact === stateFips ? "STATEWIDE" : null;
  }

  if (districtType === "county") {
    return geoidCompact.startsWith(stateFips) && /^[0-9]{5}$/.test(geoidCompact) ? geoidCompact : null;
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
  const stateFips = normalizeStateFips(input.stateFips);
  if (!stateFips) {
    return null;
  }
  const mitDistrict = input.mitDistrict.trim().toUpperCase();

  if (input.districtType === "statewide") {
    return mitDistrict === "STATEWIDE" ? stateFips : null;
  }

  if (input.districtType === "county") {
    return mitDistrict.startsWith(stateFips) && /^[0-9]{5}$/.test(mitDistrict) ? mitDistrict : null;
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

  const stateFips = normalizeStateFips(input.stateFips);
  if (!stateFips) {
    return null;
  }

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

import { isHoustonFinanceOfficeName } from "./houstonFinanceOfficeTargets.js";

export const HOUSTON_CITY_GEOID = "4835000";
export const HOUSTON_CITY_DISTRICT_NAME = "Houston city, Texas";

export function isHoustonFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  return (
    input.state?.trim().toUpperCase() === "TX" &&
    input.districtType?.trim() === "place" &&
    input.geoidCompact?.trim() === HOUSTON_CITY_GEOID &&
    input.officeScope?.trim() === "place" &&
    isHoustonFinanceOfficeName(input.officeCanonicalName)
  );
}

export function isHoustonFinanceEligibleLinkedElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  districtName: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  return (
    input.state?.trim().toUpperCase() === "TX" &&
    input.districtType?.trim() === "place" &&
    input.districtName?.trim() === HOUSTON_CITY_DISTRICT_NAME &&
    input.officeScope?.trim() === "place" &&
    isHoustonFinanceOfficeName(input.officeCanonicalName)
  );
}

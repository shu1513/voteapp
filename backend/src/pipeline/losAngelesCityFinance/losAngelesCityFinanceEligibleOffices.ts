export const LOS_ANGELES_CITY_GEOID = "0644000";

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "place::Mayor",
] as const;

export function isLosAngelesCityFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): boolean {
  return (
    input.state?.trim().toUpperCase() === "CA" &&
    input.districtType?.trim() === "place" &&
    input.geoidCompact?.trim() === LOS_ANGELES_CITY_GEOID &&
    input.officeScope?.trim() === "place" &&
    input.officeCanonicalName?.trim() === "Mayor"
  );
}

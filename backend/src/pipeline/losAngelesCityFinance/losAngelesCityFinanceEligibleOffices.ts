export const LOS_ANGELES_CITY_GEOID = "0644000";

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "place::Mayor",
  "place::Municipal Attorney",
  "place::Municipal Controller",
] as const;

const ETHICS_OFFICE_BY_CANONICAL_NAME: Readonly<Record<string, string>> = {
  Mayor: "Mayor",
  "Municipal Attorney": "City Attorney",
  "Municipal Controller": "City Controller",
};

export function toLosAngelesEthicsOfficeName(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): string | null {
  if (input.officeScope?.trim() !== "place") return null;
  return (
    ETHICS_OFFICE_BY_CANONICAL_NAME[
      input.officeCanonicalName?.trim() ?? ""
    ] ?? null
  );
}

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
    toLosAngelesEthicsOfficeName(input) !== null
  );
}

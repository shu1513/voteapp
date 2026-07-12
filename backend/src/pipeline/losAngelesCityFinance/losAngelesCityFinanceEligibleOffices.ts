export const LOS_ANGELES_CITY_GEOID = "0644000";

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "Municipal Attorney",
  "Municipal Controller",
] as const;

type LosAngelesCityFinanceEligibleOfficeName =
  (typeof LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS: readonly `place::${LosAngelesCityFinanceEligibleOfficeName}`[] =
  LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES.map(
    (name): `place::${LosAngelesCityFinanceEligibleOfficeName}` =>
      `place::${name}`,
  );

const ETHICS_OFFICE_BY_CANONICAL_NAME: Readonly<
  Record<LosAngelesCityFinanceEligibleOfficeName, string>
> = {
  Mayor: "Mayor",
  "Municipal Attorney": "City Attorney",
  "Municipal Controller": "City Controller",
};

function isLosAngelesCityFinanceEligibleOfficeName(
  value: string,
): value is LosAngelesCityFinanceEligibleOfficeName {
  return (
    LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]
  ).includes(value);
}

export function toLosAngelesEthicsOfficeName(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
}): string | null {
  if (input.officeScope?.trim() !== "place") return null;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  return isLosAngelesCityFinanceEligibleOfficeName(officeName)
    ? ETHICS_OFFICE_BY_CANONICAL_NAME[officeName]
    : null;
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

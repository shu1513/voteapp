export const LOS_ANGELES_CITY_GEOID = "0644000";

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "Municipal Attorney",
  "Municipal Controller",
  "City Council Member",
] as const;

type LosAngelesCityFinanceEligibleOfficeName =
  (typeof LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS: readonly `place::${LosAngelesCityFinanceEligibleOfficeName}`[] =
  LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES.map(
    (name): `place::${LosAngelesCityFinanceEligibleOfficeName}` =>
      `place::${name}`,
  );

const ETHICS_OFFICE_BY_CANONICAL_NAME: Readonly<
  Record<
    Exclude<LosAngelesCityFinanceEligibleOfficeName, "City Council Member">,
    string
  >
> = {
  Mayor: "Mayor",
  "Municipal Attorney": "City Attorney",
  "Municipal Controller": "City Controller",
};

export function parseLosAngelesCityCouncilSeatNumber(
  officialBallotTitle: string | null | undefined,
): number | null {
  const normalized = officialBallotTitle
    ?.normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return null;
  const match =
    /^(?:MEMBER OF THE CITY COUNCIL|CITY COUNCIL MEMBER|COUNCIL MEMBER|COUNCILMEMBER|CITY COUNCIL|COUNCIL) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const seatNumber = Number(match[1]);
  return Number.isInteger(seatNumber) && seatNumber >= 1 && seatNumber <= 15
    ? seatNumber
    : null;
}

function isLosAngelesCityCouncilSeatNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 15
  );
}

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
  seatNumber?: number | null;
}): string | null {
  if (input.officeScope?.trim() !== "place") return null;
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isLosAngelesCityFinanceEligibleOfficeName(officeName)) return null;
  if (officeName === "City Council Member") {
    return isLosAngelesCityCouncilSeatNumber(input.seatNumber)
      ? `Council District ${input.seatNumber}`
      : null;
  }
  return ETHICS_OFFICE_BY_CANONICAL_NAME[officeName];
}

export function isLosAngelesCityFinanceEligibleElection(input: {
  state: string | null | undefined;
  districtType: string | null | undefined;
  geoidCompact: string | null | undefined;
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  officialBallotTitle?: string | null;
}): boolean {
  const seatNumber =
    input.officeCanonicalName?.trim() === "City Council Member"
      ? parseLosAngelesCityCouncilSeatNumber(input.officialBallotTitle)
      : null;
  return (
    input.state?.trim().toUpperCase() === "CA" &&
    input.districtType?.trim() === "place" &&
    input.geoidCompact?.trim() === LOS_ANGELES_CITY_GEOID &&
    toLosAngelesEthicsOfficeName({ ...input, seatNumber }) !== null
  );
}

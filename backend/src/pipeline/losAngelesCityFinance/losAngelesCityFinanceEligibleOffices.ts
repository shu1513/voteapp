export const LOS_ANGELES_CITY_GEOID = "0644000";
export const LOS_ANGELES_UNIFIED_SCHOOL_DISTRICT_GEOID = "0622710";

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES = [
  "Mayor",
  "Municipal Attorney",
  "Municipal Controller",
  "City Council Member",
  "School Board Member",
] as const;

type LosAngelesCityFinanceEligibleOfficeName =
  (typeof LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES)[number];

export const LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = [
  "place::Mayor",
  "place::Municipal Attorney",
  "place::Municipal Controller",
  "place::City Council Member",
  "school_unified::School Board Member",
] as const;

const ETHICS_OFFICE_BY_CANONICAL_NAME: Readonly<
  Record<
    Exclude<
      LosAngelesCityFinanceEligibleOfficeName,
      "City Council Member" | "School Board Member"
    >,
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
  return isLosAngelesCityCouncilSeatNumber(seatNumber) ? seatNumber : null;
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

export function parseLosAngelesSchoolBoardSeatNumber(
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
    /^(?:MEMBER OF THE BOARD OF EDUCATION|BOARD OF EDUCATION|SCHOOL BOARD MEMBER|SCHOOL BOARD) DISTRICT (?:NO )?(\d{1,2})$/.exec(
      normalized,
    );
  if (!match) return null;
  const seatNumber = Number(match[1]);
  return isLosAngelesSchoolBoardSeatNumber(seatNumber) ? seatNumber : null;
}

function isLosAngelesSchoolBoardSeatNumber(
  value: number | null | undefined,
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 7
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
  const officeScope = input.officeScope?.trim();
  const officeName = input.officeCanonicalName?.trim() ?? "";
  if (!isLosAngelesCityFinanceEligibleOfficeName(officeName)) return null;
  if (officeName === "School Board Member")
    return officeScope === "school_unified" &&
      isLosAngelesSchoolBoardSeatNumber(input.seatNumber)
      ? `LAUSD District ${input.seatNumber}`
      : null;
  if (officeScope !== "place") return null;
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
      : input.officeCanonicalName?.trim() === "School Board Member"
        ? parseLosAngelesSchoolBoardSeatNumber(input.officialBallotTitle)
        : null;
  const isCity =
    input.districtType?.trim() === "place" &&
    input.geoidCompact?.trim() === LOS_ANGELES_CITY_GEOID &&
    input.officeScope?.trim() === "place";
  const isLausd =
    input.districtType?.trim() === "school_unified" &&
    input.geoidCompact?.trim() === LOS_ANGELES_UNIFIED_SCHOOL_DISTRICT_GEOID &&
    input.officeScope?.trim() === "school_unified" &&
    input.officeCanonicalName?.trim() === "School Board Member";
  return (
    input.state?.trim().toUpperCase() === "CA" &&
    (isCity || isLausd) &&
    toLosAngelesEthicsOfficeName({ ...input, seatNumber }) !== null
  );
}

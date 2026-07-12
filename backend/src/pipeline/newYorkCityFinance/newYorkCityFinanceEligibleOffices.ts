export const NEW_YORK_CITY_PLACE_GEOID = "3651000";

export type NewYorkCityCfbOfficeCode = "1" | "2" | "3" | "4";
export type NewYorkCityCfbBoroughCode = "X" | "K" | "M" | "Q" | "S";

export const NEW_YORK_CITY_BOROUGH_GEOID_BY_CODE: Readonly<Record<NewYorkCityCfbBoroughCode, string>> = {
  X: "36005",
  K: "36047",
  M: "36061",
  Q: "36081",
  S: "36085",
};

const BOROUGH_CODE_BY_GEOID = new Map(
  Object.entries(NEW_YORK_CITY_BOROUGH_GEOID_BY_CODE).map(([code, geoid]) => [
    geoid,
    code as NewYorkCityCfbBoroughCode,
  ])
);

export const NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS = new Set([
  "place::Mayor",
  "place::Public Advocate",
  "place::Comptroller",
  "county::Borough President",
]);

export type NewYorkCityCfbOfficeSearchInput = {
  officeCode: NewYorkCityCfbOfficeCode;
  boroughCode: NewYorkCityCfbBoroughCode | null;
};

export function toNewYorkCityCfbOfficeSearchInput(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  districtGeoid: string | null | undefined;
}): NewYorkCityCfbOfficeSearchInput | null {
  const key = `${input.officeScope?.trim() ?? ""}::${input.officeCanonicalName?.trim() ?? ""}`;
  const geoid = input.districtGeoid?.trim() ?? "";
  if (!NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS.has(key)) {
    return null;
  }
  if (key === "place::Mayor") {
    return geoid === NEW_YORK_CITY_PLACE_GEOID ? { officeCode: "1", boroughCode: null } : null;
  }
  if (key === "place::Public Advocate") {
    return geoid === NEW_YORK_CITY_PLACE_GEOID ? { officeCode: "2", boroughCode: null } : null;
  }
  if (key === "place::Comptroller") {
    return geoid === NEW_YORK_CITY_PLACE_GEOID ? { officeCode: "3", boroughCode: null } : null;
  }
  const boroughCode = BOROUGH_CODE_BY_GEOID.get(geoid);
  return boroughCode ? { officeCode: "4", boroughCode } : null;
}
export function isNewYorkCityFinanceEligibleOffice(input: {
  officeScope: string | null | undefined;
  officeCanonicalName: string | null | undefined;
  districtGeoid: string | null | undefined;
}): boolean {
  return toNewYorkCityCfbOfficeSearchInput(input) !== null;
}

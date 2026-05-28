export const STATE_INFO_BY_FIPS: Record<string, { abbreviation: string; name: string }> = {
  "01": { abbreviation: "AL", name: "Alabama" },
  "02": { abbreviation: "AK", name: "Alaska" },
  "04": { abbreviation: "AZ", name: "Arizona" },
  "05": { abbreviation: "AR", name: "Arkansas" },
  "06": { abbreviation: "CA", name: "California" },
  "08": { abbreviation: "CO", name: "Colorado" },
  "09": { abbreviation: "CT", name: "Connecticut" },
  "10": { abbreviation: "DE", name: "Delaware" },
  "11": { abbreviation: "DC", name: "District of Columbia" },
  "12": { abbreviation: "FL", name: "Florida" },
  "13": { abbreviation: "GA", name: "Georgia" },
  "15": { abbreviation: "HI", name: "Hawaii" },
  "16": { abbreviation: "ID", name: "Idaho" },
  "17": { abbreviation: "IL", name: "Illinois" },
  "18": { abbreviation: "IN", name: "Indiana" },
  "19": { abbreviation: "IA", name: "Iowa" },
  "20": { abbreviation: "KS", name: "Kansas" },
  "21": { abbreviation: "KY", name: "Kentucky" },
  "22": { abbreviation: "LA", name: "Louisiana" },
  "23": { abbreviation: "ME", name: "Maine" },
  "24": { abbreviation: "MD", name: "Maryland" },
  "25": { abbreviation: "MA", name: "Massachusetts" },
  "26": { abbreviation: "MI", name: "Michigan" },
  "27": { abbreviation: "MN", name: "Minnesota" },
  "28": { abbreviation: "MS", name: "Mississippi" },
  "29": { abbreviation: "MO", name: "Missouri" },
  "30": { abbreviation: "MT", name: "Montana" },
  "31": { abbreviation: "NE", name: "Nebraska" },
  "32": { abbreviation: "NV", name: "Nevada" },
  "33": { abbreviation: "NH", name: "New Hampshire" },
  "34": { abbreviation: "NJ", name: "New Jersey" },
  "35": { abbreviation: "NM", name: "New Mexico" },
  "36": { abbreviation: "NY", name: "New York" },
  "37": { abbreviation: "NC", name: "North Carolina" },
  "38": { abbreviation: "ND", name: "North Dakota" },
  "39": { abbreviation: "OH", name: "Ohio" },
  "40": { abbreviation: "OK", name: "Oklahoma" },
  "41": { abbreviation: "OR", name: "Oregon" },
  "42": { abbreviation: "PA", name: "Pennsylvania" },
  "44": { abbreviation: "RI", name: "Rhode Island" },
  "45": { abbreviation: "SC", name: "South Carolina" },
  "46": { abbreviation: "SD", name: "South Dakota" },
  "47": { abbreviation: "TN", name: "Tennessee" },
  "48": { abbreviation: "TX", name: "Texas" },
  "49": { abbreviation: "UT", name: "Utah" },
  "50": { abbreviation: "VT", name: "Vermont" },
  "51": { abbreviation: "VA", name: "Virginia" },
  "53": { abbreviation: "WA", name: "Washington" },
  "54": { abbreviation: "WV", name: "West Virginia" },
  "55": { abbreviation: "WI", name: "Wisconsin" },
  "56": { abbreviation: "WY", name: "Wyoming" },
};

export const STATE_ABBR_BY_FIPS: Record<string, string> = Object.fromEntries(
  Object.entries(STATE_INFO_BY_FIPS).map(([fips, info]) => [fips, info.abbreviation])
);

export const STATE_NAME_BY_FIPS: Record<string, string> = Object.fromEntries(
  Object.entries(STATE_INFO_BY_FIPS).map(([fips, info]) => [fips, info.name])
);

export const STATE_NAME_BY_ABBREVIATION: Record<string, string> = Object.fromEntries(
  Object.values(STATE_INFO_BY_FIPS).map((info) => [info.abbreviation, info.name])
);

/**
 * Normalizes a state FIPS value to a two-character string.
 */
export function normalizeFips(value: string): string {
  return value.padStart(2, "0");
}

/**
 * Returns USPS two-letter state abbreviation for a given state FIPS code.
 * Throws when the provided FIPS code is not in the supported 50 states + DC map.
 */
export function getStateAbbreviationByFips(fips: string): string {
  const normalized = normalizeFips(fips.trim());
  const abbreviation = STATE_INFO_BY_FIPS[normalized]?.abbreviation;

  if (!abbreviation) {
    throw new Error(`Unknown state FIPS code: ${fips}`);
  }

  return abbreviation;
}

/**
 * Returns canonical state name for a given state FIPS code.
 * Throws when the provided FIPS code is not in the supported 50 states + DC map.
 */
export function getStateNameByFips(fips: string): string {
  const normalized = normalizeFips(fips.trim());
  const stateName = STATE_INFO_BY_FIPS[normalized]?.name;

  if (!stateName) {
    throw new Error(`Unknown state FIPS code: ${fips}`);
  }

  return stateName;
}

/**
 * Returns canonical state name for a USPS two-letter abbreviation.
 * Returns undefined when the abbreviation is not in the supported 50 states + DC map.
 */
export function getStateNameByAbbreviation(abbreviation: string): string | undefined {
  const normalized = abbreviation.trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) {
    return undefined;
  }
  return STATE_NAME_BY_ABBREVIATION[normalized];
}

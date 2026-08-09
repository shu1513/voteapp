// Many county boards number their seats by DISTRICT but still elect every seat
// COUNTYWIDE — the district is only a residency requirement for the candidate.
// North Carolina's association calls this "by district at large"; Florida's
// constitution makes it the default. In those places a "District 6" seat is on
// every county voter's ballot, so flagging it as possibly-not-yours (see
// subDistrictSeat.ts) states the opposite of the truth.
//
// Measured against the live corpus 2026-08-08: without this table, 82 of the
// 178 badged NC+FL county-board rows were wrong — 46% of them — plus every
// Idaho and Indiana commissioner row.
//
// Sources (all authoritative, all verified 2026-08-08):
//   NC — NC Association of County Commissioners, "North Carolina's Boards of
//        County Commissioners: Board Sizes and Election Methods as of January
//        2024". Counties listed as "by district at large", or as a combination
//        of at-large and by-district-at-large seats.
//   FL — Florida Association of Counties, County Districting. Art. VIII s.1(e)
//        Fla. Const. elects commissioners countywide unless the county adopts
//        single-member districts by referendum under s.124.011 Fla. Stat.
//   ID — Idaho Code tit. 31 ch. 7: commissioners must reside in their district
//        but are elected by the whole county.
//   IN — Indiana county commissioners are elected "from separate districts (in
//        which they must reside) but by vote of the county as a whole".
//        (Indiana COUNTY COUNCIL seats are true district seats — not listed.)
//
// This data changes only by referendum or local act, so it is a table rather
// than a lookup. Re-check the two association tables when a cycle turns over.

// Offices this applies to. County councils/boards of supervisors share the
// county-commission election method; nothing else does.
const COUNTY_BOARD_OFFICES: ReadonlySet<string> = new Set(["County Commissioner", "County Supervisor"]);

// Whole states where a county commissioner's district is residency-only.
const COUNTYWIDE_BOARD_STATES: ReadonlySet<string> = new Set(["ID", "IN"]);

// Indiana's county COUNCIL is districted for real, so the state rule above must
// not swallow it. Keyed by the office the council maps onto in the catalog.
const STATE_RULE_EXEMPT_OFFICES: Readonly<Record<string, ReadonlySet<string>>> = {
  IN: new Set(["County Supervisor"]),
};

const NORTH_CAROLINA_COUNTYWIDE_BOARDS: ReadonlySet<string> = new Set([
  "Anson",
  "Bertie",
  "Brunswick",
  "Camden",
  "Carteret",
  "Chatham",
  "Cherokee",
  "Currituck",
  "Dare",
  "Forsyth",
  "Gaston",
  "Gates",
  "Greene",
  "Henderson",
  "Hertford",
  "Hyde",
  "Jackson",
  "Johnston",
  "Macon",
  "Mecklenburg",
  "Moore",
  "Northampton",
  // Orange is a hybrid: the primary runs purely by district but the GENERAL
  // runs by district at large. The general is the one a ballot shows to every
  // county resident, so it is listed here.
  "Orange",
  "Pender",
  "Randolph",
  "Rutherford",
  "Scotland",
  "Stanly",
  "Surry",
  "Warren",
]);

const FLORIDA_COUNTYWIDE_BOARDS: ReadonlySet<string> = new Set([
  "Alachua",
  "Baker",
  "Bay",
  "Charlotte",
  "Citrus",
  "Clay",
  "DeSoto",
  "Dixie",
  "Flagler",
  "Gilchrist",
  "Glades",
  "Hardee",
  "Hernando",
  "Highlands",
  "Holmes",
  "Indian River",
  "Lafayette",
  "Lake",
  "Lee",
  "Levy",
  "Liberty",
  "Marion",
  "Martin",
  "Monroe",
  "Nassau",
  "Okaloosa",
  "Okeechobee",
  "Osceola",
  "Pasco",
  "Polk",
  "Putnam",
  "Santa Rosa",
  "Seminole",
  "St. Johns",
  "St. Lucie",
  "Sumter",
  "Suwannee",
  "Wakulla",
  "Walton",
  "Washington",
]);

// Counties that split their own board: some numbered districts are true
// single-member seats and the rest are countywide. Only the countywide numbers
// belong here. Both verified against the counties' own published descriptions.
const SPLIT_BOARD_DISTRICTS: Readonly<Record<string, Readonly<Record<string, ReadonlySet<string>>>>> = {
  // Districts 1-4 are single-member; 5, 6 and 7 are "Countywide Districts".
  FL: {
    Hillsborough: new Set(["5", "6", "7"]),
    // Inverted from Hillsborough, and easy to get backwards: Pinellas elects
    // districts 4-7 as single-member and 1-3 countywide.
    Pinellas: new Set(["1", "2", "3"]),
  },
};

/**
 * The bare county name from a district row's name — "Anson County, North
 * Carolina" and "St. Tammany Parish, Louisiana" both reduce to the name the
 * tables above are keyed by.
 */
export function toCountyKey(districtName: string | null | undefined): string | null {
  if (typeof districtName !== "string") {
    return null;
  }
  const head = districtName.split(",")[0]?.trim() ?? "";
  const bare = head.replace(/\s+(County|Parish|Borough|Census Area)$/i, "").trim();
  return bare.length > 0 ? bare : null;
}

/**
 * True when this seat is elected by the WHOLE county despite carrying a
 * district number — i.e. when the sub-district flag would be a false alarm.
 *
 * `seatNumber` is the digits from the extracted label ("District 06" -> "06"),
 * used only by the split-board counties; a lettered or named seat simply misses
 * those sets, which is the safe direction for a county that is otherwise
 * districted for real.
 */
export function isCountywideBoardSeat(input: {
  officeCanonicalName: string | null | undefined;
  state: string | null | undefined;
  districtName: string | null | undefined;
  seatNumber?: string | null;
}): boolean {
  const { officeCanonicalName, state } = input;
  if (typeof officeCanonicalName !== "string" || !COUNTY_BOARD_OFFICES.has(officeCanonicalName)) {
    return false;
  }
  if (typeof state !== "string" || state.length === 0) {
    return false;
  }
  const stateKey = state.toUpperCase();

  if (COUNTYWIDE_BOARD_STATES.has(stateKey) && !STATE_RULE_EXEMPT_OFFICES[stateKey]?.has(officeCanonicalName)) {
    return true;
  }

  const county = toCountyKey(input.districtName);
  if (county === null) {
    return false;
  }

  const split = SPLIT_BOARD_DISTRICTS[stateKey]?.[county];
  if (split) {
    // Leading zeros are cosmetic on these ballots ("District 06"), so compare
    // on the parsed integer rather than the raw digits.
    const normalized = input.seatNumber ? String(Number(input.seatNumber)) : null;
    return normalized !== null && split.has(normalized);
  }

  if (stateKey === "NC") {
    return NORTH_CAROLINA_COUNTYWIDE_BOARDS.has(county);
  }
  if (stateKey === "FL") {
    return FLORIDA_COUNTYWIDE_BOARDS.has(county);
  }
  return false;
}

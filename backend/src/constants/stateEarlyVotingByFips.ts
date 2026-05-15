/**
 * Deterministic statewide early-voting summaries keyed by 2-digit state FIPS (50 states + DC).
 * Source reference for all rows:
 * https://www.ncsl.org/elections-and-campaigns/early-in-person-voting
 */
export type DeterministicEarlyVotingRule = {
  available: boolean;
  start: string | null;
  end: string | null;
};

export const STATE_EARLY_VOTING_BY_FIPS: Readonly<Record<string, DeterministicEarlyVotingRule>> = {
  // No early in-person voting window in this source table.
  "01": { available: false, start: null, end: null }, // Alabama
  "28": { available: false, start: null, end: null }, // Mississippi
  "33": { available: false, start: null, end: null }, // New Hampshire

  "02": { available: true, start: "Fifteen days before election", end: "Day of election" },
  "04": { available: true, start: "Twenty-seven days before election", end: "7 p.m. Friday before election" },
  "05": { available: true, start: "Fifteen days before election", end: "5 p.m. Monday before election" },
  "06": {
    available: true,
    start: "Twenty-nine days before election. Note: California conducts elections primarily by mail.",
    end: "Day of election",
  },
  "08": {
    available: true,
    start: "Voter service and polling centers must be open 15 days before an election. Note: Colorado conducts elections primarily by mail.",
    end: "Day of election",
  },
  "09": { available: true, start: "Fifteen days before election", end: "Two days before election" },
  "10": { available: true, start: "Ten days before an election", end: "Sunday before election" },
  "11": {
    available: true,
    start: "Twelve days before election. Note: D.C. conducts elections primarily by mail.",
    end: "Saturday before election",
  },
  "12": {
    available: true,
    start: "Ten days before election. May be offered 11 to 15 days before an election that contains state and federal races, at the discretion of the elections supervisor.",
    end: "Three days before election. May end two days before an election that contains state and federal races, at the discretion of the elections supervisor.",
  },
  "13": {
    available: true,
    start: "Fourth Monday prior to a primary or election; as soon as possible prior to a runoff",
    end: "Friday immediately prior to a primary, election or runoff",
  },
  "15": {
    available: true,
    start: "Ten business days prior to Election Day. Note: Hawaii conducts elections primarily by mail.",
    end: "7 p.m. on Election Day",
  },
  "16": {
    available: true,
    start: "On or after the fourth Monday before election (in-person absentee)",
    end: "5 p.m., Friday before election",
  },
  "17": {
    available: true,
    start: "Fortieth day before election for temporary polling locations and 15th day before election for permanent locations",
    end: "End of the day before election day",
  },
  "18": { available: true, start: "Twenty-eight days before election (in-person absentee)", end: "Noon, day before election" },
  "19": { available: true, start: "Twenty days before election (in-person absentee)", end: "5 p.m., day before election" },
  "20": {
    available: true,
    start: "Twenty days before election or Tuesday before election (varies by county)",
    end: "Noon, day before election",
  },
  "21": { available: true, start: "Thursday before election", end: "Saturday before election" },
  "22": { available: true, start: "Fourteen days before election", end: "Seven days before election" },
  "23": {
    available: true,
    start: "In-person absentee voting available as soon as absentee ballots are ready (30-45 days before election)",
    end: "Three business days before election, unless the voter has an acceptable excuse",
  },
  "24": { available: true, start: "On the second Thursday before an election", end: "Thursday before an election" },
  "25": {
    available: true,
    start: "Seventeen days before election for state biennial elections; 10 days before election for presidential or state primaries",
    end: "Two business days before an election",
  },
  "26": { available: true, start: "Nine days before an election", end: "Sunday before an election" },
  "27": { available: true, start: "Forty-six days before election (in-person absentee)", end: "5 p.m. the day before election" },
  "29": { available: true, start: "The second Tuesday before an election (in-person absentee)", end: "Not specified" },
  "30": { available: true, start: "Thirty days before election (in-person absentee)", end: "Day before election" },
  "31": { available: true, start: "Thirty days before each election", end: "Election Day" },
  "32": {
    available: true,
    start: "Third Saturday preceding election. Note: Nevada conducts elections primarily by mail.",
    end: "Friday before election",
  },
  "34": { available: true, start: "Ten days before the election", end: "Second calendar day before election" },
  "35": {
    available: true,
    start: "Twenty-eight days before an election at a clerk's office; on the third Saturday before an election for alternate locations",
    end: "Saturday before election",
  },
  "36": { available: true, start: "Tenth day before election", end: "Second day before an election" },
  "37": { available: true, start: "Third Thursday before election", end: "3 p.m. on the last Saturday before election" },
  "38": { available: true, start: "Fifteen days before election", end: "Day before election" },
  "39": {
    available: true,
    start: "Twenty-nine days before election (the day after voter registration closes)",
    end: "5 p.m. on the Sunday before the election",
  },
  "40": { available: true, start: "Wednesday preceding an election (in-person absentee)", end: "2 p.m. on the Saturday before election" },
  "41": {
    available: true,
    start: "Drop sites must open the Friday before an election, but may open as soon as ballots are available (18 days before). Note: Oregon conducts elections primarily by mail.",
    end: "Day of election",
  },
  "42": {
    available: true,
    start: "Counties will begin preparing in-person mail/absentee ballots after the official candidate list is certified, no earlier than 50 days before the election. The timing of availability of ballots may vary by county",
    end: "5 p.m. first Tuesday prior to day of election",
  },
  "44": { available: true, start: "Twenty days before election", end: "Day before election" },
  "45": { available: true, start: "Two weeks before Election Day", end: "Day before election" },
  "46": { available: true, start: "Forty-six days before election (in-person absentee)", end: "5 p.m. the day before the election" },
  "47": {
    available: true,
    start: "Twenty days before election",
    end: "Five days before election (seven days for a presidential preference primary)",
  },
  "48": {
    available: true,
    start: "Seventeen days before election. Note: TX SB 2753 changes the early voting period to 12 days before election day, but it is not effective until the secretary of state publishes a report required by the bill.",
    end: "Day before election",
  },
  "49": {
    available: true,
    start: "Fourteen days before election. Note: Utah conducts elections primarily by mail.",
    end: "Friday before election, though an election official may choose to extend the early voting period to the day before the election",
  },
  "50": {
    available: true,
    start: "Forty-five days before election. Note: Vermont conducts general elections primarily by mail.",
    end: "5 p.m. day before election",
  },
  "51": { available: true, start: "Forty-five days before election", end: "5 p.m. Saturday before election" },
  "53": {
    available: true,
    start: "Eighteen days before an election. Note: Washington conducts elections primarily by mail.",
    end: "8 p.m. on day of election",
  },
  "54": { available: true, start: "Thirteen days before election", end: "Three days before election" },
  "55": { available: true, start: "Fourteen days preceding the election (in-person absentee)", end: "Sunday preceding the election" },
  "56": { available: true, start: "Twenty-eight days before election (in-person absentee)", end: "Day before election" },
} as const;

export function getDeterministicEarlyVotingByFips(stateFips: string): DeterministicEarlyVotingRule | null {
  return STATE_EARLY_VOTING_BY_FIPS[stateFips] ?? null;
}

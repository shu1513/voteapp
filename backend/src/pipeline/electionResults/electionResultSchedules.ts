import type { Pool, PoolClient } from "pg";

import { getStateNameByAbbreviation } from "../../constants/usStates.js";
import type { ElectionResultPassType } from "../../types/electionResults.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CertifiedScheduleStrategy = "offset_days" | "fallback_offset_days";

export type StateElectionResultSchedule = {
  state: string;
  stateName: string;
  electionNight: {
    localTime: string;
    timeZone: string;
    notes: string;
  };
  certified: {
    strategy: CertifiedScheduleStrategy;
    offsetDays: number;
    localTime: string;
    timeZone: string;
    notes: string;
  };
};

type ScheduleInput = Omit<StateElectionResultSchedule, "stateName">;

const DEFAULT_CERTIFIED_LOCAL_TIME = "10:00";
const DEFAULT_FALLBACK_CERTIFIED_OFFSET_DAYS = 45;

function schedule(input: ScheduleInput): StateElectionResultSchedule {
  const state = input.state.trim().toUpperCase();
  const stateName = getStateNameByAbbreviation(state);
  if (!stateName) {
    throw new Error(`Unknown election result schedule state: ${input.state}`);
  }
  return { ...input, state, stateName };
}

function certifiedOffset(
  offsetDays: number,
  timeZone: string,
  notes: string,
  localTime = DEFAULT_CERTIFIED_LOCAL_TIME
): StateElectionResultSchedule["certified"] {
  return { strategy: "offset_days", offsetDays, localTime, timeZone, notes };
}

function certifiedFallback(
  timeZone: string,
  notes: string,
  offsetDays = DEFAULT_FALLBACK_CERTIFIED_OFFSET_DAYS,
  localTime = DEFAULT_CERTIFIED_LOCAL_TIME
): StateElectionResultSchedule["certified"] {
  return { strategy: "fallback_offset_days", offsetDays, localTime, timeZone, notes };
}

export const ELECTION_RESULT_SCHEDULES_BY_STATE: Record<string, StateElectionResultSchedule> = Object.fromEntries(
  [
    schedule({
      state: "AL",
      electionNight: { localTime: "19:10", timeZone: "America/Chicago", notes: "7:10 p.m. Central time." },
      certified: certifiedOffset(10, "America/Chicago", "Counties generally canvass on the second Friday after the election; first certified check uses E+10."),
    }),
    schedule({
      state: "AK",
      electionNight: { localTime: "21:10", timeZone: "America/Anchorage", notes: "9:10 p.m. Alaska time." },
      certified: certifiedFallback("America/Anchorage", "State ballot-counting review has no simple fixed day in the source table; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "AZ",
      electionNight: { localTime: "19:10", timeZone: "America/Phoenix", notes: "7:10 p.m. Arizona time." },
      certified: certifiedOffset(20, "America/Phoenix", "By the third Monday after the election; for Tuesday elections this is about E+20."),
    }),
    schedule({
      state: "AR",
      electionNight: { localTime: "19:40", timeZone: "America/Chicago", notes: "7:40 p.m. Central time." },
      certified: certifiedOffset(25, "America/Chicago", "Certification varies by office; first certified check uses conservative E+25."),
    }),
    schedule({
      state: "CA",
      electionNight: { localTime: "20:10", timeZone: "America/Los_Angeles", notes: "8:10 p.m. Pacific time." },
      certified: certifiedOffset(38, "America/Los_Angeles", "E+30 county/local; E+38 statewide for most offices; first statewide-safe check uses E+38."),
    }),
    schedule({
      state: "CO",
      electionNight: { localTime: "19:10", timeZone: "America/Denver", notes: "7:10 p.m. Mountain time." },
      certified: certifiedOffset(27, "America/Denver", "State canvass/recount determination around E+27."),
    }),
    schedule({
      state: "CT",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedFallback("America/New_York", "Certification timing varies by office; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "DE",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(2, "America/New_York", "County canvass is generally at 10:00 a.m. on E+2; state declaration follows for major offices."),
    }),
    schedule({
      state: "DC",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedFallback("America/New_York", "District of Columbia certification timing not listed in the provided table; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "FL",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern / 7:10 p.m. Central statewide-safe instant." },
      certified: certifiedOffset(14, "America/New_York", "Generally E+14."),
    }),
    schedule({
      state: "GA",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern time." },
      certified: certifiedOffset(18, "America/New_York", "E+17 for state offices/Congress; E+18 for presidential electors; first statewide-safe check uses E+18."),
    }),
    schedule({
      state: "HI",
      electionNight: { localTime: "19:10", timeZone: "Pacific/Honolulu", notes: "7:10 p.m. Hawaii time." },
      certified: certifiedOffset(30, "Pacific/Honolulu", "Usually after election-contest period; presidential by last day of November; first check uses E+30."),
    }),
    schedule({
      state: "ID",
      electionNight: { localTime: "21:10", timeZone: "America/Denver", notes: "9:10 p.m. Mountain / 8:10 p.m. Pacific statewide-safe instant." },
      certified: certifiedOffset(21, "America/Denver", "State canvass within E+21."),
    }),
    schedule({
      state: "IL",
      electionNight: { localTime: "19:10", timeZone: "America/Chicago", notes: "7:10 p.m. Central time." },
      certified: certifiedOffset(31, "America/Chicago", "Up to E+31 for most offices."),
    }),
    schedule({
      state: "IN",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern / 6:10 p.m. Central statewide-safe instant." },
      certified: certifiedFallback("America/New_York", "Many certificates by first Tuesday in December; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "IA",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central time." },
      certified: certifiedOffset(27, "America/Chicago", "Generally E+27."),
    }),
    schedule({
      state: "KS",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central / 7:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedFallback("America/Chicago", "State canvassing board meets by December 1; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "KY",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern / 6:10 p.m. Central statewide-safe instant." },
      certified: certifiedOffset(20, "America/New_York", "By the third Monday after election; for Tuesday elections this is about E+20."),
    }),
    schedule({
      state: "LA",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central time." },
      certified: certifiedOffset(30, "America/Chicago", "Within E+30."),
    }),
    schedule({
      state: "ME",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(20, "America/New_York", "Within E+20."),
    }),
    schedule({
      state: "MD",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(35, "America/New_York", "State board convenes within E+35."),
    }),
    schedule({
      state: "MA",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(16, "America/New_York", "Not before 5:00 p.m. on E+15 for many offices; first check uses E+16."),
    }),
    schedule({
      state: "MI",
      electionNight: { localTime: "21:10", timeZone: "America/New_York", notes: "9:10 p.m. Eastern / 8:10 p.m. Central statewide-safe instant." },
      certified: certifiedOffset(20, "America/New_York", "State canvass completed by E+20."),
    }),
    schedule({
      state: "MN",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central time." },
      certified: certifiedOffset(21, "America/Chicago", "Presidential electors canvassed third Tuesday after election; first check uses E+21."),
    }),
    schedule({
      state: "MS",
      electionNight: { localTime: "19:10", timeZone: "America/Chicago", notes: "7:10 p.m. Central time." },
      certified: certifiedOffset(30, "America/Chicago", "Up to E+30 for most non-legislative offices."),
    }),
    schedule({
      state: "MO",
      electionNight: { localTime: "19:10", timeZone: "America/Chicago", notes: "7:10 p.m. Central time." },
      certified: certifiedFallback("America/Chicago", "By the second Tuesday in December for most offices; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "MT",
      electionNight: { localTime: "20:10", timeZone: "America/Denver", notes: "8:10 p.m. Mountain time." },
      certified: certifiedOffset(27, "America/Denver", "Within E+27."),
    }),
    schedule({
      state: "NE",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central / 7:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedOffset(40, "America/Chicago", "Certificates within E+40; state board meets fourth Monday after election."),
    }),
    schedule({
      state: "NV",
      electionNight: { localTime: "19:10", timeZone: "America/Los_Angeles", notes: "7:10 p.m. Pacific time." },
      certified: certifiedOffset(28, "America/Los_Angeles", "State canvass on fourth Tuesday of November; first check uses E+28."),
    }),
    schedule({
      state: "NH",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(14, "America/New_York", "After recount/appeal period expires; first check uses E+14."),
    }),
    schedule({
      state: "NJ",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(30, "America/New_York", "State board meets no later than E+30."),
    }),
    schedule({
      state: "NM",
      electionNight: { localTime: "19:10", timeZone: "America/Denver", notes: "7:10 p.m. Mountain time." },
      certified: certifiedOffset(31, "America/Denver", "Certificates no earlier than E+31."),
    }),
    schedule({
      state: "NY",
      electionNight: { localTime: "21:10", timeZone: "America/New_York", notes: "9:10 p.m. Eastern time." },
      certified: certifiedOffset(25, "America/New_York", "Timing varies; counties transmit certified statements by about E+25."),
    }),
    schedule({
      state: "NC",
      electionNight: { localTime: "19:40", timeZone: "America/New_York", notes: "7:40 p.m. Eastern time." },
      certified: certifiedOffset(31, "America/New_York", "State board meeting by about E+31; protests can delay."),
    }),
    schedule({
      state: "ND",
      electionNight: { localTime: "22:10", timeZone: "America/Chicago", notes: "10:10 p.m. Central / 9:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedOffset(17, "America/Chicago", "State canvassing board meets no later than E+17, possible short adjournment."),
    }),
    schedule({
      state: "OH",
      electionNight: { localTime: "19:40", timeZone: "America/New_York", notes: "7:40 p.m. Eastern time." },
      certified: certifiedOffset(81, "America/New_York", "County canvass by E+21; outer final-amendment rule can extend to E+81, so first final-safe check uses E+81."),
    }),
    schedule({
      state: "OK",
      electionNight: { localTime: "19:10", timeZone: "America/Chicago", notes: "7:10 p.m. Central time." },
      certified: certifiedOffset(7, "America/Chicago", "Tuesday after the election after 5:00 p.m., usually E+7."),
    }),
    schedule({
      state: "OR",
      electionNight: { localTime: "20:10", timeZone: "America/Los_Angeles", notes: "8:10 p.m. Pacific / 9:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedOffset(30, "America/Los_Angeles", "Generally E+30."),
    }),
    schedule({
      state: "PA",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedFallback("America/New_York", "No fixed state completion deadline after certified county returns; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "RI",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern time." },
      certified: certifiedOffset(14, "America/New_York", "No fixed state deadline; recount petition period is seven days, first check uses E+14."),
    }),
    schedule({
      state: "SC",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern time." },
      certified: certifiedOffset(25, "America/New_York", "State board convenes within E+10 and may adjourn up to 15 days; first check uses E+25."),
    }),
    schedule({
      state: "SD",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central / 7:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedOffset(17, "America/Chicago", "State board convenes within E+7 and may adjourn up to 10 days; first check uses E+17."),
    }),
    schedule({
      state: "TN",
      electionNight: { localTime: "20:10", timeZone: "America/New_York", notes: "8:10 p.m. Eastern / 7:10 p.m. Central statewide-safe instant." },
      certified: certifiedOffset(21, "America/New_York", "Counties certify by the third Monday after election; first check uses about E+21."),
    }),
    schedule({
      state: "TX",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central / 7:10 p.m. Mountain statewide-safe instant." },
      certified: certifiedOffset(33, "America/Chicago", "Most statewide offices canvassed between E+18 and E+33; first check uses E+33."),
    }),
    schedule({
      state: "UT",
      electionNight: { localTime: "20:10", timeZone: "America/Denver", notes: "8:10 p.m. Mountain time." },
      certified: certifiedFallback("America/Denver", "State board convenes fourth Monday of November with no fixed completion deadline; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "VT",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern time." },
      certified: certifiedOffset(7, "America/New_York", "Canvassing committees meet one week after election and may recess until complete."),
    }),
    schedule({
      state: "VA",
      electionNight: { localTime: "19:10", timeZone: "America/New_York", notes: "7:10 p.m. Eastern time." },
      certified: certifiedFallback("America/New_York", "State board meets on first Monday in December; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "WA",
      electionNight: { localTime: "20:10", timeZone: "America/Los_Angeles", notes: "8:10 p.m. Pacific time." },
      certified: certifiedOffset(30, "America/Los_Angeles", "Generally E+30."),
    }),
    schedule({
      state: "WV",
      electionNight: { localTime: "19:40", timeZone: "America/New_York", notes: "7:40 p.m. Eastern time." },
      certified: certifiedOffset(30, "America/New_York", "Certificates transmitted within E+30, or later after recount."),
    }),
    schedule({
      state: "WI",
      electionNight: { localTime: "20:10", timeZone: "America/Chicago", notes: "8:10 p.m. Central time." },
      certified: certifiedFallback("America/Chicago", "By December 1; fallback first certified check uses E+45."),
    }),
    schedule({
      state: "WY",
      electionNight: { localTime: "19:10", timeZone: "America/Denver", notes: "7:10 p.m. Mountain time." },
      certified: certifiedOffset(8, "America/Denver", "By the second Wednesday after Tuesday election; first check uses E+8."),
    }),
  ].map((entry) => [entry.state, entry])
);

export function getElectionResultScheduleForState(state: string): StateElectionResultSchedule | null {
  const normalized = state.trim().toUpperCase();
  return ELECTION_RESULT_SCHEDULES_BY_STATE[normalized] ?? null;
}

export function getRequiredElectionResultScheduleForState(state: string): StateElectionResultSchedule {
  const schedule = getElectionResultScheduleForState(state);
  if (!schedule) {
    throw new Error(`Missing election result schedule for state: ${state}`);
  }
  return schedule;
}

export function getCertifiedOffsetDaysForState(state: string): number {
  return getRequiredElectionResultScheduleForState(state).certified.offsetDays;
}

export async function computeLocalElectionResultScheduledAtUtc(
  db: Queryable,
  electionDate: string,
  offsetDays: number,
  localTime: string,
  timeZone: string
): Promise<Date> {
  const result = await db.query<{ scheduled_at: Date }>(
    `
      SELECT ((($1::date + $2::int) + $3::time) AT TIME ZONE $4)::timestamptz AS scheduled_at
    `,
    [electionDate, offsetDays, localTime, timeZone]
  );
  const scheduledAt = result.rows[0]?.scheduled_at;
  if (!(scheduledAt instanceof Date) || Number.isNaN(scheduledAt.getTime())) {
    throw new Error(
      `Failed to compute election result scheduled time for date=${electionDate} offsetDays=${offsetDays} localTime=${localTime} timeZone=${timeZone}`
    );
  }
  return scheduledAt;
}

export async function computeElectionResultScheduledAtUtc(
  db: Queryable,
  input: { state: string; electionDate: string; passType: ElectionResultPassType }
): Promise<Date> {
  const schedule = getRequiredElectionResultScheduleForState(input.state);
  if (input.passType === "election_night") {
    return computeLocalElectionResultScheduledAtUtc(
      db,
      input.electionDate,
      0,
      schedule.electionNight.localTime,
      schedule.electionNight.timeZone
    );
  }

  return computeLocalElectionResultScheduledAtUtc(
    db,
    input.electionDate,
    schedule.certified.offsetDays,
    schedule.certified.localTime,
    schedule.certified.timeZone
  );
}

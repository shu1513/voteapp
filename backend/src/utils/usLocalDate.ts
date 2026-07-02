// "Today" for user-facing upcoming/past election checks.
//
// The database runs in UTC (or the host default), so CURRENT_DATE flips to the
// next day while voters in the western United States are still voting — an
// election looked "past" in Hawaii from early afternoon on election day. All
// elections in this app are US contests, so the correct boundary is the last
// US clock to leave a given date: Pacific/Honolulu (UTC-10, no daylight
// saving). An election date counts as past only once the entire United States
// has finished that day.
//
// This is a static SQL fragment, never interpolated with user input.
export const US_LATEST_LOCAL_DATE_SQL = "(now() AT TIME ZONE 'Pacific/Honolulu')::date";

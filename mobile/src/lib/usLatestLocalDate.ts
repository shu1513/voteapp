// Election dates are YYYY-MM-DD calendar strings; "today" is the last US
// clock still on a given date — Pacific/Honolulu, UTC-10, no DST — mirroring
// the backend's US_LATEST_LOCAL_DATE_SQL: an election counts as past only
// once the entire United States has finished that day. en-CA formats as
// YYYY-MM-DD. Same logic as the web's usLatestLocalDate.
export function usLatestLocalDate(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Pacific/Honolulu" }).format(new Date());
}

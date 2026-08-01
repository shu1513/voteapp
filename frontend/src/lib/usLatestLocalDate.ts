// Election dates are YYYY-MM-DD calendar strings; "today" is the last US
// clock still on a given date — Pacific/Honolulu, UTC-10, no DST — mirroring
// the backend's US_LATEST_LOCAL_DATE_SQL (usLocalDate.ts): an election
// counts as past only once the entire United States has finished that day.
// Fixing the timezone also makes the value identical on the SSR host and in
// the viewer's browser, so ongoing/past classification cannot flip during
// hydration. en-CA formats as YYYY-MM-DD.
export function usLatestLocalDate(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Pacific/Honolulu" }).format(new Date());
}

/**
 * Heuristic check for URLs that likely point to polling-place lookup resources.
 */
export function isLikelyPollingPlaceUrl(url: string): boolean {
  const trimmed = url.trim();
  if (trimmed.length === 0) {
    return false;
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }
  } catch {
    return false;
  }

  const lower = parsed.toString().toLowerCase();
  const hasPollingSignal =
    /poll|polling|polling-place|find-your-polling-place|find-my-polling-place|voterlookup|pollfinder|poll-site/.test(
      lower
    );
  const hasNonPollingSignal =
    /register|registration|absentee|mail[-\s]?ballot|vote[-\s]?by[-\s]?mail|voter-id|id-laws|identification/.test(
      lower
    );

  if (hasPollingSignal) {
    // Polling signal intentionally wins even if non-polling terms also appear.
    // We prefer recall over precision for this heuristic.
    return true;
  }

  if (hasNonPollingSignal) {
    return false;
  }

  const host = parsed.hostname.toLowerCase();
  const path = parsed.pathname.toLowerCase();

  if (host.includes("sos.") || host.startsWith("elections.") || host.includes(".elections.")) {
    return true;
  }

  if (/poll|locator/.test(path) && host.endsWith(".gov")) {
    return true;
  }

  return false;
}

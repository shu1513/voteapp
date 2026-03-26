/**
 * Heuristic check for URLs that likely point to polling-place lookup resources.
 */
export function isLikelyPollingPlaceUrl(url: string): boolean {
  const lower = url.trim().toLowerCase();
  if (lower.length === 0) {
    return false;
  }

  const hasPollingSignal =
    /poll|polling|polling-place|find-your-polling-place|find-my-polling-place|voterlookup|pollfinder|poll-site/.test(
      lower
    );
  const hasNonPollingSignal =
    /register|registration|absentee|mail[-\s]?ballot|vote[-\s]?by[-\s]?mail|voter-id|id-laws|identification/.test(
      lower
    );

  if (hasPollingSignal) {
    return true;
  }

  if (hasNonPollingSignal) {
    return false;
  }

  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    const path = parsed.pathname.toLowerCase();

    if (host.includes("sos.") || host.startsWith("elections.") || host.includes(".elections.")) {
      return true;
    }

    if (/poll|locator/.test(path) && host.endsWith(".gov")) {
      return true;
    }
  } catch {
    return false;
  }

  return false;
}

// In-memory handoff of the geocoder's matched address from the home screen
// to the ballot screen. Deliberately NOT a navigation param: Expo Router
// serializes params into its URL-based navigation state even on native, and
// a home address must never reach deep-link handling or diagnostics. Module
// state is lost on reload — the ballot screen simply omits the confirmation
// line then, same as the web after a refresh (router state there).

export type MatchedAddressHandoff = {
  address: string;
  // Geocoder candidate count; above 1 means the search was ambiguous and the
  // ballot is for the first match, so the ballot screen shows a warning.
  matchCount: number;
  // Which search produced the ballot; the partial banner names a "zip"
  // search's ZIP or a "region" search's area, and stays generic without it.
  scope: "exact" | "zip" | "region";
};

let pendingMatchedAddress: MatchedAddressHandoff | null = null;

export function setMatchedAddress(address: string, matchCount: number, scope: MatchedAddressHandoff["scope"]): void {
  pendingMatchedAddress = { address, matchCount, scope };
}

/** Returns and clears the pending handoff (single use). */
export function consumeMatchedAddress(): MatchedAddressHandoff | null {
  const handoff = pendingMatchedAddress;
  pendingMatchedAddress = null;
  return handoff;
}

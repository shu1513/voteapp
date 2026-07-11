// In-memory handoff of the geocoder's matched address from the home screen
// to the ballot screen. Deliberately NOT a navigation param: Expo Router
// serializes params into its URL-based navigation state even on native, and
// a home address must never reach deep-link handling or diagnostics. Module
// state is lost on reload — the ballot screen simply omits the confirmation
// line then, same as the web after a refresh (router state there).

let pendingMatchedAddress: string | null = null;

export function setMatchedAddress(address: string): void {
  pendingMatchedAddress = address;
}

/** Returns and clears the pending address (single handoff). */
export function consumeMatchedAddress(): string | null {
  const address = pendingMatchedAddress;
  pendingMatchedAddress = null;
  return address;
}

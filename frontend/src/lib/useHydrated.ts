import { useSyncExternalStore } from "react";

// True only after React has hydrated the server HTML. Gate any render read
// of client-only state behind this — location.state especially: it looks
// navigation-scoped, but the browser persists it in history.state, so a
// reload of an in-app-navigated entry restores it while the SSR pass for
// that same document rendered with null. Reading it during the hydration
// render therefore mismatches the server HTML and fails hydration. The
// server snapshot (false) keeps the first client render identical to SSR;
// the flip to true re-renders with the client-only state one paint later.
const emptySubscribe = () => () => {};

export function useHydrated(): boolean {
  return useSyncExternalStore(
    emptySubscribe,
    () => true,
    () => false
  );
}

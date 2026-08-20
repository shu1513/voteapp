import { useQuery } from "@tanstack/react-query";
import { apiRequest, useMe } from "@voteapp/api-client";
import { useBallotDraft } from "./ballotDraft";

// The viewer's own district ids, for gating "Make my pick" to races the
// viewer can actually vote in (docs/plans/pick-district-gate.md). Guests
// read the ballot draft's district context (set on /ballot?d=… visits);
// verified users fetch the saved-address districts. Lives in frontend (not
// @voteapp/api-client) because the guest half reads localStorage via
// ballotDraft.ts, which the shared package cannot depend on.

export function useMyDistricts(): {
  /** The viewer's district ids once known; undefined = unknown (no saved
   * address, guest without ballot context, unverified account, or fetch
   * failure) — pages show the address nudge instead of pick controls. */
  districtIds: Set<string> | undefined;
  /** True only while "what are the viewer's districts?" is not yet knowable
   * (session loading, or the verified fetch pending). Pages render neither
   * pick controls nor the nudge until this settles — the same no-flash rule
   * as the follow button. */
  isLoading: boolean;
} {
  const { me, isLoading: meLoading } = useMe();
  const draft = useBallotDraft();
  // Verified only, like useMyResearchAreas: the endpoint is verified-email-
  // gated (district ids are personal location data), so an unverified
  // account never fetches and settles as unknown.
  const enabled = me?.email_verified === true;
  const query = useQuery({
    queryKey: ["me", "districts"],
    queryFn: () => apiRequest<{ district_ids: string[] }>("/api/me/districts"),
    enabled,
    staleTime: 60_000,
  });
  const isLoading = enabled ? query.isPending : meLoading;
  // Guest districts come from the draft; logged-in from the endpoint. An
  // empty list means "no address on file yet" — unknown, not "no districts"
  // (a real ballot always has at least one).
  const ids = me === null ? draft.district_ids : (query.data?.district_ids ?? []);
  const districtIds = !isLoading && ids.length > 0 ? new Set(ids) : undefined;
  return { districtIds, isLoading };
}

import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "./client";
import { useMe } from "./useMe";

/**
 * The account's saved district ids, for gating "Make my pick" to races the
 * viewer can actually vote in (docs/plans/pick-district-gate.md). Account
 * side only: the web's useMyDistricts wraps this with its localStorage guest
 * draft (which this package cannot depend on); mobile consumes it directly —
 * mobile guests get no pick controls at all.
 */
export function useMyAccountDistricts(): {
  /** The viewer's district ids once known; undefined = unknown (no saved
   * address, guest, unverified account, or fetch failure) — pages show the
   * address nudge instead of pick controls. */
  districtIds: Set<string> | undefined;
  /** True only while "what are the viewer's districts?" is not yet knowable
   * (session loading, or the verified fetch pending). Pages render neither
   * pick controls nor the nudge until this settles — the same no-flash rule
   * as the follow button. */
  isLoading: boolean;
} {
  const { me, isLoading: meLoading } = useMe();
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
  // An empty list means "no address on file yet" — unknown, not "no
  // districts" (a real ballot always has at least one).
  const ids = query.data?.district_ids ?? [];
  const districtIds = !isLoading && ids.length > 0 ? new Set(ids) : undefined;
  return { districtIds, isLoading };
}

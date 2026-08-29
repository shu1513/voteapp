import { useMe, useMyAccountDistricts } from "@voteapp/api-client";
import { useBallotDraft } from "./ballotDraft";

// The viewer's own district ids, for gating "Make my pick" to races the
// viewer can actually vote in (docs/plans/pick-district-gate.md). The
// account half (the verified ["me", "districts"] fetch) is shared with
// mobile via @voteapp/api-client; this wrapper adds the guest half — the
// ballot draft's district context (set on /ballot?d=… visits) — which stays
// web-local because it reads localStorage via ballotDraft.ts, and the
// shared package cannot depend on that.

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
  const { me } = useMe();
  const draft = useBallotDraft();
  const { districtIds: accountDistrictIds, isLoading } = useMyAccountDistricts();
  // Guest districts come from the draft (empty = unknown, not "no
  // districts"); logged-in from the shared account hook.
  const districtIds =
    me === null
      ? draft.district_ids.length > 0
        ? new Set(draft.district_ids)
        : undefined
      : accountDistrictIds;
  return { districtIds, isLoading };
}

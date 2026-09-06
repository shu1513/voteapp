import { ApiError, useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { trackSettled } from "../lib/usage";

// Follow/unfollow toggle. Rendered only for verified users (callers gate on
// useFollows().canFollow); new follows default to both notification kinds on,
// matching the backend's defaults.

type FollowButtonProps = {
  candidateId: string;
  isFollowing: boolean;
  size?: "sm" | "md";
};

export function FollowButton({ candidateId, isFollowing, size = "md" }: FollowButtonProps) {
  const setFollow = useSetFollow();
  const saving = useFollowSaving();
  const base =
    size === "sm"
      ? "rounded-lg px-3 py-1 text-xs font-semibold transition"
      : "rounded-lg px-4 py-2 text-sm font-semibold transition";

  // Surface follow failures (notably the follow limit, a 4xx with a
  // user-readable server message) instead of a button that silently does
  // nothing. Cleared implicitly: the next mutate resets isError.
  const errorMessage = setFollow.isError
    ? setFollow.error instanceof ApiError && setFollow.error.status < 500
      ? setFollow.error.message
      : "Something went wrong. Please try again."
    : null;

  function toggle() {
    // mutateAsync (not mutate + per-call callbacks): the usage outcome must
    // land even if this button unmounts before the server answers. The
    // rejection is observed by the hook's own isError; swallow it here.
    const request = setFollow.mutateAsync({ candidate_id: candidateId, following: !isFollowing });
    trackSettled(request, "follow_result", { change: isFollowing ? "unfollow" : "follow" });
    request.catch(() => undefined);
  }

  return (
    <div>
      <button
        type="button"
        disabled={saving}
        onClick={toggle}
        // Instagram's pattern: solid blue to follow, quiet grey once
        // following (the click still unfollows). Not the brand red — the
        // header's Sign up already owns it, and two red buttons dilute both.
        className={
          isFollowing
            ? `${base} bg-gray-100 text-ink hover:bg-gray-200`
            : `${base} bg-[#0095f6] text-white hover:bg-[#1877f2]`
        }
      >
        {setFollow.isPending ? "…" : isFollowing ? "Following" : "Follow"}
      </button>
      {errorMessage ? (
        // role="alert": announced to assistive technology when it appears.
        <p role="alert" className="mt-1 max-w-56 text-xs text-rausch-dark">
          {errorMessage}
        </p>
      ) : null}
    </div>
  );
}

import { ApiError, useFollowSaving, useSetFollow } from "@voteapp/api-client";

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

  return (
    <div>
      <button
        type="button"
        disabled={saving}
        onClick={() => setFollow.mutate({ candidate_id: candidateId, following: !isFollowing })}
        className={
          isFollowing
            ? `${base} border border-line bg-white text-ink hover:border-rausch`
            : `${base} bg-rausch text-white hover:bg-rausch-dark`
        }
      >
        {setFollow.isPending ? "…" : isFollowing ? "Unfollow" : "Follow"}
      </button>
      {errorMessage ? <p className="mt-1 max-w-56 text-xs text-rausch-dark">{errorMessage}</p> : null}
    </div>
  );
}

import { ApiError, useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { Alert, Pressable, Text } from "react-native";
import { registerForPushRequestingPermission } from "../lib/pushNotifications";

// Follow/unfollow toggle. Rendered only for verified users (callers gate on
// useFollows().canFollow); new follows default to both notification kinds on,
// matching the backend's defaults. Port of the web component.
//
// A successful follow is one of the two moments the push permission prompt
// is allowed to appear (the other: saving a ballot) — the user just asked to
// be notified about someone, so the ask is in context, per the plan's
// "not on launch" rule.

type FollowButtonProps = {
  candidateId: string;
  isFollowing: boolean;
  size?: "sm" | "md";
};

export function FollowButton({ candidateId, isFollowing, size = "md" }: FollowButtonProps) {
  const setFollow = useSetFollow();
  const saving = useFollowSaving();
  const box = size === "sm" ? "rounded-lg px-3 py-1" : "rounded-lg px-4 py-2";
  const text = size === "sm" ? "text-xs font-semibold" : "text-sm font-semibold";

  return (
    <Pressable
      disabled={saving}
      onPress={() =>
        setFollow.mutate(
          { candidate_id: candidateId, following: !isFollowing },
          {
            onSuccess: () => {
              if (!isFollowing) {
                void registerForPushRequestingPermission();
              }
            },
            // Surface follow failures (notably the follow limit, a 4xx with a
            // user-readable server message) — a silent no-op button reads as
            // broken. Mirrors the web component's error line.
            onError: (error) => {
              Alert.alert(
                "Could not save",
                error instanceof ApiError && error.status < 500
                  ? error.message
                  : "Something went wrong. Please try again."
              );
            },
          }
        )
      }
      accessibilityRole="button"
      accessibilityState={{ selected: isFollowing, disabled: saving }}
      className={
        isFollowing ? `${box} border border-line bg-white active:border-rausch` : `${box} bg-rausch active:bg-rausch-dark`
      }
    >
      <Text className={isFollowing ? `${text} text-ink` : `${text} text-white`}>
        {setFollow.isPending ? "…" : isFollowing ? "Following" : "Follow"}
      </Text>
    </Pressable>
  );
}

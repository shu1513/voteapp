import { ApiError, useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { Alert, Pressable, Text } from "react-native";
import { registerForPushRequestingPermission } from "../lib/pushNotifications";

// Follow/unfollow toggle. Rendered only for verified users (callers gate on
// useFollows().canFollow); new follows default to both notification kinds on,
// matching the backend's defaults. Port of the web component.
//
// X's pattern once following: the button reads "Following" in quiet grey,
// and a tap asks before it unfollows — one accidental tap should not undo a
// follow. The web previews a red "Unfollow" on hover; phones have no hover,
// so the confirm alert carries the red (destructive) action instead.
//
// A successful follow is one of the two moments the push permission prompt
// is allowed to appear (the other: saving a ballot) — the user just asked to
// be notified about someone, so the ask is in context, per the plan's
// "not on launch" rule.

type FollowButtonProps = {
  candidateId: string;
  candidateName: string;
  isFollowing: boolean;
  size?: "sm" | "md";
};

export function FollowButton({ candidateId, candidateName, isFollowing, size = "md" }: FollowButtonProps) {
  const setFollow = useSetFollow();
  const saving = useFollowSaving();
  const box = size === "sm" ? "rounded-lg px-3 py-1" : "rounded-lg px-4 py-2";
  const text = size === "sm" ? "text-xs font-semibold" : "text-sm font-semibold";

  function submit(following: boolean) {
    setFollow.mutate(
      { candidate_id: candidateId, following },
      {
        onSuccess: () => {
          if (following) {
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
    );
  }

  function confirmUnfollow() {
    Alert.alert(`Unfollow ${candidateName}?`, "You'll stop getting updates about them.", [
      { text: "Cancel", style: "cancel" },
      { text: "Unfollow", style: "destructive", onPress: () => submit(false) },
    ]);
  }

  return (
    <Pressable
      disabled={saving}
      onPress={() => (isFollowing ? confirmUnfollow() : submit(true))}
      accessibilityRole="button"
      accessibilityState={{ selected: isFollowing, disabled: saving }}
      // Instagram blue to follow, not the brand red — rausch is the CTA
      // color elsewhere, and two red buttons dilute both. The following
      // state is quiet grey. Same colors as the web button.
      className={
        isFollowing
          ? `${box} border border-line bg-gray-100 active:bg-red-50 active:border-red-700`
          : `${box} bg-[#0095f6] active:bg-[#1877f2]`
      }
    >
      <Text className={isFollowing ? `${text} text-ink` : `${text} text-white`}>
        {setFollow.isPending ? "…" : isFollowing ? "Following" : "Follow"}
      </Text>
    </Pressable>
  );
}

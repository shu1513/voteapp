import { useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { Pressable, Text } from "react-native";

// Follow/unfollow toggle. Rendered only for verified users (callers gate on
// useFollows().canFollow); new follows default to both notification kinds on,
// matching the backend's defaults. Port of the web component.

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
      onPress={() => setFollow.mutate({ candidate_id: candidateId, following: !isFollowing })}
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

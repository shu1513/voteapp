import { ApiError, useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { Alert, Platform, Pressable, Text } from "react-native";
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
//
// react-native-web's Alert.alert is an empty function, so on the Expo web
// build both dialogs fall back to the browser's own alert/confirm — without
// that, the confirm step would silently swallow every unfollow there.

const isWeb = Platform.OS === "web";

function showError(title: string, message: string) {
  if (isWeb) {
    globalThis.alert?.(`${title}\n${message}`);
    return;
  }
  Alert.alert(title, message);
}

function confirm(title: string, message: string, onConfirm: () => void) {
  if (isWeb) {
    if (globalThis.confirm?.(`${title}\n${message}`)) {
      onConfirm();
    }
    return;
  }
  Alert.alert(title, message, [
    { text: "Cancel", style: "cancel" },
    { text: "Unfollow", style: "destructive", onPress: onConfirm },
  ]);
}

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
          showError(
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
    confirm(`Unfollow ${candidateName}?`, "You'll stop getting updates about them.", () => submit(false));
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

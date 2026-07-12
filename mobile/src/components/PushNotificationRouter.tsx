import * as Notifications from "expo-notifications";
import { router } from "expo-router";
import { useEffect } from "react";

// Routes notification taps to their in-app target: the backend puts the
// destination path in data.url ("/follows" for digests, "/my-ballot" for
// new-election alerts). useLastNotificationResponse covers both a running
// app and a cold start from the tap. Rendered by the root layout on native
// only — Expo web has no push.

// Survives root-layout remounts, which would otherwise replay the last
// response and re-navigate.
const handledResponseIds = new Set<string>();

export function PushNotificationRouter() {
  const response = Notifications.useLastNotificationResponse();

  useEffect(() => {
    if (!response) {
      return;
    }
    const id = response.notification.request.identifier;
    if (handledResponseIds.has(id)) {
      return;
    }
    handledResponseIds.add(id);

    const url = response.notification.request.content.data?.url;
    // In-app paths only; anything else in the payload is ignored.
    if (typeof url === "string" && url.startsWith("/")) {
      router.push(url as never);
    }
  }, [response]);

  return null;
}

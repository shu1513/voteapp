import AsyncStorage from "@react-native-async-storage/async-storage";
import Constants from "expo-constants";
import * as Notifications from "expo-notifications";
import { Platform } from "react-native";
import { apiRequest } from "@voteapp/api-client";

// Device push registration against POST/DELETE /api/me/push-tokens. Push is
// an enhancement, never a gate: every failure here (no permission, Expo Go's
// missing push support, simulator, no EAS project id yet, network) resolves
// to a silent no-op so the triggering flow — following a candidate, saving a
// ballot, logging in — is untouched.
//
// The permission prompt fires only from the two moments the plan names
// (first follow, saved ballot), never on launch; login re-registers silently
// when permission is already granted, because logout revoked the token.

const REGISTERED_TOKEN_STORAGE_KEY = "voteapp.push.registeredExpoPushToken";

/**
 * Foreground presentation: show pushes as banners while the app is open.
 * Called once from the root layout; no-op on web (Expo web has no push).
 */
export function initPushNotifications(): void {
  if (Platform.OS === "web") {
    return;
  }
  Notifications.setNotificationHandler({
    handleNotification: async () => ({
      shouldShowBanner: true,
      shouldShowList: true,
      shouldPlaySound: false,
      shouldSetBadge: false,
    }),
  });
}

/**
 * Silent variant for login: registers only when the OS permission is already
 * granted, so returning users get their token back (logout revoked it)
 * without a prompt.
 */
export async function registerForPushIfPermitted(): Promise<void> {
  await register({ requestPermission: false });
}

/**
 * Prompting variant for the first follow / saved ballot: asks for the OS
 * permission when undetermined, then registers. Denials are final until the
 * user flips the OS setting — both platforms suppress repeat prompts — so
 * calling this on every follow stays quiet after a "no", and after a "yes"
 * each call is just an idempotent re-register that bumps last_seen_at.
 */
export async function registerForPushRequestingPermission(): Promise<void> {
  await register({ requestPermission: true });
}

// One registration at a time: a follow spree must not stack identical
// permission prompts or duplicate POSTs.
let inFlight: Promise<void> | null = null;

async function register(options: { requestPermission: boolean }): Promise<void> {
  if (Platform.OS === "web") {
    return;
  }
  if (inFlight) {
    return inFlight;
  }
  inFlight = (async () => {
    try {
      // Android 13+ shows no permission prompt until a channel exists.
      if (Platform.OS === "android") {
        await Notifications.setNotificationChannelAsync("default", {
          name: "Default",
          importance: Notifications.AndroidImportance.DEFAULT,
        });
      }

      let permission = await Notifications.getPermissionsAsync();
      if (!permission.granted && options.requestPermission && permission.canAskAgain) {
        permission = await Notifications.requestPermissionsAsync();
      }
      if (!permission.granted) {
        return;
      }

      // Without an EAS project id (wired up in Phase L) Expo cannot mint a
      // push token; getExpoPushTokenAsync would reject. Bail quietly.
      const projectId =
        Constants.expoConfig?.extra?.eas?.projectId ?? Constants.easConfig?.projectId ?? null;
      if (typeof projectId !== "string" || projectId.length === 0) {
        return;
      }

      const expoPushToken = (await Notifications.getExpoPushTokenAsync({ projectId })).data;

      // The raw APNs/FCM token is stored server-side so a later move off the
      // Expo push service is sender-only work; losing it is not an error.
      let nativeToken: string | null = null;
      try {
        const deviceToken = await Notifications.getDevicePushTokenAsync();
        nativeToken = typeof deviceToken.data === "string" ? deviceToken.data : null;
      } catch {
        // Expo Go / unsupported environment.
      }

      await apiRequest("/api/me/push-tokens", {
        method: "POST",
        body: {
          expo_push_token: expoPushToken,
          native_token: nativeToken,
          platform: Platform.OS === "ios" ? "ios" : "android",
        },
      });
      // Remembered so logout can revoke exactly what this device registered.
      await AsyncStorage.setItem(REGISTERED_TOKEN_STORAGE_KEY, expoPushToken);
    } catch {
      // Push is best-effort; the triggering flow must not surface this.
    } finally {
      inFlight = null;
    }
  })();
  return inFlight;
}

/**
 * Logout hook: revokes this device's registration while the session is still
 * valid (the DELETE is bearer-authed), then forgets the stored token. Errors
 * are swallowed — a failed revoke must never block logout, and the token
 * dies server-side anyway once Expo reports DeviceNotRegistered.
 */
export async function revokeStoredPushRegistration(): Promise<void> {
  if (Platform.OS === "web") {
    return;
  }
  try {
    const token = await AsyncStorage.getItem(REGISTERED_TOKEN_STORAGE_KEY);
    if (!token) {
      return;
    }
    await apiRequest("/api/me/push-tokens", {
      method: "DELETE",
      body: { expo_push_token: token },
    });
    await AsyncStorage.removeItem(REGISTERED_TOKEN_STORAGE_KEY);
  } catch {
    // Best-effort; see above.
  }
}

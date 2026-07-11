import { configureApi } from "@voteapp/api-client";
import { getSessionId } from "./sessionStore";

/**
 * Points the shared api-client at the VoteApp backend and attaches the
 * Bearer session transport. Called once from the root layout.
 *
 * EXPO_PUBLIC_API_URL examples:
 *   dev simulator: http://127.0.0.1:3001
 *   production:    https://impactperdollar.com
 */
export function initApi(): void {
  configureApi({
    baseUrl: process.env.EXPO_PUBLIC_API_URL ?? "",
    getAuthHeader: async () => {
      // A keystore failure must degrade to "signed out", not kill the
      // request (expo-secure-store throws on web, where Expo serves the
      // app for development).
      try {
        const sessionId = await getSessionId();
        return sessionId ? `Bearer ${sessionId}` : null;
      } catch {
        return null;
      }
    },
  });
}

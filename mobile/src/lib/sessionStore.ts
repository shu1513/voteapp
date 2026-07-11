// Bearer session id storage (Phase 0 transport): the opaque Redis session id
// lives in the platform keystore (iOS Keychain / Android Keystore), never in
// AsyncStorage. Login/password-change flows write it, logout clears it.
//
// expo-secure-store throws on web, where Expo serves the app during
// development; an in-memory fallback keeps the session for the tab's
// lifetime there (mobile-client API responses set no cookie, so without it
// web dev logins would silently not stick).
import * as SecureStore from "expo-secure-store";

const SESSION_KEY = "voteapp_session_id";

let memoryFallback: string | null = null;

export async function getSessionId(): Promise<string | null> {
  try {
    return await SecureStore.getItemAsync(SESSION_KEY);
  } catch {
    return memoryFallback;
  }
}

export async function setSessionId(sessionId: string): Promise<void> {
  try {
    await SecureStore.setItemAsync(SESSION_KEY, sessionId);
  } catch {
    memoryFallback = sessionId;
  }
}

export async function clearSessionId(): Promise<void> {
  memoryFallback = null;
  try {
    await SecureStore.deleteItemAsync(SESSION_KEY);
  } catch {
    // Keystore unavailable (web): the memory fallback above is the store.
  }
}

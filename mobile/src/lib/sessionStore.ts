// Bearer session id storage (Phase 0 transport): the opaque Redis session id
// lives in the platform keystore (iOS Keychain / Android Keystore), never in
// AsyncStorage. Login/password-change flows write it, logout clears it.
//
// Web (where expo-secure-store throws) gets an in-memory fallback for the
// tab's lifetime — that surface can't receive a session id from the backend
// anyway (browser-provenance gate), so this only serves dev flows. On
// native, a keystore WRITE failure propagates so login fails loudly instead
// of "succeeding" into a session that silently dies on the next app launch;
// read/clear failures degrade to signed-out rather than bricking requests.
import { Platform } from "react-native";
import * as SecureStore from "expo-secure-store";

const SESSION_KEY = "voteapp_session_id";

let memoryFallback: string | null = null;

export async function getSessionId(): Promise<string | null> {
  try {
    return await SecureStore.getItemAsync(SESSION_KEY);
  } catch {
    return Platform.OS === "web" ? memoryFallback : null;
  }
}

export async function setSessionId(sessionId: string): Promise<void> {
  if (Platform.OS === "web") {
    memoryFallback = sessionId;
    return;
  }
  await SecureStore.setItemAsync(SESSION_KEY, sessionId);
}

export async function clearSessionId(): Promise<void> {
  memoryFallback = null;
  try {
    await SecureStore.deleteItemAsync(SESSION_KEY);
  } catch {
    // Clearing must never block logout; on web the memory reset above is
    // the store, and a native delete failure leaves only a dead id behind.
  }
}

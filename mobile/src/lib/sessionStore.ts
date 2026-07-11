// Bearer session id storage (Phase 0 transport): the opaque Redis session id
// lives in the platform keystore (iOS Keychain / Android Keystore), never in
// AsyncStorage. Login/password-change screens write it, logout clears it.
import * as SecureStore from "expo-secure-store";

const SESSION_KEY = "voteapp_session_id";

export function getSessionId(): Promise<string | null> {
  return SecureStore.getItemAsync(SESSION_KEY);
}

export function setSessionId(sessionId: string): Promise<void> {
  return SecureStore.setItemAsync(SESSION_KEY, sessionId);
}

export function clearSessionId(): Promise<void> {
  return SecureStore.deleteItemAsync(SESSION_KEY);
}

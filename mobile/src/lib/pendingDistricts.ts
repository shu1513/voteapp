// Anonymous-to-account district handoff, ported from the web's
// sessionStorage version onto AsyncStorage: the ids from the last anonymous
// address resolve wait here until GET /api/me reports email_verified: true —
// POST /api/me/districts/initialize is verified-email-gated, and login works
// while still unverified, so initializing right after login would 403.
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "voteapp_pending_district_ids";

export async function savePendingDistrictIds(districtIds: readonly string[]): Promise<void> {
  try {
    await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify(districtIds));
  } catch {
    // Storage unavailable: the handoff just won't happen.
  }
}

export async function readPendingDistrictIds(): Promise<string[]> {
  try {
    const raw = await AsyncStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((id): id is string => typeof id === "string") : [];
  } catch {
    return [];
  }
}

export async function clearPendingDistrictIds(): Promise<void> {
  try {
    await AsyncStorage.removeItem(STORAGE_KEY);
  } catch {
    // Ignore.
  }
}

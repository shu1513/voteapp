// Anonymous-to-account district handoff, ported from the web's
// sessionStorage version onto AsyncStorage: the ids from the last anonymous
// address resolve wait here until GET /api/me reports email_verified: true —
// POST /api/me/districts/initialize is verified-email-gated, and login works
// while still unverified, so initializing right after login would 403.
//
// Unlike sessionStorage, AsyncStorage survives restarts indefinitely, so the
// payload carries a timestamp and expires: a search from weeks ago (possibly
// by a different person on the device) must not silently initialize an
// account. The window is generous because the legitimate flow includes an
// email-verification round trip that can take hours.
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "voteapp_pending_district_ids";
const TTL_MS = 24 * 60 * 60 * 1000;

export async function savePendingDistrictIds(districtIds: readonly string[]): Promise<void> {
  try {
    await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify({ districtIds, savedAt: Date.now() }));
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
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !Array.isArray((parsed as { districtIds?: unknown }).districtIds) ||
      typeof (parsed as { savedAt?: unknown }).savedAt !== "number"
    ) {
      // Unknown shape (including the pre-timestamp bare-array format):
      // discard rather than hand off ids of unknown age.
      await AsyncStorage.removeItem(STORAGE_KEY);
      return [];
    }
    const { districtIds, savedAt } = parsed as { districtIds: unknown[]; savedAt: number };
    if (Date.now() - savedAt > TTL_MS) {
      await AsyncStorage.removeItem(STORAGE_KEY);
      return [];
    }
    return districtIds.filter((id): id is string => typeof id === "string");
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

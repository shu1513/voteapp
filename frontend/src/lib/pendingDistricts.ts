// Anonymous-to-account district handoff. The ids from the last anonymous
// address resolve wait in sessionStorage until GET /api/me reports
// email_verified: true — POST /api/me/districts/initialize is
// verified-email-gated, and login works while still unverified, so
// initializing right after login would 403.

const STORAGE_KEY = "voteapp_pending_district_ids";

export function savePendingDistrictIds(districtIds: readonly string[]): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(districtIds));
  } catch {
    // Storage unavailable (private mode): the handoff just won't happen.
  }
}

export function readPendingDistrictIds(): string[] {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((id): id is string => typeof id === "string") : [];
  } catch {
    return [];
  }
}

export function clearPendingDistrictIds(): void {
  try {
    sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    // Ignore.
  }
}

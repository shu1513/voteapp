// Anonymous-to-account district handoff. The ids from the last anonymous
// address resolve wait in sessionStorage until GET /api/me reports
// email_verified: true — POST /api/me/districts/initialize is
// verified-email-gated, and login works while still unverified, so
// initializing right after login would 403. The handoff itself runs in
// districtHandoff.ts (mounted once in App); this module only owns the queue.

const STORAGE_KEY = "voteapp_pending_district_ids";

// Save/clear notify subscribers so the handoff store (districtHandoff.ts)
// can re-derive its status without polling sessionStorage.
const listeners = new Set<() => void>();

function notify(): void {
  for (const listener of listeners) {
    listener();
  }
}

export function subscribePendingDistrictIds(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function savePendingDistrictIds(districtIds: readonly string[]): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(districtIds));
  } catch {
    // Storage unavailable (private mode): the handoff just won't happen.
  }
  notify();
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
  notify();
}

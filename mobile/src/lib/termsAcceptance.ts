// Port of the web's terms-acceptance memory onto AsyncStorage. Same rules,
// same 90-day window, same reasons — see frontend/src/lib/termsAcceptance.ts.
//
// Stores a version and a timestamp and nothing else: no identifier, no server
// record, nothing that identifies anybody.
//
// This may only ever decide whether the sheet OPENS. It must never pre-tick
// the checkbox inside it.
import AsyncStorage from "@react-native-async-storage/async-storage";

import { TERMS_VERSION } from "@voteapp/api-client";

const STORAGE_KEY = "voteapp_terms_acceptance";

export const TERMS_ACCEPTANCE_TTL_MS = 90 * 24 * 60 * 60 * 1000;

/**
 * True only when this device accepted the CURRENT terms inside the window.
 * Everything else — nothing stored, unreadable, superseded version, expired
 * or future-dated timestamp, storage failure — returns false and the visitor
 * sees the sheet. Fails closed.
 */
export async function hasCurrentTermsAcceptance(now: number = Date.now()): Promise<boolean> {
  try {
    const raw = await AsyncStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return false;
    }
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) {
      return false;
    }
    const { version, acceptedAt } = parsed as { version?: unknown; acceptedAt?: unknown };
    if (typeof version !== "string" || typeof acceptedAt !== "number" || !Number.isFinite(acceptedAt)) {
      return false;
    }
    if (version !== TERMS_VERSION) {
      return false;
    }
    const age = now - acceptedAt;
    // Negative age means a clock change or a tampered value: unusable, rather
    // than an acceptance that never expires.
    return age >= 0 && age <= TERMS_ACCEPTANCE_TTL_MS;
  } catch {
    return false;
  }
}

/** Records an acceptance the visitor just made. Never called on their behalf. */
export async function rememberTermsAcceptance(now: number = Date.now()): Promise<void> {
  try {
    await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify({ version: TERMS_VERSION, acceptedAt: now }));
  } catch {
    // Storage unavailable: the visitor is asked again next time. Never block
    // the search on being able to remember it.
  }
}

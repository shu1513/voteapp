// Port of the web's frontend/src/lib/draftCompleteSeen.ts onto AsyncStorage:
// the device-local "already congratulated" marker for the draft completion
// notice, one entry per election date. Shared by every account on the
// device; that is the guarantee, nothing per-user across devices. Storage
// failures fall back to a module-level set so the notice still fires at
// most once per app run.
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "voteapp_draft_complete_seen";

const seenInMemory = new Set<string>();

async function readSeenDates(): Promise<string[] | null> {
  try {
    const raw = await AsyncStorage.getItem(STORAGE_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((date): date is string => typeof date === "string") : [];
  } catch {
    return null;
  }
}

// Memory first: a device whose reads work but whose writes fail would
// otherwise repeat the notice.
export async function hasDraftCompleteBeenSeen(date: string): Promise<boolean> {
  if (seenInMemory.has(date)) {
    return true;
  }
  return (await readSeenDates())?.includes(date) ?? false;
}

export async function markDraftCompleteSeen(date: string): Promise<void> {
  const dates = await readSeenDates();
  if (dates === null) {
    seenInMemory.add(date);
    return;
  }
  if (dates.includes(date)) {
    return;
  }
  try {
    await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify([...dates, date]));
  } catch {
    seenInMemory.add(date);
  }
}

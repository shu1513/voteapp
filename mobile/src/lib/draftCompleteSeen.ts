// Port of the web's frontend/src/lib/draftCompleteSeen.ts onto AsyncStorage:
// the device-local "already shown" markers for the draft completion
// surfaces, one entry per election date, one scope per surface (the pick
// screens' notice, the My Draft milestone) so each shows exactly once.
// Shared by every account on the device; that is the guarantee, nothing
// per-user across devices. Storage failures fall back to a module-level
// set so each surface still shows at most once per app run.
import AsyncStorage from "@react-native-async-storage/async-storage";

export type DraftCompleteSeenScope = "notice" | "milestone";

const STORAGE_KEYS: Record<DraftCompleteSeenScope, string> = {
  notice: "voteapp_draft_complete_seen",
  milestone: "voteapp_draft_milestone_seen",
};

const seenInMemory: Record<DraftCompleteSeenScope, Set<string>> = {
  notice: new Set(),
  milestone: new Set(),
};

async function readSeenDates(scope: DraftCompleteSeenScope): Promise<string[] | null> {
  try {
    const raw = await AsyncStorage.getItem(STORAGE_KEYS[scope]);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((date): date is string => typeof date === "string") : [];
  } catch {
    return null;
  }
}

// Memory first: a device whose reads work but whose writes fail would
// otherwise repeat the surface.
export async function hasDraftCompleteBeenSeen(
  date: string,
  scope: DraftCompleteSeenScope = "notice"
): Promise<boolean> {
  if (seenInMemory[scope].has(date)) {
    return true;
  }
  return (await readSeenDates(scope))?.includes(date) ?? false;
}

export async function markDraftCompleteSeen(date: string, scope: DraftCompleteSeenScope = "notice"): Promise<void> {
  const dates = await readSeenDates(scope);
  if (dates === null) {
    seenInMemory[scope].add(date);
    return;
  }
  if (dates.includes(date)) {
    return;
  }
  try {
    await AsyncStorage.setItem(STORAGE_KEYS[scope], JSON.stringify([...dates, date]));
  } catch {
    seenInMemory[scope].add(date);
  }
}

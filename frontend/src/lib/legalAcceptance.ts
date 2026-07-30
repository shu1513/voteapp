const LEGAL_SUBJECT_STORAGE_KEY = "elections-simplified-legal-subject-id";

export function createLegalAcceptanceId(): string {
  return crypto.randomUUID();
}

/** Stable pseudonymous browser ID. Stores identity only—never assent state. */
export function getOrCreateLegalSubjectId(): string {
  const existing = localStorage.getItem(LEGAL_SUBJECT_STORAGE_KEY);
  if (existing) {
    return existing;
  }
  const created = crypto.randomUUID();
  localStorage.setItem(LEGAL_SUBJECT_STORAGE_KEY, created);
  return created;
}

export function normalizeElectionTitleKey(value: string): string {
  return value
    .toLowerCase()
    .replace(/\boffice\s*no(?=\.|\s|\d)\.?\s*/g, "office no ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\boffice no 0+(\d+)\b/g, "office no $1");
}

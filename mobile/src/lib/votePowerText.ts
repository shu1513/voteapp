// Text color for the vote-power label, hottest at the top — text color only,
// no background tint (a tinted badge read as a button). Leverage above the
// statewide baseline glows warm (red → orange → amber as it cools), the
// average step sits cool at sky, and low-leverage ones fade to gray — two
// gray steps, not purple, because purple marks the viewer's saved issues on
// the same card and must mean only that. Full literal class strings so
// NativeWind can see them. "unknown" never renders a label, so it has no
// entry; unrecognized labels fall back to the soft ink. Mirror of the web's
// frontend/src/lib/votePowerBadge.ts (same ladder, same reasons).
const TEXT_CLASS_BY_LABEL: Record<string, string> = {
  very_high: "text-red-700",
  high: "text-orange-700",
  above_average: "text-amber-700",
  medium: "text-sky-700",
  low: "text-gray-600",
  very_low: "text-gray-500",
};

export function votePowerTextClass(label: string): string {
  return TEXT_CLASS_BY_LABEL[label] ?? "text-ink-soft";
}

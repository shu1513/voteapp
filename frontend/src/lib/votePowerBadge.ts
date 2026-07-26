// Color scale for the vote-power label, hottest at the top — text color only,
// no background tint: a boxed/tinted badge read as a button, and box styling
// is reserved for interactive elements. The label answers
// "how much does my vote matter here" at a glance, so high-leverage elections
// glow warm (red/orange/amber) and low-leverage ones cool off through purple
// to gray. Full literal class strings — Tailwind only generates classes it can see in
// source. The "unknown" label never renders a badge, so it has no entry;
// unrecognized labels fall back to the neutral surface chip.
const BADGE_CLASS_BY_LABEL: Record<string, string> = {
  very_high: "text-red-700",
  high: "text-orange-700",
  medium: "text-amber-800",
  low: "text-purple-700",
  very_low: "text-gray-500",
};

export function votePowerBadgeClass(label: string): string {
  return BADGE_CLASS_BY_LABEL[label] ?? "text-ink-soft";
}

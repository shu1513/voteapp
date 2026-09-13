// Color scale for the vote-power label, hottest at the top — text color only,
// no background tint: a boxed/tinted badge read as a button, and box styling
// is reserved for interactive elements. The label answers
// "how much does my vote matter here" at a glance, so leverage above the
// statewide baseline glows warm (red → orange → amber as it cools), the
// average step sits cool at sky, and low-leverage ones fade to gray — two gray
// steps, not purple, because purple marks the viewer's saved issues on the
// same card (SAVED_AREA_TEXT_CLASS) and must mean only that. Sky, not amber,
// for the middle: amber-800 is a muddy brown that reads as a warning rather
// than a midpoint, and lighter ambers miss 4.5:1 at this size; amber-700
// (#b45309, ~5:1 on white) is the one amber that passes, and it takes the
// above-average step as the coolest warm tone. sky-700 is far enough off
// dem-blue (#0015bc) that it does not read as a party color. Full literal
// class strings — Tailwind only generates classes it can see in source. The
// "unknown" label never renders a badge, so it has no entry; unrecognized
// labels fall back to the neutral surface chip.
const BADGE_CLASS_BY_LABEL: Record<string, string> = {
  very_high: "text-red-700",
  high: "text-orange-700",
  above_average: "text-amber-700",
  medium: "text-sky-700",
  low: "text-gray-600",
  very_low: "text-gray-500",
};

export function votePowerBadgeClass(label: string): string {
  return BADGE_CLASS_BY_LABEL[label] ?? "text-ink-soft";
}

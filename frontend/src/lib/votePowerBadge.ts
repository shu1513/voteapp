// Color scale for the vote-power badge, hottest at the top: the badge answers
// "how much does my vote matter here" at a glance, so high-leverage elections
// glow warm (red/orange/amber) and low-leverage ones cool off to gray. Full
// literal class strings — Tailwind only generates classes it can see in
// source. The "unknown" label never renders a badge, so it has no entry;
// unrecognized labels fall back to the neutral surface chip.
const BADGE_CLASS_BY_LABEL: Record<string, string> = {
  very_high: "bg-red-600/10 text-red-700",
  high: "bg-orange-500/10 text-orange-700",
  medium: "bg-amber-400/20 text-amber-800",
  low: "bg-slate-500/10 text-slate-600",
  very_low: "bg-gray-400/10 text-gray-500",
};

export function votePowerBadgeClass(label: string): string {
  return BADGE_CLASS_BY_LABEL[label] ?? "bg-surface text-ink-soft";
}

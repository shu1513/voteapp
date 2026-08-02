/**
 * "Only my issues" toggle chip plus the hidden-count line, shared by the
 * anonymous and saved ballot pages. Render only when api-client's
 * deriveOnlyMyIssues says showFilter (the derivation lives there so web and
 * mobile share one copy — and this file only exports a component, keeping
 * fast refresh); the pages own the on/off state (lib/useIssuesFilterParam)
 * so the choice survives navigating into an election and back.
 */
export function OnlyMyIssuesToggle({
  on,
  hiddenCount,
  onChange,
}: {
  on: boolean;
  hiddenCount: number;
  onChange: (on: boolean) => void;
}) {
  return (
    // aria-live sits on this always-mounted container, not on the count
    // span: a live region that mounts WITH its content is unreliably
    // announced, while content appearing inside an existing region is the
    // well-supported case. Polite, so pressing the toggle announces the
    // hidden count — the whole point of the count is that a filtered
    // ballot must never silently read as the full one.
    <div className="flex flex-wrap items-center gap-2" aria-live="polite">
      <button
        type="button"
        onClick={() => onChange(!on)}
        aria-pressed={on}
        className={`rounded-full border px-3 py-1 text-sm font-medium transition ${
          on ? "border-ink bg-ink text-white" : "border-line bg-white text-ink hover:bg-surface"
        }`}
      >
        Only my issues
      </button>
      {on && hiddenCount > 0 ? (
        // The hidden count is always visible while the filter hides any
        // race: filtered-out elections still elect real officials, so the
        // filtered ballot must never look like the full one. At 0 hidden
        // there is nothing concealed and the pressed chip alone carries the
        // state.
        <span className="text-xs text-ink-soft">
          {hiddenCount} election{hiddenCount === 1 ? "" : "s"} hidden ·{" "}
          <button
            type="button"
            onClick={() => onChange(false)}
            className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
          >
            Show all
          </button>
        </span>
      ) : null}
    </div>
  );
}

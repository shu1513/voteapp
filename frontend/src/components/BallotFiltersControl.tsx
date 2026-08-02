import { useId, useState, type ReactNode } from "react";
import type { VoteImpactThreshold } from "@voteapp/api-client";
import { DisclosureTrigger } from "./DisclosureTrigger";

function FilterCheckbox({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex cursor-pointer items-center gap-2 text-sm text-ink-soft">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        className="h-4 w-4 accent-rausch"
      />
      {label}
    </label>
  );
}

/**
 * The unified "Filters" disclosure for the ballot pages: one chip-styled
 * button opening an inline panel that holds the session-scoped filters
 * (checkboxes — per-filter visibility comes from api-client's
 * deriveBallotFilters) and, on the saved page, the persisted ordering
 * preference the caller passes as orderSection. The two halves persist
 * differently, so the panel keeps them under separate "Show" / "Order"
 * headings rather than blending them. The pages own the filter state
 * (lib/useBallotFilterParams) so the choices survive navigating into an
 * election and back.
 *
 * The impact checkboxes are nested thresholds ("High or above" ⊂ "Average
 * or above"), so exactly one can be engaged: checking one swaps the other
 * off, and unchecking means any impact. Labels reuse the card vocabulary
 * from formatVotePowerLabel — "Average", never "medium".
 *
 * Inline disclosure, not a floating popover: the panel opens in flow below
 * the controls row, so there is no portal, positioning, or outside-click
 * machinery to maintain. Renders nothing when it has nothing to offer — no
 * offerable filter and no order section.
 */
export function BallotFiltersControl({
  showIssues,
  issuesOn,
  onIssuesChange,
  showImpactHigh,
  showImpactMedium,
  impactLevel,
  onImpactChange,
  activeFilterCount,
  hiddenCount,
  onShowAll,
  orderSection,
}: {
  showIssues: boolean;
  issuesOn: boolean;
  onIssuesChange: (on: boolean) => void;
  showImpactHigh: boolean;
  showImpactMedium: boolean;
  impactLevel: VoteImpactThreshold | null;
  onImpactChange: (level: VoteImpactThreshold | null) => void;
  activeFilterCount: number;
  hiddenCount: number;
  onShowAll: () => void;
  orderSection?: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const showImpactGroup = showImpactHigh || showImpactMedium;
  const showSection = showIssues || showImpactGroup;
  if (!showSection && !orderSection) {
    return null;
  }

  return (
    <div className="flex flex-col items-end gap-2">
      {/* aria-live sits on this always-mounted container, not on the count
          span: a live region that mounts WITH its content is unreliably
          announced, while content appearing inside an existing region is
          the well-supported case. Polite, so engaging a filter announces
          the hidden count — a filtered ballot must never silently read as
          the full one. */}
      <div className="flex flex-wrap items-center gap-2" aria-live="polite">
        <DisclosureTrigger open={open} panelId={panelId} onClick={() => setOpen(!open)}>
          {/* The badge counts active FILTERS only, never ordering — it
              answers "is anything hidden-by-choice right now". */}
          Filters{activeFilterCount > 0 ? ` · ${activeFilterCount}` : ""}
        </DisclosureTrigger>
        {hiddenCount > 0 ? (
          // Always visible while any filter hides a race — outside the
          // panel, so closing the disclosure never conceals that the list
          // is filtered. Filtered-out elections still elect real officials.
          <span className="text-xs text-ink-soft">
            {hiddenCount} election{hiddenCount === 1 ? "" : "s"} hidden ·{" "}
            <button
              type="button"
              onClick={onShowAll}
              className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
            >
              Show all
            </button>
          </span>
        ) : null}
      </div>
      {open ? (
        <div id={panelId} className="flex flex-col gap-3 rounded-lg border border-line bg-white p-3">
          {showSection ? (
            <div className="flex flex-col gap-2">
              <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Show</p>
              {showIssues ? (
                <FilterCheckbox label="Affects my issues" checked={issuesOn} onChange={onIssuesChange} />
              ) : null}
              {showImpactGroup ? (
                <div className="flex flex-col gap-2">
                  <p className="text-xs text-ink-soft">Vote impact</p>
                  {showImpactHigh ? (
                    <FilterCheckbox
                      label="High or above"
                      checked={impactLevel === "high"}
                      onChange={(checked) => onImpactChange(checked ? "high" : null)}
                    />
                  ) : null}
                  {showImpactMedium ? (
                    <FilterCheckbox
                      label="Average or above"
                      checked={impactLevel === "medium"}
                      onChange={(checked) => onImpactChange(checked ? "medium" : null)}
                    />
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}
          {orderSection ? (
            <div className="flex flex-col gap-2">
              <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Order</p>
              {orderSection}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

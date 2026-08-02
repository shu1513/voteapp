import { useId, useState, type ReactNode } from "react";

const CHIP_ON = "rounded-full border border-ink bg-ink px-3 py-1 text-sm font-medium text-white transition";
const CHIP_OFF =
  "rounded-full border border-line bg-white px-3 py-1 text-sm font-medium text-ink transition hover:bg-surface";

/**
 * The unified "Filters" disclosure for the ballot pages: one chip-styled
 * button opening an inline panel that holds the session-scoped filters
 * ("Only my issues", "High impact only" — per-filter visibility comes from
 * api-client's deriveBallotFilters) and, on the saved page, the persisted
 * ordering preference the caller passes as orderSection. The two halves
 * persist differently, so the panel keeps them under separate "Show" /
 * "Order" headings rather than blending them. The pages own the filter
 * state (lib/useBallotFilterParams) so the choices survive navigating into
 * an election and back.
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
  showImpact,
  impactOn,
  onImpactChange,
  activeFilterCount,
  hiddenCount,
  onShowAll,
  orderSection,
}: {
  showIssues: boolean;
  issuesOn: boolean;
  onIssuesChange: (on: boolean) => void;
  showImpact: boolean;
  impactOn: boolean;
  onImpactChange: (on: boolean) => void;
  activeFilterCount: number;
  hiddenCount: number;
  onShowAll: () => void;
  orderSection?: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const showSection = showIssues || showImpact;
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
        <button
          type="button"
          onClick={() => setOpen(!open)}
          aria-expanded={open}
          aria-controls={panelId}
          className={open ? CHIP_ON : CHIP_OFF}
        >
          {/* The badge counts active FILTERS only, never ordering — it
              answers "is anything hidden-by-choice right now". */}
          Filters{activeFilterCount > 0 ? ` · ${activeFilterCount}` : ""}
        </button>
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
            <div className="flex flex-col items-end gap-2">
              <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Show</p>
              <div className="flex flex-wrap justify-end gap-2">
                {showIssues ? (
                  <button
                    type="button"
                    onClick={() => onIssuesChange(!issuesOn)}
                    aria-pressed={issuesOn}
                    className={issuesOn ? CHIP_ON : CHIP_OFF}
                  >
                    Only my issues
                  </button>
                ) : null}
                {showImpact ? (
                  <button
                    type="button"
                    onClick={() => onImpactChange(!impactOn)}
                    aria-pressed={impactOn}
                    className={impactOn ? CHIP_ON : CHIP_OFF}
                  >
                    High impact only
                  </button>
                ) : null}
              </div>
            </div>
          ) : null}
          {orderSection ? (
            <div className="flex flex-col items-end gap-2">
              <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Order</p>
              {orderSection}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

import { useState } from "react";
import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  MouseSensor,
  pointerWithin,
  TouchSensor,
  useDraggable,
  useDroppable,
  useSensor,
  useSensors,
  type CollisionDetection,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { INTEGRITY_ETHICS_SLUG, sortByResearchAreaPriority } from "@voteapp/api-client";
import { newRankedResearchArea, type RankedResearchArea } from "../lib/rankedResearchAreas";

// Controlled ranked-selection editor for research areas, shared by the
// post-signup welcome step and the settings section. The parent owns the
// ordered list and persistence; every edit (add, remove, reorder, direction,
// must) comes back as one complete next list through onChange,
// so the parent can save per-edit (settings) or batch into a single save
// (welcome). List position is the rank: first = rank 1. No cap on length.
//
// Two panels, side by side from lg up: the ranked list (a drop zone) and the
// pool of unchosen issues. Tapping a pool card appends it — that path, not
// drag, is the accessible one — and dragging a card onto the ranked panel
// inserts it where it drops. Chosen issues leave the pool, so the pool only
// ever shrinks under the user, never reflows a growing list above it (the
// complaint with the old stacked layout).

export type ResearchAreaOption = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

type ResearchAreaPickerProps = {
  areas: ResearchAreaOption[];
  ranked: RankedResearchArea[];
  disabled: boolean;
  onChange: (next: RankedResearchArea[]) => void;
};

const RANKED_PANEL_ID = "ranked-panel";

export function ResearchAreaPicker({ areas, ranked, disabled, onChange }: ResearchAreaPickerProps) {
  // Mouse: drag starts after 4px of movement, so the buttons stay plain
  // clicks. Touch: press-and-hold (200ms) then drag, so the lists do not
  // hijack page scrolling. Keyboard sorting stays for accessibility.
  // MouseSensor + TouchSensor deliberately, not PointerSensor: PointerSensor
  // also claims touch input, which would bypass the press-and-hold delay.
  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 4 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 200, tolerance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  const areaById = new Map(areas.map((area) => [area.id, area]));
  const orderedIds = ranked.map((row) => row.research_area_id);

  // Reorders resolve against row/panel centers so keyboard sorting (which has
  // no pointer) keeps working — minus the panel itself, whose large center
  // could otherwise beat the intended row and turn a reorder into a no-op.
  // Pool drags demand the pointer actually inside a target: closest-center
  // would return SOME target even for a card dropped back in the pool, which
  // would add it.
  const collisionDetection: CollisionDetection = (args) => {
    if (orderedIds.includes(String(args.active.id))) {
      return closestCenter({
        ...args,
        droppableContainers: args.droppableContainers.filter((c) => c.id !== RANKED_PANEL_ID),
      });
    }
    return pointerWithin(args);
  };

  function addAt(id: string, index: number) {
    const next = [...ranked];
    next.splice(index, 0, newRankedResearchArea(id));
    onChange(next);
  }

  function onDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (!over) {
      return;
    }
    const activeId = String(active.id);
    const overId = String(over.id);
    const from = orderedIds.indexOf(activeId);
    if (from >= 0) {
      const to = orderedIds.indexOf(overId);
      if (to < 0 || from === to) {
        return;
      }
      onChange(arrayMove(ranked, from, to));
      return;
    }
    // Pool card: dropped on a ranked row takes that row's rank; dropped on
    // the panel's empty space appends.
    const to = orderedIds.indexOf(overId);
    if (to >= 0) {
      addAt(activeId, to);
    } else if (overId === RANKED_PANEL_ID) {
      addAt(activeId, ranked.length);
    }
  }

  function remove(id: string) {
    onChange(ranked.filter((row) => row.research_area_id !== id));
  }

  function update(id: string, patch: Partial<Omit<RankedResearchArea, "research_area_id">>) {
    onChange(ranked.map((row) => (row.research_area_id === id ? { ...row, ...patch } : row)));
  }

  return (
    <DndContext sensors={sensors} collisionDetection={collisionDetection} onDragEnd={onDragEnd}>
      <div className="mt-3 gap-x-6 lg:grid lg:grid-cols-2 lg:items-start">
        <div>
          <p className="text-sm font-medium text-ink">
            Your priorities{" "}
            <span className="font-normal text-ink-soft">(top = most important)</span>
          </p>
          <RankedDropZone empty={ranked.length === 0}>
            {ranked.length > 0 ? (
              <SortableContext items={orderedIds} strategy={verticalListSortingStrategy}>
                <ol className="space-y-1.5">
                  {ranked.map((row, index) => {
                    const area = areaById.get(row.research_area_id);
                    return (
                      <SortableAreaRow
                        key={row.research_area_id}
                        row={row}
                        index={index}
                        name={area?.name ?? "Unknown area"}
                        isEthics={area?.slug === INTEGRITY_ETHICS_SLUG}
                        disabled={disabled}
                        onChange={(patch) => update(row.research_area_id, patch)}
                        onRemove={() => remove(row.research_area_id)}
                      />
                    );
                  })}
                </ol>
              </SortableContext>
            ) : (
              <p className="px-4 py-8 text-center text-sm text-ink-soft">
                Tap an issue to add it here, or drag it over — then drag to rank.
              </p>
            )}
          </RankedDropZone>
        </div>
        <div className="mt-6 lg:mt-0">
          <p className="text-sm font-medium text-ink">
            Choose issues{" "}
            <span className="font-normal text-ink-soft">({ranked.length} selected)</span>
          </p>
          {/* Public-salience order, not the catalog's alphabetical order — the
              same ranking the election and candidate pages use, so the issues
              most users pick first sit at the top of the grid. */}
          <ul className="mt-2 grid grid-cols-[repeat(auto-fill,minmax(11rem,1fr))] gap-2">
            {sortByResearchAreaPriority(areas)
              .filter((area) => !orderedIds.includes(area.id))
              .map((area) => (
                <PoolAreaCard
                  key={area.id}
                  area={area}
                  disabled={disabled}
                  onAdd={() => addAt(area.id, ranked.length)}
                />
              ))}
          </ul>
        </div>
      </div>
    </DndContext>
  );
}

// The whole left panel accepts pool-card drops, so a drop does not have to
// land exactly on a row — and the empty state has a target at all.
function RankedDropZone({ empty, children }: { empty: boolean; children: React.ReactNode }) {
  const { setNodeRef, isOver } = useDroppable({ id: RANKED_PANEL_ID });
  return (
    <div
      ref={setNodeRef}
      // lg:pb-10: from lg up the panels sit side by side, so the zone keeps
      // an (invisible) catch area under the rows — dropping a card into the
      // empty left-column space right below the list appends instead of
      // dying. Stacked (below lg) the pool sits directly underneath, where
      // that padding would just be a gap.
      className={`mt-2 rounded-lg ${
        empty
          ? `border border-dashed ${isOver ? "border-green-700 bg-green-50" : "border-line"}`
          : `lg:pb-10 ${isOver ? "bg-green-50 ring-2 ring-green-700/40" : ""}`
      }`}
    >
      {children}
    </div>
  );
}

// Compact pool card: name only, with the description behind an ⓘ toggle —
// a tap target, not a title tooltip, because touch never sees tooltips.
// Tap the name to add; press-and-hold (touch) or drag (mouse) to drop it
// into the ranked panel. Drag listeners sit on the <li>, and the keyboard
// activator attributes are deliberately NOT spread: they would turn
// Enter/Space on the add button into "start dragging" and swallow the click.
// Keyboard users add by clicking; sorting afterwards is keyboard-accessible.
function PoolAreaCard({
  area,
  disabled,
  onAdd,
}: {
  area: ResearchAreaOption;
  disabled: boolean;
  onAdd: () => void;
}) {
  const [showInfo, setShowInfo] = useState(false);
  const { setNodeRef, listeners, transform, isDragging } = useDraggable({ id: area.id, disabled });
  return (
    <li
      ref={setNodeRef}
      {...listeners}
      style={{ transform: CSS.Translate.toString(transform) }}
      className={`cursor-grab touch-manipulation select-none rounded-lg border bg-white transition hover:border-green-700 ${
        isDragging ? "z-10 border-green-700 shadow-md" : "border-line"
      }`}
    >
      <span className="flex items-stretch">
        <button
          type="button"
          disabled={disabled}
          onClick={onAdd}
          className="min-w-0 flex-1 px-3 py-2 text-left text-sm font-medium text-ink disabled:cursor-not-allowed disabled:opacity-50"
        >
          {area.name}
        </button>
        {area.description ? (
          <button
            type="button"
            aria-expanded={showInfo}
            aria-label={`About ${area.name}`}
            onClick={() => setShowInfo((open) => !open)}
            className="px-2 text-ink-soft hover:text-ink"
          >
            <span
              aria-hidden
              className="flex h-4 w-4 items-center justify-center rounded-full border border-current text-[10px] font-serif italic"
            >
              i
            </span>
          </button>
        ) : null}
      </span>
      {showInfo && area.description ? (
        <p className="px-3 pb-2 text-xs text-ink-soft">{area.description}</p>
      ) : null}
    </li>
  );
}

function SortableAreaRow({
  row,
  index,
  name,
  isEthics,
  disabled,
  onChange,
  onRemove,
}: {
  row: RankedResearchArea;
  index: number;
  name: string;
  isEthics: boolean;
  disabled: boolean;
  onChange: (patch: Partial<Omit<RankedResearchArea, "research_area_id">>) => void;
  onRemove: () => void;
}) {
  const { attributes, listeners, setNodeRef, setActivatorNodeRef, transform, transition, isDragging } =
    useSortable({ id: row.research_area_id, disabled });
  // The whole row is the drag surface for mouse (grab it anywhere) and touch
  // (press and hold); the buttons inside still click normally thanks to the
  // sensors' activation constraints. Keyboard sorting is anchored on the ⠿
  // handle (setActivatorNodeRef + attributes): without an activator node,
  // dnd-kit treats Space/Enter on ANY element in the row as "start dragging"
  // and swallows the click, so the toggles and × would be unreachable by
  // keyboard.
  // Direction-neutral on purpose: with Oppose selected, the veto fires on
  // records that SUPPORT the goal, so "who opposes this" would read backwards.
  const vetoLabel = isEthics
    ? "Skip candidates with any documented ethics or conviction record"
    : "Must: never pick a candidate or measure that goes against my position on this";
  return (
    <li
      ref={setNodeRef}
      {...listeners}
      style={{ transform: CSS.Transform.toString(transform), transition }}
      // touch-manipulation, not touch-none: the TouchSensor prevents
      // scrolling itself once its press-and-hold delay activates, so plain
      // touches on the list still scroll the page.
      className={`flex cursor-grab touch-manipulation flex-wrap items-center gap-x-2 gap-y-1 rounded-lg border border-line bg-white px-2 py-2 text-sm select-none ${
        isDragging ? "z-10 shadow-md" : ""
      }`}
    >
      <span
        ref={setActivatorNodeRef}
        {...attributes}
        aria-label={`${name}, rank ${index + 1}. Drag to reorder.`}
        className="px-1 text-ink-soft"
      >
        ⠿
      </span>
      <span className="w-6 shrink-0 text-center text-xs font-semibold text-green-800">#{index + 1}</span>
      <span className="min-w-0 flex-1 basis-32 text-ink">{name}</span>
      <span className="ml-auto flex items-center gap-1">
        {isEthics ? null : (
          <span role="group" aria-label={`Your position on ${name}`} className="flex rounded-md border border-line">
            {(["support", "oppose"] as const).map((direction) => (
              <button
                key={direction}
                type="button"
                disabled={disabled}
                aria-pressed={row.direction === direction}
                onClick={() => onChange({ direction })}
                className={`px-2 py-0.5 text-xs first:rounded-l-md last:rounded-r-md disabled:opacity-50 ${
                  row.direction === direction
                    ? direction === "support"
                      ? "bg-green-700 text-white"
                      : "bg-red-700 text-white"
                    : "text-ink-soft hover:bg-surface"
                }`}
              >
                {direction === "support" ? "Support" : "Oppose"}
              </button>
            ))}
          </span>
        )}
        <button
          type="button"
          disabled={disabled}
          aria-pressed={row.hard_veto}
          aria-label={`${vetoLabel} (${name})`}
          title={vetoLabel}
          onClick={() => onChange({ hard_veto: !row.hard_veto })}
          className={`rounded-md border px-2 py-0.5 text-xs disabled:opacity-50 ${
            row.hard_veto
              ? "border-rausch-dark bg-rausch-dark text-white"
              : "border-line text-ink-soft hover:border-rausch-dark"
          }`}
        >
          {isEthics ? "Skip if negative record" : "Must"}
        </button>
        <button
          type="button"
          disabled={disabled}
          onClick={onRemove}
          aria-label={`Remove ${name}`}
          className="px-2 text-ink-soft hover:text-rausch-dark disabled:opacity-30"
        >
          ×
        </button>
      </span>
    </li>
  );
}

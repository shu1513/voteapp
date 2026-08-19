import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  useSensor,
  useSensors,
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
import { sortByResearchAreaPriority } from "@voteapp/api-client";
import { newRankedResearchArea, type RankedResearchArea } from "../lib/rankedResearchAreas";

// Controlled ranked-selection editor for research areas, shared by the
// post-signup welcome step and the settings section. The parent owns the
// ordered list and persistence; every edit (add, remove, reorder, direction,
// line-in-the-sand) comes back as one complete next list through onChange,
// so the parent can save per-edit (settings) or batch into a single save
// (welcome). List position is the rank: first = rank 1. No cap on length.

export type ResearchAreaOption = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

// An ethics record is a strike whatever the user "supports", so this area
// has no direction control and its line-in-the-sand reads as "skip anyone
// with an ethics record" (mirrors candidateRecordResearchAreaPolicy: stance
// is forbidden on this area, its tags only mark that a record exists).
const INTEGRITY_ETHICS_SLUG = "integrity_and_ethics";

type ResearchAreaPickerProps = {
  areas: ResearchAreaOption[];
  ranked: RankedResearchArea[];
  disabled: boolean;
  onChange: (next: RankedResearchArea[]) => void;
};

export function ResearchAreaPicker({ areas, ranked, disabled, onChange }: ResearchAreaPickerProps) {
  // Mouse: drag starts after 4px of movement, so the remove button stays a
  // plain click. Touch: press-and-hold (200ms) then drag, so the list does
  // not hijack page scrolling. Keyboard sorting stays for accessibility.
  // MouseSensor + TouchSensor deliberately, not PointerSensor: PointerSensor
  // also claims touch input, which would bypass the press-and-hold delay.
  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 4 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 200, tolerance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  const areaById = new Map(areas.map((area) => [area.id, area]));
  const orderedIds = ranked.map((row) => row.research_area_id);

  function onDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }
    const from = orderedIds.indexOf(String(active.id));
    const to = orderedIds.indexOf(String(over.id));
    if (from < 0 || to < 0) {
      return;
    }
    onChange(arrayMove(ranked, from, to));
  }

  function remove(id: string) {
    onChange(ranked.filter((row) => row.research_area_id !== id));
  }

  function update(id: string, patch: Partial<Omit<RankedResearchArea, "research_area_id">>) {
    onChange(ranked.map((row) => (row.research_area_id === id ? { ...row, ...patch } : row)));
  }

  return (
    <div>
      {ranked.length > 0 ? (
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
          <SortableContext items={orderedIds} strategy={verticalListSortingStrategy}>
            <ol className="mt-3 space-y-1.5">
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
        </DndContext>
      ) : (
        <p className="mt-3 text-sm text-ink-soft">Nothing selected yet — pick below.</p>
      )}
      <p className="mt-4 text-sm font-medium text-ink">
        Choose issues{" "}
        <span className="font-normal text-ink-soft">({ranked.length} selected)</span>
      </p>
      {/* Full cards with the description visible, not a title tooltip: the
          welcome step is many users' first contact with these labels, and
          touch devices never see tooltips. */}
      {/* Public-salience order, not the catalog's alphabetical order — the
          same ranking the election and candidate pages use, so the issues
          most users pick first sit at the top of the grid. */}
      {/* Selected areas stay in the grid (tinted, with a rank badge) instead
          of disappearing into the top list, so the grid never reflows under
          the user mid-selection. Clicking a selected card unselects it. */}
      <ul className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
        {sortByResearchAreaPriority(areas).map((area) => {
          const rank = orderedIds.indexOf(area.id);
          const selected = rank >= 0;
          return (
            <li key={area.id}>
              <button
                type="button"
                disabled={disabled}
                aria-pressed={selected}
                aria-label={selected ? `${area.name}, rank ${rank + 1}. Click to remove.` : undefined}
                onClick={() =>
                  selected ? remove(area.id) : onChange([...ranked, newRankedResearchArea(area.id)])
                }
                // h-full + items-start: every card in a grid row stretches to
                // the tallest card, so mixed-length descriptions no longer
                // leave ragged gaps between rows.
                // Selected = the app's affirmative green (the YES-vote box and
                // the chosen-pick button use the same border-green-700 /
                // bg-green-50 pair); rausch red stays for destructive controls.
                className={`flex h-full w-full items-start gap-2 rounded-lg border px-3 py-2 text-left transition disabled:cursor-not-allowed disabled:opacity-50 ${
                  selected
                    ? "border-green-700 bg-green-50"
                    : "border-line bg-white hover:border-green-700"
                }`}
              >
                <span className="min-w-0 flex-1">
                  <span className="block text-sm font-medium text-ink">{area.name}</span>
                  {area.description ? (
                    <span className="mt-0.5 block text-xs text-ink-soft">{area.description}</span>
                  ) : null}
                </span>
                {selected ? (
                  <span
                    aria-hidden
                    className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-green-700 text-[11px] font-semibold text-white"
                  >
                    {rank + 1}
                  </span>
                ) : null}
              </button>
            </li>
          );
        })}
      </ul>
    </div>
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
  const vetoLabel = isEthics
    ? "Skip candidates with any integrity or ethics record"
    : "Line in the sand: never pick a candidate who opposes this";
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
                  row.direction === direction ? "bg-ink text-white" : "text-ink-soft hover:bg-surface"
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
          {isEthics ? "Skip if ethics record" : "Line in the sand"}
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

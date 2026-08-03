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
import { MAX_RESEARCH_AREA_RANK, sortByResearchAreaPriority } from "@voteapp/api-client";

// Controlled ranked-selection editor for research areas, shared by the
// post-signup welcome step and the settings section. The parent owns the
// ordered id list and persistence; every edit (add, remove, reorder) comes
// back as one complete next list through onChange, so the parent can save
// per-edit (settings) or batch into a single save (welcome).

export type ResearchAreaOption = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

type ResearchAreaPickerProps = {
  areas: ResearchAreaOption[];
  orderedIds: string[];
  disabled: boolean;
  onChange: (nextIds: string[]) => void;
};

export function ResearchAreaPicker({ areas, orderedIds, disabled, onChange }: ResearchAreaPickerProps) {
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
  const atCapacity = orderedIds.length >= MAX_RESEARCH_AREA_RANK;

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
    onChange(arrayMove(orderedIds, from, to));
  }

  return (
    <div>
      {orderedIds.length > 0 ? (
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
          <SortableContext items={orderedIds} strategy={verticalListSortingStrategy}>
            <ol className="mt-3 space-y-1.5">
              {orderedIds.map((id, index) => (
                <SortableAreaRow
                  key={id}
                  id={id}
                  index={index}
                  name={areaById.get(id)?.name ?? "Unknown area"}
                  disabled={disabled}
                  onRemove={() => onChange(orderedIds.filter((other) => other !== id))}
                />
              ))}
            </ol>
          </SortableContext>
        </DndContext>
      ) : (
        <p className="mt-3 text-sm text-ink-soft">
          Nothing selected yet — pick up to {MAX_RESEARCH_AREA_RANK} below.
        </p>
      )}
      <p className="mt-4 text-sm font-medium text-ink">
        Add issues{" "}
        <span className="font-normal text-ink-soft">
          ({orderedIds.length}/{MAX_RESEARCH_AREA_RANK})
        </span>
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
                disabled={disabled || (!selected && atCapacity)}
                aria-pressed={selected}
                aria-label={selected ? `${area.name}, rank ${rank + 1}. Click to remove.` : undefined}
                onClick={() =>
                  selected
                    ? onChange(orderedIds.filter((other) => other !== area.id))
                    : onChange([...orderedIds, area.id])
                }
                // h-full + items-start: every card in a grid row stretches to
                // the tallest card, so mixed-length descriptions no longer
                // leave ragged gaps between rows.
                className={`flex h-full w-full items-start gap-2 rounded-lg border px-3 py-2 text-left transition disabled:cursor-not-allowed disabled:opacity-50 ${
                  selected
                    ? "border-rausch bg-rausch/5"
                    : "border-line bg-white hover:border-rausch"
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
                    className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-rausch text-[11px] font-semibold text-white"
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
  id,
  index,
  name,
  disabled,
  onRemove,
}: {
  id: string;
  index: number;
  name: string;
  disabled: boolean;
  onRemove: () => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id, disabled });
  // The whole row is the drag surface — grab it anywhere with a mouse, or
  // press and hold on touch. The remove button still clicks normally thanks
  // to the sensors' activation constraints.
  return (
    <li
      ref={setNodeRef}
      {...attributes}
      {...listeners}
      style={{ transform: CSS.Transform.toString(transform), transition }}
      aria-label={`${name}, rank ${index + 1}. Drag to reorder.`}
      // touch-manipulation, not touch-none: the TouchSensor prevents
      // scrolling itself once its press-and-hold delay activates, so plain
      // touches on the list still scroll the page.
      className={`flex cursor-grab touch-manipulation items-center gap-2 rounded-lg border border-line bg-white px-2 py-2 text-sm select-none ${
        isDragging ? "z-10 shadow-md" : ""
      }`}
    >
      <span aria-hidden className="px-1 text-ink-soft">
        ⠿
      </span>
      <span className="w-6 shrink-0 text-center text-xs font-semibold text-rausch-dark">#{index + 1}</span>
      <span className="flex-1 text-ink">{name}</span>
      <button
        type="button"
        disabled={disabled}
        onClick={onRemove}
        aria-label={`Remove ${name}`}
        className="px-2 text-ink-soft hover:text-rausch-dark disabled:opacity-30"
      >
        ×
      </button>
    </li>
  );
}

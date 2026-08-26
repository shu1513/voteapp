import { Combobox, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import type { AddressLocation, AddressSuggestion } from "@voteapp/api-client";
import { useAdoptPreHydrationValue } from "../lib/preHydrationInput";
import { useAddressSuggestions } from "@voteapp/api-client";

// ARIA combobox via Headless UI (the contract doc says not to hand-roll
// keyboard handling). Autocomplete failing must never block the form: the
// input stays a plain controlled text field and the dropdown simply
// disappears.

type AddressAutocompleteProps = {
  value: string;
  /** location is set only when the value came from a completed dropdown
   * selection; every keystroke or fallback call passes it as undefined, so
   * callers that track coordinates must clear them when it is absent.
   * granularity arrives only on a completed selection too: "zip" means the
   * value was replaced with the bare five-digit ZIP for the partial-ballot
   * flow, "region" means the selection is an area with no supported flow —
   * callers offering guidance must clear it whenever it is absent. */
  onChange: (value: string, location?: AddressLocation | null, granularity?: "address" | "zip" | "region") => void;
  inputId: string;
  placeholder?: string;
};

export function AddressAutocomplete({ value, onChange, inputId, placeholder }: AddressAutocompleteProps) {
  const { suggestions, enabled, onInputChanged, selectSuggestion, clearSuggestions, warmup } =
    useAddressSuggestions();

  // The landing page is prerendered: text typed before hydration exists only
  // in the DOM. Adopt it the same way a keystroke would land — into the
  // caller's state AND the suggestion machinery, so autocomplete wakes up
  // instead of staying dormant until the next keystroke.
  useAdoptPreHydrationValue(inputId, (adopted) => {
    onChange(adopted);
    onInputChanged(adopted);
  });

  async function onSelect(suggestion: AddressSuggestion | null) {
    if (!suggestion) {
      return;
    }
    // Optimistically show the picked description, then upgrade to the full
    // retrieved address (falls back to the description if retrieve fails).
    onChange(suggestion.description);
    const retrieved = await selectSuggestion(suggestion);
    if (!retrieved) {
      return;
    }
    if (retrieved.granularity === "zip" && retrieved.postal_code) {
      // A picked ZIP becomes the bare five digits — exactly what the resolve
      // endpoint's partial-ballot path takes.
      onChange(retrieved.postal_code, null, "zip");
      return;
    }
    onChange(retrieved.address, retrieved.location, retrieved.granularity);
  }

  return (
    <Combobox<AddressSuggestion | null>
      value={null}
      onChange={(suggestion) => {
        void onSelect(suggestion);
      }}
      immediate={false}
    >
      <div className="relative">
        <ComboboxInput
          id={inputId}
          className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          placeholder={placeholder}
          autoComplete="street-address"
          value={value}
          onChange={(event) => {
            onChange(event.target.value);
            onInputChanged(event.target.value);
          }}
          onFocus={() => warmup()}
          onBlur={() => clearSuggestions()}
        />
        {enabled && suggestions.length > 0 ? (
          <ComboboxOptions
            static
            className="absolute z-10 mt-1 w-full overflow-hidden rounded-xl border border-line bg-white shadow-md"
          >
            {suggestions.map((suggestion) => (
              <ComboboxOption
                key={suggestion.place_id}
                value={suggestion}
                className="cursor-pointer px-3 py-2 text-sm data-focus:bg-surface"
              >
                <span className="font-semibold text-ink">{suggestion.main_text}</span>{" "}
                <span className="text-ink-soft">{suggestion.secondary_text}</span>
              </ComboboxOption>
            ))}
            {/* Required attribution when predictions are shown without a map. */}
            <li className="border-t border-line px-3 py-1.5 text-right text-[10px] text-ink-soft">
              powered by Google
            </li>
          </ComboboxOptions>
        ) : null}
      </div>
    </Combobox>
  );
}

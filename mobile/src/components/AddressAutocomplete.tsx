import type { AddressSuggestion } from "@voteapp/api-client";
import { useAddressSuggestions } from "@voteapp/api-client";
import { Pressable, Text, TextInput, View } from "react-native";

// Port of the web AddressAutocomplete. The suggestion machinery
// (useAddressSuggestions: debounce, AbortController, session token) is the
// shared hook unchanged; only the dropdown rendering is native. Autocomplete
// failing must never block the form: the input stays a plain controlled text
// field and the suggestion list simply disappears.

type AddressAutocompleteProps = {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  /** Screen-reader label. React Native does not associate a sibling <Text>
   * with the input, so callers whose visible label differs from the default
   * must pass it here or screen readers announce the wrong thing. */
  accessibilityLabel?: string;
};

export function AddressAutocomplete({
  value,
  onChange,
  placeholder,
  accessibilityLabel = "Your address",
}: AddressAutocompleteProps) {
  const { suggestions, enabled, onInputChanged, selectSuggestion, clearSuggestions, warmup } =
    useAddressSuggestions();

  async function onSelect(suggestion: AddressSuggestion) {
    // Optimistically show the picked description, then upgrade to the full
    // retrieved address (falls back to the description if retrieve fails).
    onChange(suggestion.description);
    clearSuggestions();
    const address = await selectSuggestion(suggestion);
    if (address) {
      onChange(address);
    }
  }

  return (
    <View>
      <TextInput
        className="mt-1 w-full rounded-md border border-line bg-white px-3 py-3 text-ink"
        placeholder={placeholder}
        placeholderTextColor="#717171"
        autoComplete="street-address"
        accessibilityLabel={accessibilityLabel}
        value={value}
        onChangeText={(text) => {
          onChange(text);
          onInputChanged(text);
        }}
        onFocus={() => warmup()}
      />
      {enabled && suggestions.length > 0 ? (
        <View className="mt-1 overflow-hidden rounded-xl border border-line bg-white">
          {suggestions.map((suggestion) => (
            <Pressable
              key={suggestion.place_id}
              className="px-3 py-2 active:bg-surface"
              onPress={() => {
                void onSelect(suggestion);
              }}
            >
              <Text className="text-sm">
                <Text className="font-semibold text-ink">{suggestion.main_text}</Text>{" "}
                <Text className="text-ink-soft">{suggestion.secondary_text}</Text>
              </Text>
            </Pressable>
          ))}
          {/* Required attribution when predictions are shown without a map. */}
          <View className="border-t border-line px-3 py-1.5">
            <Text className="text-right text-[10px] text-ink-soft">powered by Google</Text>
          </View>
        </View>
      ) : null}
    </View>
  );
}

import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, apiRequest } from "./client";
import type { AddressAutocompleteResponse, AddressRetrieveResponse, AddressSuggestion } from "./types";

// Autocomplete state machine per docs/address-autocomplete-frontend.md:
// - fresh crypto.randomUUID() session token at the first keystroke that
//   triggers a suggest call; the SAME token on every suggest and the final
//   retrieve; token dies after a retrieve or when the entry is abandoned.
// - 275ms debounce, 3-char minimum, in-flight requests aborted when a new
//   one fires, out-of-order responses ignored.
// - Any failure hides the dropdown and never blocks manual entry; the
//   "not configured" 500 disables autocomplete for the session.

export const SUGGEST_DEBOUNCE_MS = 275;
export const SUGGEST_MIN_CHARS = 3;

export type UseAddressSuggestionsResult = {
  suggestions: AddressSuggestion[];
  /** False once the backend reports autocomplete is not configured. */
  enabled: boolean;
  onInputChanged: (text: string) => void;
  /** Resolves to the full address string, or null when retrieve failed. */
  selectSuggestion: (suggestion: AddressSuggestion) => Promise<string | null>;
  clearSuggestions: () => void;
};

export function useAddressSuggestions(): UseAddressSuggestionsResult {
  const [suggestions, setSuggestions] = useState<AddressSuggestion[]>([]);
  const [enabled, setEnabled] = useState(true);
  const sessionTokenRef = useRef<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const requestSeqRef = useRef(0);
  const enabledRef = useRef(true);

  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
      abortRef.current?.abort();
    };
  }, []);

  const fireSuggest = useCallback(async (input: string) => {
    // Google Places autocomplete session token: groups suggest+retrieve
    // calls for billing. Uniqueness matters, cryptographic strength does not.
    // React Native's Hermes has no crypto.randomUUID — the mobile app must
    // polyfill it at startup (expo-crypto's Crypto.randomUUID), the standard
    // Expo approach. Web and Node 19+ provide it natively.
    sessionTokenRef.current ??= crypto.randomUUID();
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const seq = ++requestSeqRef.current;
    try {
      const response = await apiRequest<AddressAutocompleteResponse>("/api/address/autocomplete", {
        method: "POST",
        body: { input, session_token: sessionTokenRef.current },
        signal: controller.signal,
      });
      if (seq === requestSeqRef.current) {
        setSuggestions(response.suggestions);
      }
    } catch (error) {
      if (seq !== requestSeqRef.current) {
        return; // Superseded; newer request owns the state.
      }
      setSuggestions([]);
      if (error instanceof ApiError && error.status === 500) {
        // "Address autocomplete is not configured" — hide the feature
        // entirely and fall back to plain input for the session.
        enabledRef.current = false;
        setEnabled(false);
      }
      // Every other failure (429, 5xx, network, abort-timeout): no dropdown,
      // user keeps typing.
    }
  }, []);

  const onInputChanged = useCallback(
    (text: string) => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
      const trimmed = text.trim();
      if (trimmed.length === 0) {
        // Entry abandoned: the session is dead; the next entry gets a fresh
        // token.
        sessionTokenRef.current = null;
      }
      if (!enabledRef.current || trimmed.length < SUGGEST_MIN_CHARS) {
        abortRef.current?.abort();
        requestSeqRef.current += 1;
        setSuggestions([]);
        return;
      }
      debounceRef.current = setTimeout(() => {
        void fireSuggest(trimmed);
      }, SUGGEST_DEBOUNCE_MS);
    },
    [fireSuggest]
  );

  const selectSuggestion = useCallback(async (suggestion: AddressSuggestion): Promise<string | null> => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    abortRef.current?.abort();
    requestSeqRef.current += 1;
    const sessionToken = sessionTokenRef.current;
    setSuggestions([]);
    if (!sessionToken) {
      return null;
    }
    // Billing-relevant: the session ends the moment the final retrieve is
    // issued. Clear BEFORE awaiting so typing during the in-flight retrieve
    // starts a fresh session instead of reusing the spent token (the local
    // sessionToken copy still rides on the retrieve request).
    sessionTokenRef.current = null;
    try {
      const response = await apiRequest<AddressRetrieveResponse>("/api/address/autocomplete/retrieve", {
        method: "POST",
        body: { place_id: suggestion.place_id, session_token: sessionToken },
      });
      return response.address;
    } catch {
      // Retrieve failed; the user still has their typed text and can submit
      // it manually. The token stays dead either way — the session state is
      // indeterminate and a fresh session costs nothing.
      return null;
    }
  }, []);

  const clearSuggestions = useCallback(() => {
    setSuggestions([]);
  }, []);

  return { suggestions, enabled, onInputChanged, selectSuggestion, clearSuggestions };
}

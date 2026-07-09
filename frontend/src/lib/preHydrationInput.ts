import { useEffect } from "react";

// SSR ships every form as working HTML before React hydrates, so a fast
// visitor (or a test runner, or browser autofill) can put text into an input
// during the gap. Those keystrokes land in the DOM but fire no React
// onChange, so controlled state silently misses them — a register submit
// then drops the first name that is visibly sitting in the field. These
// hooks run at hydration (mount) and fold whatever is already in the DOM
// back into state. After that the normal controlled flow owns the input.
//
// Lookup is by id rather than ref so pages can rescue inputs rendered by
// wrapper components (Headless UI's ComboboxInput, LegalGate) without
// threading refs through them.

export function useAdoptPreHydrationValue(inputId: string, adopt: (value: string) => void) {
  useEffect(() => {
    const element = document.getElementById(inputId);
    if (element instanceof HTMLInputElement && element.value) {
      adopt(element.value);
    }
    // Mount-only by design: this is a hydration rescue, not a subscription.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}

export function useAdoptPreHydrationChecked(inputId: string, adopt: (checked: boolean) => void) {
  useEffect(() => {
    const element = document.getElementById(inputId);
    if (element instanceof HTMLInputElement && element.checked) {
      adopt(element.checked);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}

import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// With vitest globals disabled, testing-library cannot auto-register its
// cleanup hook; do it explicitly so renders don't leak across tests.
afterEach(() => {
  cleanup();
});

// jsdom has no ResizeObserver; Headless UI's Combobox observes elements on
// mount, which otherwise surfaces as an unhandled ReferenceError and a
// failing (exit 1) test run even when every assertion passes.
class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

globalThis.ResizeObserver ??= ResizeObserverStub as unknown as typeof ResizeObserver;

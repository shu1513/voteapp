import { afterEach, describe, expect, it, vi } from "vitest";
import { render } from "@testing-library/react";
import { useAdoptPreHydrationChecked, useAdoptPreHydrationValue } from "./preHydrationInput";

// jsdom cannot truly hydrate, so the pre-hydration state is simulated by
// planting the input in the document before the hook's component mounts —
// same shape the hook sees in production: a DOM input that already holds a
// value React state never learned about.

function plantInput(attrs: { id: string; type?: string; value?: string; checked?: boolean }) {
  const input = document.createElement("input");
  input.id = attrs.id;
  input.type = attrs.type ?? "text";
  if (attrs.value !== undefined) {
    input.value = attrs.value;
  }
  if (attrs.checked !== undefined) {
    input.checked = attrs.checked;
  }
  document.body.appendChild(input);
  return input;
}

function HookHost({ hook }: { hook: () => void }) {
  hook();
  return null;
}

afterEach(() => {
  document.body.innerHTML = "";
});

describe("useAdoptPreHydrationValue", () => {
  it("adopts a value already sitting in the DOM input", () => {
    plantInput({ id: "register-first-name", value: "Smoke" });
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationValue("register-first-name", adopt)} />);
    expect(adopt).toHaveBeenCalledWith("Smoke");
  });

  it("does nothing for an empty input", () => {
    plantInput({ id: "register-first-name", value: "" });
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationValue("register-first-name", adopt)} />);
    expect(adopt).not.toHaveBeenCalled();
  });

  it("does nothing when the input does not exist", () => {
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationValue("missing", adopt)} />);
    expect(adopt).not.toHaveBeenCalled();
  });
});

describe("useAdoptPreHydrationChecked", () => {
  it("adopts a checked checkbox", () => {
    plantInput({ id: "signup-terms", type: "checkbox", checked: true });
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationChecked("signup-terms", adopt)} />);
    expect(adopt).toHaveBeenCalledWith(true);
  });

  it("does nothing for an unchecked checkbox", () => {
    plantInput({ id: "signup-terms", type: "checkbox", checked: false });
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationChecked("signup-terms", adopt)} />);
    expect(adopt).not.toHaveBeenCalled();
  });
});

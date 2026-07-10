import { useState } from "react";
import { renderToString } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { useAdoptPreHydrationChecked, useAdoptPreHydrationValue } from "./preHydrationInput";

// Two layers of coverage. The `plantInput` suites exercise the hooks'
// mechanics in isolation (lookup, guards). The hydration suites reproduce
// the actual production sequence: server HTML → user modifies the DOM
// before React attaches → hydrateRoot → the hook folds the DOM state back
// into controlled state.

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

// Minimal controlled form matching the register-page shape: state starts
// empty, the hook rescues what hydration finds in the DOM.
function ControlledNameForm() {
  const [name, setName] = useState("");
  useAdoptPreHydrationValue("hydrate-name", setName);
  return (
    <div>
      <input id="hydrate-name" value={name} onChange={(event) => setName(event.target.value)} />
      <output data-testid="state-value">{name}</output>
    </div>
  );
}

function ControlledCheckboxForm() {
  const [accepted, setAccepted] = useState(false);
  useAdoptPreHydrationChecked("hydrate-terms", setAccepted);
  return (
    <div>
      <input
        id="hydrate-terms"
        type="checkbox"
        checked={accepted}
        onChange={(event) => setAccepted(event.target.checked)}
      />
      <output data-testid="state-checked">{String(accepted)}</output>
    </div>
  );
}

// The production sequence, deterministically: render the server HTML, let a
// "user" mutate the DOM before React attaches, then hydrate for real.
function hydrateAfterDomEdit(ui: React.ReactElement, edit: (container: HTMLElement) => void) {
  const container = document.createElement("div");
  container.innerHTML = renderToString(ui);
  document.body.appendChild(container);
  edit(container);
  return render(ui, { container, hydrate: true });
}

describe("hydration rescue (value)", () => {
  it("folds text typed before hydration into controlled state", () => {
    hydrateAfterDomEdit(<ControlledNameForm />, (container) => {
      const input = container.querySelector("input");
      if (input) {
        input.value = "Smoke";
      }
    });
    expect(screen.getByTestId("state-value").textContent).toBe("Smoke");
    expect((document.getElementById("hydrate-name") as HTMLInputElement).value).toBe("Smoke");
  });

  it("leaves state empty when nothing was typed before hydration", () => {
    hydrateAfterDomEdit(<ControlledNameForm />, () => {});
    expect(screen.getByTestId("state-value").textContent).toBe("");
  });
});

describe("hydration rescue (checked)", () => {
  it("folds a checkbox clicked before hydration into controlled state", () => {
    hydrateAfterDomEdit(<ControlledCheckboxForm />, (container) => {
      const input = container.querySelector("input");
      if (input) {
        input.checked = true;
      }
    });
    expect(screen.getByTestId("state-checked").textContent).toBe("true");
    expect((document.getElementById("hydrate-terms") as HTMLInputElement).checked).toBe(true);
  });

  it("leaves the box unchecked when it was not clicked before hydration", () => {
    hydrateAfterDomEdit(<ControlledCheckboxForm />, () => {});
    expect(screen.getByTestId("state-checked").textContent).toBe("false");
  });
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

  it("does nothing when the checkbox does not exist", () => {
    const adopt = vi.fn();
    render(<HookHost hook={() => useAdoptPreHydrationChecked("missing", adopt)} />);
    expect(adopt).not.toHaveBeenCalled();
  });
});

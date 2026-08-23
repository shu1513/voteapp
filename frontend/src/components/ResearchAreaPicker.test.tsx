import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ResearchAreaPicker } from "./ResearchAreaPicker";

function area(id: string, slug: string, name: string) {
  return { id, slug, name, description: null };
}

describe("ResearchAreaPicker", () => {
  it("lists unselected areas in public-salience order, unranked areas last", () => {
    render(
      <ResearchAreaPicker
        // Deliberately shuffled; the catalog API returns alphabetical order.
        areas={[
          area("a-legal", "legal_competence", "Legal Competence"),
          area("a-env", "environment_and_public_health", "Environment and Public Health"),
          area("a-health", "healthcare_affordability", "Healthcare Affordability"),
          area("a-wealth", "reduce_wealth_gap", "Reduce Wealth Gap"),
        ]}
        ranked={[]}
        disabled={false}
        onChange={() => {}}
      />
    );

    const names = screen.getAllByRole("button").map((button) => button.textContent);
    expect(names).toEqual([
      "Healthcare Affordability",
      "Environment and Public Health",
      "Reduce Wealth Gap",
      "Legal Competence",
    ]);
  });

  it("adds a card as a support / no-line row and reports toggles as one complete next list", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    const areas = [
      area("a-health", "healthcare_affordability", "Healthcare Affordability"),
      area("a-ethics", "integrity_and_ethics", "Integrity and Ethics"),
    ];
    const { rerender } = render(<ResearchAreaPicker areas={areas} ranked={[]} disabled={false} onChange={onChange} />);

    await user.click(screen.getByRole("button", { name: "Healthcare Affordability" }));
    expect(onChange).toHaveBeenLastCalledWith([
      { research_area_id: "a-health", direction: "support", hard_veto: false },
    ]);

    const ranked = [
      { research_area_id: "a-health", direction: "support" as const, hard_veto: false },
      { research_area_id: "a-ethics", direction: "support" as const, hard_veto: false },
    ];
    rerender(<ResearchAreaPicker areas={areas} ranked={ranked} disabled={false} onChange={onChange} />);

    await user.click(screen.getByRole("button", { name: "Oppose", pressed: false }));
    expect(onChange).toHaveBeenLastCalledWith([
      { research_area_id: "a-health", direction: "oppose", hard_veto: false },
      ranked[1],
    ]);

    await user.click(screen.getByRole("button", { name: /Must.*Healthcare/ }));
    expect(onChange).toHaveBeenLastCalledWith([{ ...ranked[0], hard_veto: true }, ranked[1]]);

    // Ethics: no direction control (an ethics record is always a strike),
    // and the must reads as "skip anyone with such a record".
    expect(screen.getAllByRole("button", { name: "Support" })).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: /Skip candidates with any integrity or ethics record/ }));
    expect(onChange).toHaveBeenLastCalledWith([ranked[0], { ...ranked[1], hard_veto: true }]);

    await user.click(screen.getByRole("button", { name: "Remove Healthcare Affordability" }));
    expect(onChange).toHaveBeenLastCalledWith([ranked[1]]);
  });
});

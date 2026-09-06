import { describe, expect, it } from "vitest";

import {
  MANUAL_PROTECTED_LINK_RETURNING,
  assertLinkWriteNotBlocked,
  manualProtectedLinkAssignments,
  manualProtectedRetireCondition,
} from "../../../src/pipeline/finance/manualLinkProtection.js";

describe("manualLinkProtection", () => {
  it("keeps status and source only when automation writes over a manual row", () => {
    const sql = manualProtectedLinkAssignments("xx_candidate_finance_links");
    expect(sql).toContain(
      "link_status = CASE\n          WHEN xx_candidate_finance_links.link_source = 'manual' AND EXCLUDED.link_source <> 'manual' THEN xx_candidate_finance_links.link_status\n          ELSE EXCLUDED.link_status\n        END"
    );
    expect(sql).toContain(
      "link_source = CASE\n          WHEN xx_candidate_finance_links.link_source = 'manual' AND EXCLUDED.link_source <> 'manual' THEN xx_candidate_finance_links.link_source\n          ELSE EXCLUDED.link_source\n        END"
    );
    expect(() => manualProtectedLinkAssignments("Bad Table")).toThrow("Invalid finance links table identifier");
    expect(MANUAL_PROTECTED_LINK_RETURNING).toBe("id, link_status, link_source");
  });

  it("never lets automation retire a manual identity; a manual write may", () => {
    expect(manualProtectedRetireCondition("$4")).toBe("(link_source IS DISTINCT FROM 'manual' OR $4 = 'manual')");
    expect(() => manualProtectedRetireCondition("4")).toThrow("Invalid retire-condition parameter");
  });

  it("fails closed only for an automatic write that landed on an operator-disabled manual row", () => {
    const blocked = { id: "l1", link_status: "inactive", link_source: "manual" };
    expect(() => assertLinkWriteNotBlocked("Zetaland", blocked, "state_bulk")).toThrow(
      "Zetaland automatic finance link matches an operator-disabled manual link"
    );
    // Active manual row: automation reuses it.
    expect(() => assertLinkWriteNotBlocked("Zetaland", { ...blocked, link_status: "active" }, "state_bulk")).not.toThrow();
    // Automatic row, whatever its status: not protected.
    expect(() => assertLinkWriteNotBlocked("Zetaland", { ...blocked, link_source: "state_bulk" }, "state_bulk")).not.toThrow();
    // Manual write: applies, including deactivation.
    expect(() => assertLinkWriteNotBlocked("Zetaland", blocked, "manual")).not.toThrow();
    // Mocks that only return an id (no status columns) are not treated as blocked.
    expect(() => assertLinkWriteNotBlocked("Zetaland", { id: "l1" }, "state_bulk")).not.toThrow();
    expect(() => assertLinkWriteNotBlocked("Zetaland", undefined, "state_bulk")).not.toThrow();
  });
});

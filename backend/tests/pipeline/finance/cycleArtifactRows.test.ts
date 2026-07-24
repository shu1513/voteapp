import { describe, expect, it } from "vitest";

import { mergeCycleArtifactRows } from "../../../src/pipeline/finance/cycleArtifactRows.js";

describe("mergeCycleArtifactRows", () => {
  it("merges an artifact larger than the JavaScript function argument limit", () => {
    const artifactRows = Array.from({ length: 200_000 }, (_, index) => ({ id: String(index) }));

    const rows = mergeCycleArtifactRows({
      artifacts: [artifactRows],
      rowIdentity: (row) => row.id,
    });

    expect(rows).toHaveLength(200_000);
    expect(rows[0]).toEqual({ id: "0" });
    expect(rows.at(-1)).toEqual({ id: "199999" });
  });

  it("counts a stable row identity once across years and keeps the newer row", () => {
    const priorYearRow = { id: "same-row", value: "prior" };
    const currentYearRow = { id: "same-row", value: "current" };

    const rows = mergeCycleArtifactRows({
      artifacts: [[priorYearRow], [currentYearRow]],
      rowIdentity: (row) => row.id,
    });

    expect(rows).toEqual([currentYearRow]);
  });

  it("does not collapse repeated rows inside the winning official artifact", () => {
    const duplicateOne = { id: "same-row", sequence: 1 };
    const duplicateTwo = { id: "same-row", sequence: 2 };

    const rows = mergeCycleArtifactRows({
      artifacts: [[{ id: "same-row", sequence: 0 }], [duplicateOne, duplicateTwo]],
      rowIdentity: (row) => row.id,
    });

    expect(rows).toEqual([duplicateOne, duplicateTwo]);
  });

  it("computes each row identity once", () => {
    const artifacts = [[{ id: "prior" }, { id: "same" }], [{ id: "same" }]];
    let identityCallCount = 0;

    const rows = mergeCycleArtifactRows({
      artifacts,
      rowIdentity: (row) => {
        identityCallCount += 1;
        return row.id;
      },
    });

    expect(rows).toEqual([{ id: "prior" }, { id: "same" }]);
    expect(identityCallCount).toBe(3);
  });
});

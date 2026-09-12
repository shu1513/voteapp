import { describe, expect, it } from "vitest";

import { pointInPolygon, samplePoints } from "../../src/scripts/majorCityCoverage.js";

const square = { rings: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]] };
const squareWithHole = {
  rings: [
    [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]],
    [[0.25, 0.25], [0.75, 0.25], [0.75, 0.75], [0.25, 0.75], [0.25, 0.25]],
  ],
};

describe("majorCityCoverage geometry", () => {
  it("classifies points against a ring with a hole", () => {
    expect(pointInPolygon(0.1, 0.1, square)).toBe(true);
    expect(pointInPolygon(1.5, 0.5, square)).toBe(false);
    expect(pointInPolygon(0.5, 0.5, squareWithHole)).toBe(false);
    expect(pointInPolygon(0.1, 0.9, squareWithHole)).toBe(true);
  });

  it("samples only points inside the polygon", () => {
    const all = samplePoints(square);
    expect(all).toHaveLength(3600);
    const withHole = samplePoints(squareWithHole);
    expect(withHole.length).toBeLessThan(all.length);
    expect(withHole.every(([lon, lat]) => pointInPolygon(lon, lat, squareWithHole))).toBe(true);
  });
});

import { describe, expect, it } from "vitest";

import { usLatestLocalDateIso } from "../../src/utils/usLocalDate.js";

describe("usLatestLocalDateIso", () => {
  it("returns the UTC date while every US timezone is on the same date", () => {
    // 2026-07-13 22:00 UTC = 12:00 in Honolulu on the 13th.
    expect(usLatestLocalDateIso(new Date("2026-07-13T22:00:00Z"))).toBe("2026-07-13");
  });

  it("stays on the US-local date after the UTC calendar rolls over", () => {
    // 2026-07-14 00:11 UTC = 2026-07-13 17:11 in Los Angeles / 14:11 in
    // Honolulu — the exact live case: a records checkpoint stamped from the
    // UTC date claimed 2026-07-14 as researched on the evening of the 13th.
    expect(usLatestLocalDateIso(new Date("2026-07-14T00:11:00Z"))).toBe("2026-07-13");
  });

  it("rolls to the next date only once Hawaii does", () => {
    // 2026-07-14 10:00 UTC = 2026-07-14 00:00 in Honolulu.
    expect(usLatestLocalDateIso(new Date("2026-07-14T10:00:00Z"))).toBe("2026-07-14");
    expect(usLatestLocalDateIso(new Date("2026-07-14T09:59:59Z"))).toBe("2026-07-13");
  });
});

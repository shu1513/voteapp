import { describe, expect, it } from "vitest";
import {
  buildPickCardOgSvg,
  formatElectionDateShort,
  pickCardOgHeadline,
  renderPickCardOgImage,
} from "../../src/api/pickCardOgImage.js";

describe("formatElectionDateShort", () => {
  it("shortens an ISO election date", () => {
    expect(formatElectionDateShort("2026-11-03")).toBe("Nov 3, 2026");
    expect(formatElectionDateShort("2027-01-12")).toBe("Jan 12, 2027");
  });

  it("passes through anything that is not an ISO date", () => {
    expect(formatElectionDateShort("soon")).toBe("soon");
  });
});

describe("pickCardOgHeadline", () => {
  it("uses the exact named share pitch", () => {
    expect(pickCardOgHeadline("Shu", "2026-11-03")).toBe("See Shu's picks for Nov 3, 2026 Elections");
  });

  it("falls back to an unnamed pitch when the owner hides their name", () => {
    expect(pickCardOgHeadline(null, "2026-11-03")).toBe("See the picks for Nov 3, 2026 Elections");
  });
});

describe("buildPickCardOgSvg", () => {
  it("escapes markup in the first name instead of injecting it", () => {
    const svg = buildPickCardOgSvg({ firstName: '<img src="x">&', electionDate: "2026-11-03" });
    expect(svg).not.toContain("<img");
    expect(svg).toContain("&lt;img");
    expect(svg).toContain("&amp;");
  });

  it("keeps the whole date on one line", () => {
    const svg = buildPickCardOgSvg({ firstName: "Shu", electionDate: "2026-11-03" });
    // Non-breaking spaces bind "Nov 3, 2026" into a single wrap token.
    expect(svg).toContain("Nov 3, 2026");
  });

  it("lays out the longest legal first name without truncating", () => {
    const name = "Maria Guadalupe Fernanda De Los Angeles Rodriguez Villanueva Echeverria Ortiz".slice(0, 80);
    const svg = buildPickCardOgSvg({ firstName: name, electionDate: "2026-11-03" });
    expect(svg).toContain("Villanueva");
    expect(svg).toContain("Elections");
  });
});

describe("renderPickCardOgImage", () => {
  it("renders a 1200x630 PNG", () => {
    const png = renderPickCardOgImage({ firstName: "Shu", electionDate: "2026-11-03" });
    // PNG signature.
    expect(png.subarray(0, 8).toString("hex")).toBe("89504e470d0a1a0a");
    // IHDR width/height are big-endian uint32 at bytes 16 and 20.
    expect(png.readUInt32BE(16)).toBe(1200);
    expect(png.readUInt32BE(20)).toBe(630);
  });
});

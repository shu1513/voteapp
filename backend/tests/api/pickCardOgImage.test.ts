import { describe, expect, it } from "vitest";
import {
  buildPickCardOgSvg,
  formatElectionDateShort,
  measureTextPx,
  pickCardOgHeadline,
  renderPickCardOgImage,
} from "../../src/api/pickCardOgImage.js";

/** Every headline line of an SVG with its font size, brand label excluded.
 * Entities are unescaped so the text measures as it renders. */
function headlineLines(svg: string): Array<{ fontSize: number; text: string }> {
  const unescape = (text: string) =>
    text
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'")
      .replace(/&amp;/g, "&");
  return [...svg.matchAll(/font-size="(\d+)"[^>]*>([^<]*)<\/text>/g)]
    .map((match) => ({ fontSize: Number(match[1]), text: unescape(match[2]) }))
    .filter((line) => line.text !== "Elections Simplified");
}

describe("measureTextPx", () => {
  it("reads real advance widths from the vendored font", () => {
    // Inter Bold's W is close to 1em; i far narrower. Exact values come from
    // the font tables, so only sanity-bound them.
    const wide = measureTextPx("W", 100);
    const narrow = measureTextPx("i", 100);
    expect(wide).toBeGreaterThan(80);
    expect(wide).toBeLessThan(120);
    expect(narrow).toBeLessThan(wide / 2);
  });

  it("gives glyphs outside the font a pessimistic fallback width", () => {
    expect(measureTextPx("😀", 100)).toBe(140);
  });
});

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

  it("strips XML-invalid control characters instead of failing the parse", () => {
    const svg = buildPickCardOgSvg({ firstName: `Av${String.fromCharCode(1)}a`, electionDate: "2026-11-03" });
    expect(svg).toContain("Ava&apos;s picks");
    expect(svg).not.toContain(String.fromCharCode(1));
  });

  it("strips the XML-invalid noncharacters U+FFFE and U+FFFF", () => {
    for (const codePoint of [0xfffe, 0xffff]) {
      const svg = buildPickCardOgSvg({
        firstName: `Av${String.fromCharCode(codePoint)}a`,
        electionDate: "2026-11-03",
      });
      expect(svg).toContain("Ava&apos;s picks");
      expect(svg).not.toContain(String.fromCharCode(codePoint));
    }
  });

  it("never splits a surrogate pair when chunking an emoji name", () => {
    // 40 emoji = 80 UTF-16 units: passes length validation, forces chunking.
    const svg = buildPickCardOgSvg({ firstName: "😀".repeat(40), electionDate: "2026-11-03" });
    const loneSurrogate = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
    expect(loneSurrogate.test(svg)).toBe(false);
    expect(svg).toContain("😀");
  });

  it("truncates the name, not the message, when the headline cannot fit", () => {
    // The fixed suffix is the image's purpose; an 80-W name must lose W's,
    // not "picks for … Elections".
    const svg = buildPickCardOgSvg({ firstName: "W".repeat(80), electionDate: "2026-11-03" });
    expect(svg).toContain("…");
    expect(svg).toContain("picks for");
    expect(svg).toContain("Elections</text>");
  });

  it("hard-splits a single word wider than a line so no line overflows", () => {
    // 44 W's: wide enough that no font size fits it unbroken, small enough
    // that its chunks + the fixed suffix fit four lines without truncation.
    const svg = buildPickCardOgSvg({ firstName: "W".repeat(44), electionDate: "2026-11-03" });
    const lines = headlineLines(svg);
    expect(lines.filter((line) => /^W+$/.test(line.text)).length).toBeGreaterThan(0);
    for (const line of lines) {
      expect(measureTextPx(line.text, line.fontSize)).toBeLessThanOrEqual(1020);
    }
  });

  it("keeps every line inside the text region for adversarially wide names", () => {
    // Layout uses measured widths, so this must hold for any input at all.
    for (const firstName of ["W".repeat(80), "😀".repeat(40), "MWMWMWMWMW", "Iiii", null]) {
      const svg = buildPickCardOgSvg({ firstName, electionDate: "2026-11-03" });
      for (const line of headlineLines(svg)) {
        expect(measureTextPx(line.text, line.fontSize)).toBeLessThanOrEqual(1020);
      }
    }
  });
});

describe("renderPickCardOgImage", () => {
  it("renders a 1200x630 PNG", async () => {
    const png = await renderPickCardOgImage({ firstName: "Shu", electionDate: "2026-11-03" });
    // PNG signature.
    expect(png.subarray(0, 8).toString("hex")).toBe("89504e470d0a1a0a");
    // IHDR width/height are big-endian uint32 at bytes 16 and 20.
    expect(png.readUInt32BE(16)).toBe(1200);
    expect(png.readUInt32BE(20)).toBe(630);
  });

  it("renders a control-character name instead of returning a parse failure", async () => {
    const png = await renderPickCardOgImage({
      firstName: `Av${String.fromCharCode(1)}a`,
      electionDate: "2026-11-03",
    });
    expect(png.subarray(0, 8).toString("hex")).toBe("89504e470d0a1a0a");
  });

  it("serves repeat requests for the same card from the cache", async () => {
    const first = renderPickCardOgImage({ firstName: "CacheProbe", electionDate: "2026-11-03" });
    const second = renderPickCardOgImage({ firstName: "CacheProbe", electionDate: "2026-11-03" });
    // Same promise, not merely equal bytes: concurrent misses share one render.
    expect(second).toBe(first);
    await first;
  });
});

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { renderAsync } from "@resvg/resvg-js";

// The per-share link-preview image for /picks/<token>. Scrapers (iMessage,
// WhatsApp, Facebook, X) show the og:image far larger than the title text, so
// a share only *feels* personal if the picture itself carries the owner's
// name. This renders the same layout as the static frontend/public/og-card.png
// (white card, rausch top bar, brand label, bold dark headline) with a
// per-share headline: "See Shu's picks for Nov 3, 2026 Elections".
//
// Rendering is SVG composed here → PNG via resvg (prebuilt native binary, no
// system dependencies). The headline font is vendored at
// backend/assets/fonts/Inter_700Bold.ttf (SIL OFL, license alongside) because
// the deploy host's system fonts are not guaranteed; system fonts stay loaded
// as a fallback so a name outside Inter's coverage (e.g. CJK) degrades to a
// host font instead of tofu.

const WIDTH = 1200;
const HEIGHT = 630;
const MARGIN_X = 90;
const USABLE_WIDTH = WIDTH - MARGIN_X * 2;

// Colors mirror frontend/src/index.css: --color-rausch and --color-ink.
const BRAND_COLOR = "#ff385c";
const INK_COLOR = "#222222";

const moduleDir = dirname(fileURLToPath(import.meta.url));
const FONT_PATH = join(moduleDir, "../../assets/fonts/Inter_700Bold.ttf");

// Touch the file at module load: a missing font should fail server startup
// loudly, not surface as a per-request 500 the first time a scraper asks.
// resvg itself loads from the path (its 2.x API takes fontFiles, not buffers).
readFileSync(FONT_PATH);

const MONTH_ABBREVIATIONS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/** "2026-11-03" → "Nov 3, 2026". String math, not Date: a Date round-trip
 * would shift the day near midnight depending on the server timezone. */
export function formatElectionDateShort(electionDate: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(electionDate);
  if (!match) {
    return electionDate;
  }
  const [, year, month, day] = match;
  const monthName = MONTH_ABBREVIATIONS[Number(month) - 1];
  return monthName ? `${monthName} ${Number(day)}, ${year}` : electionDate;
}

/** The exact share pitch. The unnamed form covers shares whose owner kept
 * their name off the card (show_owner_name = false). */
export function pickCardOgHeadline(firstName: string | null, electionDate: string): string {
  const date = formatElectionDateShort(electionDate);
  return firstName
    ? `See ${firstName}'s picks for ${date} Elections`
    : `See the picks for ${date} Elections`;
}

function escapeXml(text: string): string {
  return (
    text
      // C0 controls and DEL are invalid XML 1.0 even escaped — first-name
      // validation only checks trim + length, so U+0001 etc. reach this
      // boundary and would fail the SVG parse (a 500) if kept.
      .replace(/[\u0000-\u001f\u007f]/g, "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;")
  );
}

// Greedy word wrap against an estimated glyph width. 0.56em approximates the
// average advance of Inter Bold for mixed-case English; resvg gives us no
// text-measurement API, so the fit check is an estimate with the margin
// built into the factor. Wide-glyph-heavy real names can still exceed the
// text region by a little, but the 90px side margins absorb that; only the
// pathological cases below get special handling.
const AVG_GLYPH_WIDTH_EM = 0.56;

// A single word longer than a whole line (an 80-char first name with no
// spaces — garbage input, but length validation admits it) is hard-split
// into chunks that each get their own line. Chunks are sized at 1.1em per
// glyph — Inter Bold's widest glyph, W, advances ~1.05em — because a chunk
// is by definition estimate-hostile, and an overflowing line clips at the
// canvas edge. The caller's font-size ladder absorbs the extra lines; if
// even the smallest size cannot hold them, its four-line truncation drops
// tail chunks, which is the right degradation for such a name.
const CHUNK_GLYPH_WIDTH_EM = 1.1;

function wrapHeadline(text: string, fontSize: number): string[] {
  const maxChars = Math.floor(USABLE_WIDTH / (fontSize * AVG_GLYPH_WIDTH_EM));
  const chunkChars = Math.max(1, Math.floor(USABLE_WIDTH / (fontSize * CHUNK_GLYPH_WIDTH_EM)));
  const lines: string[] = [];
  let current = "";
  const flush = () => {
    if (current.length > 0) {
      lines.push(current);
      current = "";
    }
  };
  for (const word of text.split(" ")) {
    if (word.length > maxChars) {
      flush();
      for (let start = 0; start < word.length; start += chunkChars) {
        lines.push(word.slice(start, start + chunkChars));
      }
      continue;
    }
    const candidate = current.length === 0 ? word : `${current} ${word}`;
    if (candidate.length <= maxChars || current.length === 0) {
      current = candidate;
    } else {
      lines.push(current);
      current = word;
    }
  }
  flush();
  return lines;
}

// Largest first: the common "See Shu's picks for Nov 3, 2026 Elections" fits
// two 72px lines; sizes below exist for long first names (≤80 chars, enforced
// at registration). 40px wraps the worst legal case within four lines.
const HEADLINE_FONT_SIZES = [72, 60, 50, 40];
const MAX_HEADLINE_LINES = 4;
const LINE_HEIGHT_FACTOR = 1.2;

function layoutHeadline(text: string): { fontSize: number; lines: string[] } {
  let fallback: { fontSize: number; lines: string[] } | null = null;
  for (const fontSize of HEADLINE_FONT_SIZES) {
    const lines = wrapHeadline(text, fontSize);
    const maxChars = Math.floor(USABLE_WIDTH / (fontSize * AVG_GLYPH_WIDTH_EM));
    const fits = lines.length <= MAX_HEADLINE_LINES && lines.every((line) => line.length <= maxChars);
    if (fits) {
      return { fontSize, lines };
    }
    fallback = { fontSize, lines: lines.slice(0, MAX_HEADLINE_LINES) };
  }
  // Unreachable for legal names; truncating at the smallest size beats a 500.
  return fallback ?? { fontSize: HEADLINE_FONT_SIZES.at(-1) ?? 40, lines: [text] };
}

export type PickCardOgImageInput = {
  firstName: string | null;
  electionDate: string;
};

/** Exported separately from the PNG step so tests can assert on markup
 * (escaping, wording) without decoding pixels. */
export function buildPickCardOgSvg(input: PickCardOgImageInput): string {
  // Non-breaking spaces inside the date keep "Nov 3, 2026" on one line; the
  // wrapper only splits on regular spaces.
  const date = formatElectionDateShort(input.electionDate);
  const headline = pickCardOgHeadline(input.firstName, input.electionDate).replace(
    date,
    date.replace(/ /g, " ")
  );
  const { fontSize, lines } = layoutHeadline(headline);
  const lineHeight = Math.round(fontSize * LINE_HEIGHT_FACTOR);

  // Center the headline block in the space under the brand label (y 170–600),
  // so one-line and four-line headlines both sit visually balanced.
  const blockTop = 170;
  const blockHeight = 600 - blockTop;
  const textHeight = lines.length * lineHeight;
  // First baseline: top offset plus ~0.8em ascent of the first line.
  const firstBaseline = Math.round(blockTop + (blockHeight - textHeight) / 2 + fontSize * 0.8);

  const headlineTspans = lines
    .map(
      (line, index) =>
        `<text x="${MARGIN_X}" y="${firstBaseline + index * lineHeight}" font-family="Inter" font-size="${fontSize}" font-weight="700" fill="${INK_COLOR}">${escapeXml(line)}</text>`
    )
    .join("\n  ");

  return `<svg width="${WIDTH}" height="${HEIGHT}" viewBox="0 0 ${WIDTH} ${HEIGHT}" xmlns="http://www.w3.org/2000/svg">
  <rect width="${WIDTH}" height="${HEIGHT}" fill="#ffffff"/>
  <rect width="${WIDTH}" height="28" fill="${BRAND_COLOR}"/>
  <text x="${MARGIN_X}" y="132" font-family="Inter" font-size="42" font-weight="700" fill="${BRAND_COLOR}">Elections Simplified</text>
  ${headlineTspans}
</svg>`;
}

// Rendering costs ~100ms of CPU, and the endpoint is public: without a
// cache, every request renders (Cloudflare treats each distinct query
// string as a separate cache key, so origin traffic is easy to force).
// The image depends only on first name + election date, so a small
// keyed cache turns any amount of repeat traffic into lookups. Promises
// are cached, not buffers, so concurrent misses for the same card share
// one render. ~30KB per entry bounds the cache at a few MB.
const RENDER_CACHE_MAX_ENTRIES = 200;
const renderCache = new Map<string, Promise<Buffer>>();

async function renderPng(input: PickCardOgImageInput): Promise<Buffer> {
  const rendered = await renderAsync(buildPickCardOgSvg(input), {
    fitTo: { mode: "width", value: WIDTH },
    font: {
      fontFiles: [FONT_PATH],
      defaultFontFamily: "Inter",
      loadSystemFonts: true,
    },
  });
  return Buffer.from(rendered.asPng());
}

export function renderPickCardOgImage(input: PickCardOgImageInput): Promise<Buffer> {
  const key = `${input.firstName ?? ""} ${input.electionDate}`;
  const cached = renderCache.get(key);
  if (cached) {
    return cached;
  }
  const pending = renderPng(input);
  renderCache.set(key, pending);
  // A failed render must not be pinned as the permanent answer for this card.
  pending.catch(() => renderCache.delete(key));
  if (renderCache.size > RENDER_CACHE_MAX_ENTRIES) {
    // Maps iterate in insertion order, so the first key is the oldest entry.
    const oldest = renderCache.keys().next().value;
    if (oldest !== undefined) {
      renderCache.delete(oldest);
    }
  }
  return pending;
}

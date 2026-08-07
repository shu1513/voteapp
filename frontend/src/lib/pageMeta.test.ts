import { describe, expect, it } from "vitest";
import { meta as rootMeta } from "../root";
import { meta as termsMeta } from "../routes/terms";
import { meta as privacyMeta } from "../routes/privacy";
import { meta as disclaimerMeta } from "../routes/disclaimer";
import { DEFAULT_DESCRIPTION, pageMeta, SITE_ORIGIN } from "./pageMeta";

type Descriptor = Record<string, unknown>;

function byProperty(descriptors: Descriptor[], property: string): string | undefined {
  const found = descriptors.find((entry) => entry.property === property || entry.name === property);
  return found?.content as string | undefined;
}

// React Router replaces the parent's meta array with the leaf's rather than
// merging, so a route that exports its own meta drops every tag it does not
// re-declare. Nothing on screen changes when that happens — the loss only
// shows up as a bare URL in somebody else's chat app, which is exactly the
// kind of regression that ships unnoticed.
const ROUTE_METAS: Array<[string, Descriptor[]]> = [
  ["root", (rootMeta as unknown as () => Descriptor[])()],
  ["terms", (termsMeta as unknown as () => Descriptor[])()],
  ["privacy", (privacyMeta as unknown as () => Descriptor[])()],
  ["disclaimer", (disclaimerMeta as unknown as () => Descriptor[])()],
];

describe("every route that sets meta ships a share card", () => {
  it.each(ROUTE_METAS)("%s has a title and description", (_name, descriptors) => {
    expect(descriptors.find((entry) => typeof entry.title === "string")).toBeDefined();
    expect(byProperty(descriptors, "description")).toBeTruthy();
  });

  it.each(ROUTE_METAS)("%s has the Open Graph tags scrapers read", (_name, descriptors) => {
    expect(byProperty(descriptors, "og:title")).toBeTruthy();
    expect(byProperty(descriptors, "og:description")).toBeTruthy();
    expect(byProperty(descriptors, "og:type")).toBe("website");
    // Absolute: scrapers do not resolve relative URLs.
    expect(byProperty(descriptors, "og:image")).toBe(`${SITE_ORIGIN}/og-card.png`);
    // og:url is only declared by routes that know their own path. Root omits
    // it on purpose — it is the fallback for the ballot and candidate pages,
    // and claiming their canonical URL is "/" would be a lie.
    const ogUrl = byProperty(descriptors, "og:url");
    if (ogUrl !== undefined) {
      expect(ogUrl).toMatch(new RegExp(`^${SITE_ORIGIN}/`));
    }
  });

  it.each(ROUTE_METAS)("%s asks X for the large card rather than the thumbnail", (_name, descriptors) => {
    expect(byProperty(descriptors, "twitter:card")).toBe("summary_large_image");
    expect(byProperty(descriptors, "twitter:image")).toBe(`${SITE_ORIGIN}/og-card.png`);
  });
});

describe("pageMeta", () => {
  it("mirrors the page title and description into both namespaces", () => {
    const descriptors = pageMeta({ title: "A Page", description: "What it is.", path: "/a" }) as Descriptor[];

    expect(byProperty(descriptors, "og:title")).toBe("A Page");
    expect(byProperty(descriptors, "twitter:title")).toBe("A Page");
    expect(byProperty(descriptors, "og:description")).toBe("What it is.");
    expect(byProperty(descriptors, "twitter:description")).toBe("What it is.");
    expect(byProperty(descriptors, "og:url")).toBe(`${SITE_ORIGIN}/a`);
  });

  it("falls back to the site description when a page gives none", () => {
    const descriptors = pageMeta({ title: "A Page" }) as Descriptor[];
    expect(byProperty(descriptors, "description")).toBe(DEFAULT_DESCRIPTION);
  });

  it("omits og:url when the route does not know its path", () => {
    // Root uses this form, and root is what a candidate or ballot page falls
    // back to; a hardcoded "/" would misdeclare their canonical URL.
    const descriptors = pageMeta({ title: "A Page" }) as Descriptor[];
    expect(byProperty(descriptors, "og:url")).toBeUndefined();
  });

  it("declares the image dimensions so the card renders large", () => {
    const descriptors = pageMeta({ title: "A Page" }) as Descriptor[];
    expect(byProperty(descriptors, "og:image:width")).toBe("1200");
    expect(byProperty(descriptors, "og:image:height")).toBe("630");
  });

  it("lets a page swap in its own share image in both namespaces", () => {
    // The shared pick card points here at its generated per-share image.
    const descriptors = pageMeta({
      title: "A Page",
      image: { url: `${SITE_ORIGIN}/api/pick-cards/tok/og-image.png`, alt: "Shu's picks" },
    }) as Descriptor[];
    expect(byProperty(descriptors, "og:image")).toBe(`${SITE_ORIGIN}/api/pick-cards/tok/og-image.png`);
    expect(byProperty(descriptors, "twitter:image")).toBe(`${SITE_ORIGIN}/api/pick-cards/tok/og-image.png`);
    expect(byProperty(descriptors, "og:image:alt")).toBe("Shu's picks");
    expect(byProperty(descriptors, "twitter:image:alt")).toBe("Shu's picks");
  });
});

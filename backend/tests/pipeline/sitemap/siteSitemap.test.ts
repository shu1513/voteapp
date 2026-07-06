import { describe, expect, it, vi } from "vitest";

import {
  buildSiteSitemapXml,
  createCachedSiteSitemap,
  listSiteSitemapUrls,
  normalizeSiteOrigin,
} from "../../../src/pipeline/sitemap/siteSitemap.js";

function createDbMock() {
  const query = vi
    .fn()
    .mockResolvedValueOnce({
      rows: [
        {
          path: "/elections/11111111-1111-4111-8111-111111111111",
          lastmod: new Date("2026-07-01T12:34:56.000Z"),
        },
      ],
    })
    .mockResolvedValueOnce({
      rows: [
        {
          path: "/candidates/22222222-2222-4222-8222-222222222222",
          lastmod: "2026-07-02T00:00:00.000Z",
        },
      ],
    });
  return { query };
}

describe("site sitemap", () => {
  it("normalizes an absolute site origin", () => {
    expect(normalizeSiteOrigin("https://impactperdollar.com/")).toBe("https://impactperdollar.com");
    expect(normalizeSiteOrigin("http://localhost:5173")).toBe("http://localhost:5173");
  });

  it("rejects origins with paths or unsupported protocols", () => {
    expect(() => normalizeSiteOrigin("https://impactperdollar.com/app")).toThrow(/must not include/);
    expect(() => normalizeSiteOrigin("ftp://impactperdollar.com")).toThrow(/http or https/);
    expect(() => normalizeSiteOrigin("not a url")).toThrow(/absolute http\(s\) URL/);
  });

  it("builds XML with escaped absolute URLs and lastmod values", () => {
    const xml = buildSiteSitemapXml({
      siteOrigin: "https://example.test",
      urls: [
        { path: "/" },
        { path: "/elections/11111111-1111-4111-8111-111111111111", lastmod: "2026-07-01T12:34:56Z" },
        { path: "/search?q=one&two", lastmod: new Date("2026-07-02T00:00:00.000Z") },
      ],
    });

    expect(xml).toContain('<?xml version="1.0" encoding="UTF-8"?>');
    expect(xml).toContain("<loc>https://example.test/</loc>");
    expect(xml).toContain("<lastmod>2026-07-01T12:34:56.000Z</lastmod>");
    expect(xml).toContain("<loc>https://example.test/search?q=one&amp;two</loc>");
  });

  it("lists static URLs followed by election and active candidate URLs", async () => {
    const db = createDbMock();

    const urls = await listSiteSitemapUrls(db);

    expect(urls).toEqual([
      { path: "/" },
      { path: "/disclaimer" },
      { path: "/terms" },
      { path: "/privacy" },
      {
        path: "/elections/11111111-1111-4111-8111-111111111111",
        lastmod: new Date("2026-07-01T12:34:56.000Z"),
      },
      {
        path: "/candidates/22222222-2222-4222-8222-222222222222",
        lastmod: "2026-07-02T00:00:00.000Z",
      },
    ]);
    expect(db.query).toHaveBeenCalledTimes(2);
    expect(db.query.mock.calls[1]?.[0]).toContain("deleted_at IS NULL");
    expect(db.query.mock.calls[1]?.[0]).toContain("merged_into_candidate_id IS NULL");
  });

  it("caches generated XML for the configured TTL", async () => {
    const db = createDbMock();
    let now = 1_000;
    const getSitemapXml = createCachedSiteSitemap({
      db,
      siteOrigin: "https://example.test",
      ttlMs: 60_000,
      now: () => now,
    });

    const first = await getSitemapXml();
    const second = await getSitemapXml();
    now += 60_001;
    db.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });
    const third = await getSitemapXml();

    expect(first).toBe(second);
    expect(third).not.toBe(first);
    expect(db.query).toHaveBeenCalledTimes(4);
  });
});

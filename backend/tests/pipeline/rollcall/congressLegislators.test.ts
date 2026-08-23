import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CONGRESS_LEGISLATORS_PINNED_SHA,
  congressLegislatorsUrl,
  indexLegislators,
  loadCongressLegislators,
  parseLegislatorsYaml,
} from "../../../src/pipeline/rollcall/congressLegislators.js";

const CURRENT_YAML = `
- id:
    bioguide: A000370
    thomas: '02201'
    fec:
    - H4NC12100
    - h4nc12100
    govtrack: 412607
  name:
    first: Alma
    middle: S.
    last: Adams
    official_full: Alma S. Adams
  terms:
  - type: rep
    start: '2023-01-03'
    end: '2025-01-03'
    state: NC
    district: 12
    party: Democrat
  - type: rep
    start: '2025-01-03'
    end: '2027-01-03'
    state: nc
    district: 12
    party: Democrat
- id:
    bioguide: A000382
    lis: S428
    fec:
    - S4MD00327
  name:
    first: Angela
    last: Alsobrooks
  terms:
  - type: sen
    start: '2025-01-03'
    end: '2031-01-03'
    state: MD
    class: 1
    party: Democrat
- id:
    bioguide: N000000
  name:
    first: No
    last: Fec
  terms:
  - type: rep
    start: '2025-01-03'
    end: '2027-01-03'
    state: PR
    district: 0
`;

describe("parseLegislatorsYaml", () => {
  it("keeps bioguide, LIS, upper-cased FEC ids, a display name, and the term windows", () => {
    const people = parseLegislatorsYaml(CURRENT_YAML, "legislators-current.yaml");
    expect(people).toEqual([
      {
        bioguide: "A000370",
        lis: null,
        fecIds: ["H4NC12100"],
        name: "Alma S. Adams",
        terms: [
          { type: "rep", start: "2023-01-03", end: "2025-01-03", state: "NC", district: 12 },
          { type: "rep", start: "2025-01-03", end: "2027-01-03", state: "NC", district: 12 },
        ],
      },
      {
        bioguide: "A000382",
        lis: "S428",
        fecIds: ["S4MD00327"],
        name: "Angela Alsobrooks",
        terms: [{ type: "sen", start: "2025-01-03", end: "2031-01-03", state: "MD", district: null }],
      },
      {
        bioguide: "N000000",
        lis: null,
        fecIds: [],
        name: "No Fec",
        terms: [{ type: "rep", start: "2025-01-03", end: "2027-01-03", state: "PR", district: 0 }],
      },
    ]);
  });

  it("rejects a file that is not a list, or an entry with a bad term", () => {
    expect(() => parseLegislatorsYaml("id: x", "f.yaml")).toThrow(/f.yaml: top level is not a list/);
    expect(() =>
      parseLegislatorsYaml(
        "- id: {bioguide: X1}\n  name: {first: A, last: B}\n  terms:\n  - {type: gov, start: '2020-01-01', end: '2021-01-01', state: OH}\n",
        "f.yaml"
      )
    ).toThrow(/\(X1\) term 0: term type is not rep\/sen: gov/);
    expect(() =>
      parseLegislatorsYaml(
        "- id: {bioguide: X1}\n  name: {first: A, last: B}\n  terms:\n  - {type: rep, start: '2020-1-1', end: '2021-01-01', state: OH}\n",
        "f.yaml"
      )
    ).toThrow(/start is not an ISO date: 2020-1-1/);
  });
});

describe("indexLegislators", () => {
  it("indexes by bioguide and by LIS id, and refuses duplicates", () => {
    const people = parseLegislatorsYaml(CURRENT_YAML, "legislators-current.yaml");
    const index = indexLegislators(people);
    expect(index.count).toBe(3);
    expect(index.byBioguide.get("A000370")?.name).toBe("Alma S. Adams");
    expect(index.byLis.get("S428")?.bioguide).toBe("A000382");
    expect(index.byLis.size).toBe(1);
    expect(() => indexLegislators([...people, people[0]!])).toThrow(/bioguide A000370 twice/);
    expect(() => indexLegislators([...people, { ...people[1]!, bioguide: "Z1" }])).toThrow(/LIS id S428 twice/);
  });
});

describe("loadCongressLegislators", () => {
  const dirs: string[] = [];
  afterEach(() => {
    for (const dir of dirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function tempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "congress-legislators-"));
    dirs.push(dir);
    return dir;
  }

  it("pins the URL to a commit sha", () => {
    expect(congressLegislatorsUrl("a".repeat(40), "legislators-current.yaml")).toBe(
      `https://raw.githubusercontent.com/unitedstates/congress-legislators/${"a".repeat(40)}/legislators-current.yaml`
    );
    expect(CONGRESS_LEGISLATORS_PINNED_SHA).toMatch(/^[0-9a-f]{40}$/);
  });

  it("downloads both files once, caches them by sha, and merges them into one index", async () => {
    const cacheDir = tempDir();
    const sha = "b".repeat(40);
    const historical = "- id: {bioguide: H1, lis: S001, fec: [S0OH00001]}\n  name: {first: Old, last: Senator}\n  terms:\n  - {type: sen, start: '2019-01-03', end: '2025-01-03', state: OH}\n";
    const fetchFn = vi.fn(async (url: string) => {
      const body = url.endsWith("legislators-current.yaml") ? CURRENT_YAML : historical;
      return new Response(body, { status: 200 });
    });

    const first = await loadCongressLegislators({ sha, cacheDir, fetchFn });
    expect(fetchFn).toHaveBeenCalledTimes(2);
    expect(fetchFn.mock.calls.map(([url]) => url)).toEqual([
      congressLegislatorsUrl(sha, "legislators-current.yaml"),
      congressLegislatorsUrl(sha, "legislators-historical.yaml"),
    ]);
    expect(first.sha).toBe(sha);
    expect(first.index.count).toBe(4);
    expect(first.index.byLis.get("S001")?.name).toBe("Old Senator");
    expect(first.files.map((file) => [file.file, file.fromCache, file.count])).toEqual([
      ["legislators-current.yaml", false, 3],
      ["legislators-historical.yaml", false, 1],
    ]);
    expect(readFileSync(join(cacheDir, sha, "legislators-historical.yaml"), "utf8")).toBe(historical);
    // Written via rename: no temporary file is left behind.
    expect(readdirSync(join(cacheDir, sha)).sort()).toEqual(["legislators-current.yaml", "legislators-historical.yaml"]);

    const second = await loadCongressLegislators({ sha, cacheDir, fetchFn });
    expect(fetchFn).toHaveBeenCalledTimes(2);
    expect(second.files.every((file) => file.fromCache)).toBe(true);
    expect(second.files.map((file) => file.sha256)).toEqual(first.files.map((file) => file.sha256));
  });

  it("does not cache a body that fails to parse, and surfaces non-200 answers", async () => {
    const cacheDir = tempDir();
    const sha = "c".repeat(40);
    const bad = vi.fn(async () => new Response("id: not-a-list", { status: 200 }));
    await expect(loadCongressLegislators({ sha, cacheDir, fetchFn: bad })).rejects.toThrow(/top level is not a list/);
    expect(existsSync(join(cacheDir, sha, "legislators-current.yaml"))).toBe(false);

    const missing = vi.fn(async () => new Response("", { status: 404 }));
    await expect(loadCongressLegislators({ sha, cacheDir, fetchFn: missing })).rejects.toThrow(/HTTP 404/);
  });

  it("serves a complete cache without touching the network, and rejects a non-sha pin", async () => {
    const cacheDir = tempDir();
    const sha = "d".repeat(40);
    mkdirSync(join(cacheDir, sha));
    writeFileSync(join(cacheDir, sha, "legislators-current.yaml"), CURRENT_YAML);
    writeFileSync(join(cacheDir, sha, "legislators-historical.yaml"), "[]\n");
    const fetchFn = vi.fn(async () => {
      throw new Error("network must not be used");
    });
    const loaded = await loadCongressLegislators({ sha, cacheDir, fetchFn });
    expect(fetchFn).not.toHaveBeenCalled();
    expect(loaded.index.count).toBe(3);

    await expect(loadCongressLegislators({ sha: "main", cacheDir, fetchFn })).rejects.toThrow(
      /must be a 40-hex commit sha/
    );
  });
});

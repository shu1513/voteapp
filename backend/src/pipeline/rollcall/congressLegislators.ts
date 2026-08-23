import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parse as parseYaml } from "yaml";

import type { FetchFn } from "./federalRollCallXml.js";

// Federal member crosswalk (docs/plans/roll-call-vote-import.md §2):
// unitedstates/congress-legislators (CC0) maps bioguide id (House XML) and
// LIS id (Senate XML) to FEC ids, which is the only hook into
// candidates.fec_ids. Both files are needed: a member who left during the
// current Congress moves from -current to -historical, and several of them
// are Nov-2026 candidates for another office.
//
// The files are read at a pinned commit so a run is reproducible, and cached
// on disk by that sha (immutable content, so the cache never goes stale).

// main @ 2026-08-19, verified 2026-08-23.
export const CONGRESS_LEGISLATORS_PINNED_SHA = "750c0608efb6ef1fc3257ba72c99af3771d35088";
export const CONGRESS_LEGISLATORS_FILES = ["legislators-current.yaml", "legislators-historical.yaml"] as const;
export type CongressLegislatorsFile = (typeof CONGRESS_LEGISLATORS_FILES)[number];

export function congressLegislatorsUrl(sha: string, file: CongressLegislatorsFile): string {
  return `https://raw.githubusercontent.com/unitedstates/congress-legislators/${sha}/${file}`;
}

export type LegislatorTermType = "rep" | "sen";

export type LegislatorTerm = {
  type: LegislatorTermType;
  // ISO dates; the window is inclusive on both ends (consecutive terms of
  // one person share a boundary day, and that is the day the new Congress
  // first votes).
  start: string;
  end: string;
  state: string;
  // House seat; null for senators. Kept for the report only — the roll-call
  // XML carries no district, so nothing verifies against it.
  district: number | null;
};

export type Legislator = {
  bioguide: string;
  lis: string | null;
  // Upper-cased, de-duplicated, in file order.
  fecIds: string[];
  name: string;
  terms: LegislatorTerm[];
};

export type LegislatorIndex = {
  byBioguide: ReadonlyMap<string, Legislator>;
  byLis: ReadonlyMap<string, Legislator>;
  count: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(record: Record<string, unknown>, key: string, where: string): string {
  const value = record[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${where}: ${key} is missing or not a string`);
  }
  return value.trim();
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

function readIsoDate(record: Record<string, unknown>, key: string, where: string): string {
  const value = readString(record, key, where);
  if (!ISO_DATE.test(value)) {
    throw new Error(`${where}: ${key} is not an ISO date: ${value}`);
  }
  return value;
}

function parseTerm(raw: unknown, where: string): LegislatorTerm {
  if (!isRecord(raw)) {
    throw new Error(`${where}: term is not a mapping`);
  }
  const type = readString(raw, "type", where);
  if (type !== "rep" && type !== "sen") {
    throw new Error(`${where}: term type is not rep/sen: ${type}`);
  }
  const district = raw.district;
  if (district !== undefined && !Number.isInteger(district)) {
    throw new Error(`${where}: term district is not an integer`);
  }
  return {
    type,
    start: readIsoDate(raw, "start", where),
    end: readIsoDate(raw, "end", where),
    state: readString(raw, "state", where).toUpperCase(),
    district: typeof district === "number" ? district : null,
  };
}

function parseLegislator(raw: unknown, where: string): Legislator {
  if (!isRecord(raw) || !isRecord(raw.id) || !isRecord(raw.name)) {
    throw new Error(`${where}: entry lacks id/name mappings`);
  }
  const bioguide = readString(raw.id, "bioguide", where);
  const lisRaw = raw.id.lis;
  if (lisRaw !== undefined && typeof lisRaw !== "string") {
    throw new Error(`${where} (${bioguide}): lis is not a string`);
  }
  const fecRaw = raw.id.fec ?? [];
  if (!Array.isArray(fecRaw) || !fecRaw.every((id) => typeof id === "string")) {
    throw new Error(`${where} (${bioguide}): fec is not a list of strings`);
  }
  const termsRaw = raw.terms;
  if (!Array.isArray(termsRaw) || termsRaw.length === 0) {
    throw new Error(`${where} (${bioguide}): terms is missing or empty`);
  }
  const officialFull = raw.name.official_full;
  const name =
    typeof officialFull === "string" && officialFull.trim().length > 0
      ? officialFull.trim()
      : `${readString(raw.name, "first", where)} ${readString(raw.name, "last", where)}`;
  return {
    bioguide,
    lis: lisRaw === undefined ? null : lisRaw.trim(),
    fecIds: [...new Set(fecRaw.map((id) => id.trim().toUpperCase()).filter((id) => id.length > 0))],
    name,
    terms: termsRaw.map((term, index) => parseTerm(term, `${where} (${bioguide}) term ${index}`)),
  };
}

/** The people of one legislators-*.yaml file, in file order. */
export function parseLegislatorsYaml(text: string, file: string): Legislator[] {
  const parsed: unknown = parseYaml(text);
  if (!Array.isArray(parsed)) {
    throw new Error(`${file}: top level is not a list`);
  }
  return parsed.map((entry, index) => parseLegislator(entry, `${file} entry ${index}`));
}

/**
 * One lookup over both files. The project keeps a person in exactly one
 * file, so a repeated bioguide or LIS id is a data problem worth failing on
 * rather than silently picking a side.
 */
export function indexLegislators(legislators: readonly Legislator[]): LegislatorIndex {
  const byBioguide = new Map<string, Legislator>();
  const byLis = new Map<string, Legislator>();
  for (const legislator of legislators) {
    if (byBioguide.has(legislator.bioguide)) {
      throw new Error(`congress-legislators lists bioguide ${legislator.bioguide} twice`);
    }
    byBioguide.set(legislator.bioguide, legislator);
    if (legislator.lis !== null) {
      if (byLis.has(legislator.lis)) {
        throw new Error(`congress-legislators lists LIS id ${legislator.lis} twice`);
      }
      byLis.set(legislator.lis, legislator);
    }
  }
  return { byBioguide, byLis, count: byBioguide.size };
}

export type LoadCongressLegislatorsOptions = {
  sha?: string;
  // Files are stored at <cacheDir>/<sha>/<file>.
  cacheDir: string;
  fetchFn?: FetchFn;
  timeoutMs?: number;
};

export type CongressLegislatorsFileInfo = {
  file: CongressLegislatorsFile;
  path: string;
  sha256: string;
  fromCache: boolean;
  count: number;
};

export type LoadedCongressLegislators = {
  sha: string;
  index: LegislatorIndex;
  files: CongressLegislatorsFileInfo[];
};

const DEFAULT_TIMEOUT_MS = 120_000;
const USER_AGENT = "voteapp-rollcall-import (+https://electionssimplified.com)";

async function fetchText(url: string, fetchFn: FetchFn, timeoutMs: number): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchFn(url, {
      method: "GET",
      headers: { accept: "text/plain,text/yaml;q=0.9,*/*;q=0.8", "user-agent": USER_AGENT },
      signal: controller.signal,
    });
    if (response.status !== 200) {
      throw new Error(`HTTP ${response.status} for ${url}`);
    }
    return await response.text();
  } finally {
    clearTimeout(timeout);
  }
}

export async function loadCongressLegislators(options: LoadCongressLegislatorsOptions): Promise<LoadedCongressLegislators> {
  const sha = options.sha ?? CONGRESS_LEGISLATORS_PINNED_SHA;
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error(`congress-legislators sha must be a 40-hex commit sha, got: ${sha}`);
  }
  const fetchFn = options.fetchFn ?? globalThis.fetch?.bind(globalThis);
  if (!fetchFn) {
    throw new Error("global fetch is unavailable for congress-legislators download");
  }
  const dir = join(options.cacheDir, sha);
  mkdirSync(dir, { recursive: true });

  const legislators: Legislator[] = [];
  const files: CongressLegislatorsFileInfo[] = [];
  for (const file of CONGRESS_LEGISLATORS_FILES) {
    const path = join(dir, file);
    const fromCache = existsSync(path);
    const text = fromCache
      ? readFileSync(path, "utf8")
      : await fetchText(congressLegislatorsUrl(sha, file), fetchFn, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    const parsed = parseLegislatorsYaml(text, file);
    if (!fromCache) {
      // Cached only once it parsed, so a truncated body is never kept.
      writeFileSync(path, text);
    }
    legislators.push(...parsed);
    files.push({
      file,
      path,
      sha256: createHash("sha256").update(text).digest("hex"),
      fromCache,
      count: parsed.length,
    });
  }
  return { sha, index: indexLegislators(legislators), files };
}

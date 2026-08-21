import { findBlockedSourceReason } from "../pipeline/candidates/candidateRecordSourcePolicy.js";
import {
  CURRENT_RACE_RATING_FAVORED_SIDES,
  CURRENT_RACE_RATING_INTENSITIES,
  CURRENT_RACE_RATING_OUTLETS,
  deriveConsensusLabel,
  type CurrentRaceRatingFavoredSide,
  type CurrentRaceRatingIntensity,
  type CurrentRaceRatingObservation,
  type CurrentRaceRatingOutlet,
} from "../pipeline/competitiveness/currentRaceRatingConsensus.js";
import type { CurrentRaceRatingRecord } from "../pipeline/competitiveness/currentRaceRatingWriter.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type CurrentRaceRatingContext = {
  electionId: string;
  isDcDelegate: boolean;
};

export type CurrentRaceRatingPayload = {
  ratings: CurrentRaceRatingRecord[];
};

type ParseOptions = {
  contexts: readonly CurrentRaceRatingContext[];
};

type ParseResult =
  | { ok: true; payload: CurrentRaceRatingPayload }
  | { ok: false; reason: string };

const OUTLET_SET = new Set<string>(CURRENT_RACE_RATING_OUTLETS);
const FAVORED_SIDE_SET = new Set<string>(CURRENT_RACE_RATING_FAVORED_SIDES);
const INTENSITY_SET = new Set<number>(CURRENT_RACE_RATING_INTENSITIES);

// Each outlet's observations must come from its own site; wikipedia is the
// cross-check slot on the row-level source_url, never an observation url.
const OUTLET_DOMAINS: Record<CurrentRaceRatingOutlet, string> = {
  inside_elections: "insideelections.com",
  sabato: "centerforpolitics.org",
};

// Cook's terms forbid storing their ratings in any form; they are never an
// acceptable source anywhere in a payload. See the plan doc's legal section.
const BANNED_DOMAINS = ["cookpolitical.com"] as const;

const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

// These fields are derived by deriveConsensusLabel — a payload that carries
// them was built by agent arithmetic, which is exactly what we reject.
const DERIVED_FIELDS = ["competitiveness_label", "confidence", "as_of"] as const;

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isValidIsoDate(value: string): boolean {
  if (!ISO_DATE_PATTERN.test(value)) {
    return false;
  }
  // Date.parse rolls impossible days over (2026-02-30 -> March 2), so a
  // round-trip back to the input string is the actual calendar check.
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function hostnameMatches(hostname: string, domain: string): boolean {
  return hostname === domain || hostname.endsWith(`.${domain}`);
}

function validateEvidenceUrl(rawUrl: unknown, outlet?: CurrentRaceRatingOutlet): string | { reason: string } {
  if (!isNonEmptyString(rawUrl)) {
    return { reason: "url must be non-empty string" };
  }
  const url = normalizeHttpUrl(rawUrl);
  if (!url) {
    return { reason: `url must be valid http(s) URL: ${rawUrl}` };
  }
  const hostname = new URL(url).hostname.toLowerCase();
  if (BANNED_DOMAINS.some((domain) => hostnameMatches(hostname, domain))) {
    return { reason: `url domain '${hostname}' is banned as a rating source` };
  }
  const blockedReason = findBlockedSourceReason([url]);
  if (blockedReason) {
    return { reason: blockedReason };
  }
  if (outlet && !hostnameMatches(hostname, OUTLET_DOMAINS[outlet])) {
    return {
      reason: `observation url for outlet ${outlet} must be on ${OUTLET_DOMAINS[outlet]}, got ${hostname}`,
    };
  }
  return url;
}

function parseObservation(
  value: unknown
): { ok: true; observation: CurrentRaceRatingObservation } | { ok: false; reason: string } {
  if (!isPlainObject(value)) {
    return { ok: false, reason: "observations entries must be objects" };
  }

  const outlet = value.outlet;
  if (!isNonEmptyString(outlet) || !OUTLET_SET.has(outlet.trim())) {
    return { ok: false, reason: `observation outlet is invalid: ${String(outlet)}` };
  }
  const normalizedOutlet = outlet.trim() as CurrentRaceRatingOutlet;

  if (!isNonEmptyString(value.raw_rating)) {
    return { ok: false, reason: "observation raw_rating must be non-empty string" };
  }

  const favored = value.favored;
  if (!isNonEmptyString(favored) || !FAVORED_SIDE_SET.has(favored.trim())) {
    return { ok: false, reason: `observation favored is invalid: ${String(favored)}` };
  }
  const normalizedFavored = favored.trim() as CurrentRaceRatingFavoredSide;

  const intensity = value.intensity;
  if (typeof intensity !== "number" || !INTENSITY_SET.has(intensity)) {
    return { ok: false, reason: `observation intensity must be one of 0, 2, 3, 4, 5: ${String(intensity)}` };
  }
  const normalizedIntensity = intensity as CurrentRaceRatingIntensity;

  // A toss-up has no favored side and every non-toss-up rating names one.
  if (normalizedIntensity === 0 && normalizedFavored !== "none") {
    return { ok: false, reason: "observation with intensity 0 (toss-up) must use favored=none" };
  }
  if (normalizedIntensity !== 0 && normalizedFavored === "none") {
    return { ok: false, reason: "observation with intensity > 0 must name a favored side" };
  }

  if (!isNonEmptyString(value.as_of) || !isValidIsoDate(value.as_of.trim())) {
    return { ok: false, reason: `observation as_of must be a valid YYYY-MM-DD date: ${String(value.as_of)}` };
  }

  const url = validateEvidenceUrl(value.url, normalizedOutlet);
  if (typeof url !== "string") {
    return { ok: false, reason: `observation ${url.reason}` };
  }

  return {
    ok: true,
    observation: {
      outlet: normalizedOutlet,
      raw_rating: value.raw_rating.trim(),
      favored: normalizedFavored,
      intensity: normalizedIntensity,
      as_of: value.as_of.trim(),
      url,
    },
  };
}

function parseRow(
  value: unknown,
  context: CurrentRaceRatingContext
): { ok: true; row: CurrentRaceRatingRecord } | { ok: false; reason: string } {
  if (!isPlainObject(value)) {
    return { ok: false, reason: "ratings entries must be objects" };
  }

  for (const field of DERIVED_FIELDS) {
    if (field in value) {
      return {
        ok: false,
        reason: `rating ${field} is derived from observations and must not appear in the payload`,
      };
    }
  }

  const method = value.method;
  if (method === "mayoral_rubric") {
    return { ok: false, reason: "mayoral_rubric payloads are not supported until v1.1" };
  }
  if (method !== "outlet_consensus") {
    return { ok: false, reason: `rating method is invalid: ${String(method)}` };
  }

  const evidenceStatus = value.evidence_status;
  if (evidenceStatus !== "rated" && evidenceStatus !== "none_found") {
    return { ok: false, reason: `rating evidence_status is invalid: ${String(evidenceStatus)}` };
  }

  const sourceUrl = validateEvidenceUrl(value.source_url);
  if (typeof sourceUrl !== "string") {
    return { ok: false, reason: `rating source_url ${sourceUrl.reason}` };
  }

  const notes = value.notes;
  if (notes !== undefined && !isNonEmptyString(notes)) {
    return { ok: false, reason: "rating notes must be a non-empty string when present" };
  }

  if (evidenceStatus === "none_found") {
    if (Array.isArray(value.observations) && value.observations.length > 0) {
      return { ok: false, reason: "none_found ratings must not include observations" };
    }
    return {
      ok: true,
      row: {
        election_id: context.electionId,
        method: "outlet_consensus",
        evidence_status: "none_found",
        competitiveness_label: null,
        confidence: null,
        as_of: null,
        decisive_round: null,
        evidence: {
          observations: [],
          ...(notes !== undefined ? { notes: notes.trim() } : {}),
        },
        source_url: sourceUrl,
      },
    };
  }

  if (context.isDcDelegate) {
    return {
      ok: false,
      reason: `election ${context.electionId} is the DC delegate race, which outlets do not rate; record none_found or exclude it`,
    };
  }

  if (!Array.isArray(value.observations) || value.observations.length === 0) {
    return { ok: false, reason: "rated ratings must include at least one observation" };
  }

  const observations: CurrentRaceRatingObservation[] = [];
  const seenOutlets = new Set<CurrentRaceRatingOutlet>();
  for (const rawObservation of value.observations) {
    const parsed = parseObservation(rawObservation);
    if (!parsed.ok) {
      return parsed;
    }
    if (seenOutlets.has(parsed.observation.outlet)) {
      return { ok: false, reason: `observations contain duplicate outlet: ${parsed.observation.outlet}` };
    }
    seenOutlets.add(parsed.observation.outlet);
    observations.push(parsed.observation);
  }

  const consensus = deriveConsensusLabel(observations);
  // as_of = the newest feed snapshot used; per-observation as_of stays in
  // evidence so staleness of one outlet never hides the other's currency.
  const asOf = observations.map((observation) => observation.as_of).sort().at(-1)!;

  return {
    ok: true,
    row: {
      election_id: context.electionId,
      method: "outlet_consensus",
      evidence_status: "rated",
      competitiveness_label: consensus.competitiveness_label,
      confidence: consensus.confidence,
      as_of: asOf,
      decisive_round: null,
      evidence: {
        observations,
        mean_intensity: consensus.mean_intensity,
        ...(notes !== undefined ? { notes: notes.trim() } : {}),
      },
      source_url: sourceUrl,
    },
  };
}

export function parseCurrentRaceRatingPayload(payload: unknown, options: ParseOptions): ParseResult {
  if (!isPlainObject(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }
  if (options.contexts.length === 0) {
    return { ok: false, reason: "contexts must contain at least one election" };
  }
  if (options.contexts.length > 10) {
    return { ok: false, reason: "contexts must contain at most 10 elections" };
  }
  if (!Array.isArray(payload.ratings)) {
    return { ok: false, reason: "payload.ratings must be array" };
  }

  const contextsById = new Map(options.contexts.map((context) => [context.electionId, context]));
  const seenElectionIds = new Set<string>();
  const rows: CurrentRaceRatingRecord[] = [];

  for (const rawRow of payload.ratings) {
    if (!isPlainObject(rawRow)) {
      return { ok: false, reason: "payload.ratings contains invalid row" };
    }
    const electionId = rawRow.election_id;
    if (!isNonEmptyString(electionId)) {
      return { ok: false, reason: "rating election_id must be non-empty string" };
    }
    const context = contextsById.get(electionId.trim());
    if (!context) {
      return { ok: false, reason: `rating contains election_id outside provided context: ${electionId}` };
    }
    if (seenElectionIds.has(context.electionId)) {
      return { ok: false, reason: `payload.ratings contains duplicate election_id: ${context.electionId}` };
    }
    const parsed = parseRow(rawRow, context);
    if (!parsed.ok) {
      return parsed;
    }
    seenElectionIds.add(context.electionId);
    rows.push(parsed.row);
  }

  for (const context of options.contexts) {
    if (!seenElectionIds.has(context.electionId)) {
      return { ok: false, reason: `payload.ratings is missing election_id: ${context.electionId}` };
    }
  }

  return { ok: true, payload: { ratings: rows } };
}

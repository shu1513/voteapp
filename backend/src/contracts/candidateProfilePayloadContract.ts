import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import { normalizeTwitterHandle } from "../utils/candidateIdentity.js";

export type CandidateProfilePayload = {
  display_name: string;
  first_name: string;
  last_name: string;
  party?: string;
  date_of_birth?: string;
  twitter_handle?: string;
  linkedin_url?: string;
  official_website_url?: string;
  fec_ids?: string[];
  state_filing_ids?: string[];
  current_office?: string;
  summary?: string;
  sources: string[];
};

export type CandidateProfilePayloadParseOptions = {
  requireFecIds?: boolean;
  allowFecIds?: boolean;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }
  const parsed = new Date(Date.UTC(year, month - 1, day));
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() === month - 1 &&
    parsed.getUTCDate() === day
  );
}

function normalizeSources(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      return null;
    }
    const url = normalizeHttpUrl(item);
    if (!url) {
      return null;
    }
    if (!seen.has(url)) {
      seen.add(url);
      normalized.push(url);
    }
  }
  return normalized.length > 0 ? normalized : null;
}

function normalizeOptionalStringArray(value: unknown): string[] | null | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    return null;
  }

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (!isNonEmptyString(item)) {
      return null;
    }
    const text = item.trim();
    if (!seen.has(text)) {
      seen.add(text);
      normalized.push(text);
    }
  }
  return normalized;
}

export function parseCandidateProfilePayload(
  payload: unknown,
  options: CandidateProfilePayloadParseOptions = {}
):
  | { ok: true; payload: CandidateProfilePayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!isNonEmptyString(input.display_name)) {
    return { ok: false, reason: "payload.display_name must be non-empty string" };
  }
  if (!isNonEmptyString(input.first_name)) {
    return { ok: false, reason: "payload.first_name must be non-empty string" };
  }
  if (!isNonEmptyString(input.last_name)) {
    return { ok: false, reason: "payload.last_name must be non-empty string" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "payload.sources must contain valid URL strings" };
  }

  let party: string | undefined;
  if (input.party !== undefined && input.party !== null) {
    if (!isNonEmptyString(input.party)) {
      return { ok: false, reason: "payload.party must be non-empty string when present" };
    }
    party = input.party.trim();
  }

  let dateOfBirth: string | undefined;
  if (input.date_of_birth !== undefined && input.date_of_birth !== null) {
    if (!isNonEmptyString(input.date_of_birth) || !isIsoDate(input.date_of_birth.trim())) {
      return { ok: false, reason: "payload.date_of_birth must be YYYY-MM-DD when present" };
    }
    dateOfBirth = input.date_of_birth.trim();
  }

  let twitterHandle: string | undefined;
  if (input.twitter_handle !== undefined && input.twitter_handle !== null) {
    if (!isNonEmptyString(input.twitter_handle)) {
      return { ok: false, reason: "payload.twitter_handle must be non-empty string when present" };
    }
    const normalized = normalizeTwitterHandle(input.twitter_handle);
    if (!normalized) {
      return { ok: false, reason: "payload.twitter_handle must be a valid handle when present" };
    }
    twitterHandle = normalized;
  }

  let linkedinUrl: string | undefined;
  if (input.linkedin_url !== undefined && input.linkedin_url !== null) {
    if (!isNonEmptyString(input.linkedin_url)) {
      return { ok: false, reason: "payload.linkedin_url must be non-empty string when present" };
    }
    const normalized = normalizeHttpUrl(input.linkedin_url);
    if (!normalized) {
      return { ok: false, reason: "payload.linkedin_url must be valid http(s) URL when present" };
    }
    linkedinUrl = normalized;
  }

  let officialWebsiteUrl: string | undefined;
  if (input.official_website_url !== undefined && input.official_website_url !== null) {
    if (!isNonEmptyString(input.official_website_url)) {
      return { ok: false, reason: "payload.official_website_url must be non-empty string when present" };
    }
    const normalized = normalizeHttpUrl(input.official_website_url);
    if (!normalized) {
      return { ok: false, reason: "payload.official_website_url must be valid http(s) URL when present" };
    }
    officialWebsiteUrl = normalized;
  }

  const allowFecIds = options.allowFecIds !== false;
  const requireFecIds = options.requireFecIds === true;
  if (!allowFecIds && input.fec_ids !== undefined && input.fec_ids !== null) {
    return {
      ok: false,
      reason:
        "payload.fec_ids is not allowed for this contest mode; omit fec_ids from the profile payload — identity IDs are inherited from the staged roster row",
    };
  }
  const fecIds = allowFecIds ? normalizeOptionalStringArray(input.fec_ids) : undefined;
  if (allowFecIds && fecIds === null) {
    return { ok: false, reason: "payload.fec_ids must be string array when present" };
  }
  const normalizedFecIds = fecIds ?? undefined;
  if (requireFecIds && (!normalizedFecIds || normalizedFecIds.length === 0)) {
    return { ok: false, reason: "payload.fec_ids must contain at least one FEC ID for federal contests" };
  }

  const stateFilingIds = normalizeOptionalStringArray(input.state_filing_ids);
  if (stateFilingIds === null) {
    return { ok: false, reason: "payload.state_filing_ids must be string array when present" };
  }

  let currentOffice: string | undefined;
  if (input.current_office !== undefined && input.current_office !== null) {
    if (!isNonEmptyString(input.current_office)) {
      return { ok: false, reason: "payload.current_office must be non-empty string when present" };
    }
    currentOffice = input.current_office.trim();
  }

  let summary: string | undefined;
  if (input.summary !== undefined && input.summary !== null) {
    if (!isNonEmptyString(input.summary)) {
      return { ok: false, reason: "payload.summary must be non-empty string when present" };
    }
    summary = input.summary.trim();
  }

  return {
    ok: true,
    payload: {
      display_name: input.display_name.trim(),
      first_name: input.first_name.trim(),
      last_name: input.last_name.trim(),
      ...(party ? { party } : {}),
      ...(dateOfBirth ? { date_of_birth: dateOfBirth } : {}),
      ...(twitterHandle ? { twitter_handle: twitterHandle } : {}),
      ...(linkedinUrl ? { linkedin_url: linkedinUrl } : {}),
      ...(officialWebsiteUrl ? { official_website_url: officialWebsiteUrl } : {}),
      ...(normalizedFecIds !== undefined ? { fec_ids: normalizedFecIds } : {}),
      ...(stateFilingIds !== undefined ? { state_filing_ids: stateFilingIds } : {}),
      ...(currentOffice ? { current_office: currentOffice } : {}),
      ...(summary ? { summary } : {}),
      sources,
    },
  };
}

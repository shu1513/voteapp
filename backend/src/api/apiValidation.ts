import { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
import { UUID_PATTERN, isUuid } from "../utils/uuid.js";

export { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
export { UUID_PATTERN } from "../utils/uuid.js";

export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
export const BALLOT_LOOKUP_PATH = "/api/ballot";
export const ELECTION_DETAIL_PATH_PREFIX = "/api/elections/";
export const ME_DISTRICTS_INITIALIZE_PATH = "/api/me/districts/initialize";
export const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
export const MAX_BALLOT_DISTRICT_IDS = 50;

export type AddressResolvePayload = {
  address: string;
};

export type InitializeUserDistrictsPayload = {
  district_ids: string[];
};

export function parseAddressBodyValue(parsed: unknown): AddressResolvePayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const address = (parsed as { address?: unknown }).address;
  if (typeof address !== "string" || address.trim().length === 0) {
    throw new TypeError("Request body must include non-empty string field: address");
  }

  return {
    address: address.trim(),
  };
}

export function parseAddressPayload(rawBody: string): AddressResolvePayload {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody);
  } catch {
    throw new SyntaxError("Request body must be valid JSON");
  }

  return parseAddressBodyValue(parsed);
}

export function parseInitializeUserDistrictsBodyValue(parsed: unknown): InitializeUserDistrictsPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const districtIds = (parsed as { district_ids?: unknown }).district_ids;
  if (!Array.isArray(districtIds)) {
    throw new TypeError("Request body must include array field: district_ids");
  }

  const normalizedDistrictIds: string[] = [];
  const seen = new Set<string>();
  for (const rawDistrictId of districtIds) {
    if (typeof rawDistrictId !== "string") {
      throw new TypeError("district_ids must contain only UUID strings");
    }
    const districtId = rawDistrictId.trim();
    if (districtId.length === 0) {
      continue;
    }
    if (!isUuid(districtId)) {
      throw new TypeError(`district_ids contains invalid UUID: ${districtId}`);
    }
    const dedupeKey = districtId.toLowerCase();
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    normalizedDistrictIds.push(districtId);
  }

  if (normalizedDistrictIds.length === 0) {
    throw new TypeError("district_ids must include at least one district UUID");
  }
  if (normalizedDistrictIds.length > MAX_INITIALIZE_DISTRICT_IDS) {
    throw new TypeError(`district_ids supports at most ${MAX_INITIALIZE_DISTRICT_IDS} UUIDs`);
  }

  return {
    district_ids: normalizedDistrictIds,
  };
}

export function parseDistrictIds(url: URL): string[] {
  const rawValues = url.searchParams
    .getAll("district_ids")
    .flatMap((value) => value.split(","))
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  const districtIds = [...new Set(rawValues)];
  if (districtIds.length === 0) {
    throw new TypeError("Query parameter district_ids must include at least one district UUID");
  }
  if (districtIds.length > MAX_BALLOT_DISTRICT_IDS) {
    throw new TypeError(`Query parameter district_ids supports at most ${MAX_BALLOT_DISTRICT_IDS} UUIDs`);
  }
  const invalidId = districtIds.find((id) => !isUuid(id));
  if (invalidId) {
    throw new TypeError(`Query parameter district_ids contains invalid UUID: ${invalidId}`);
  }
  return districtIds;
}

export function isElectionDetailPath(pathname: string): boolean {
  return pathname.startsWith(ELECTION_DETAIL_PATH_PREFIX);
}

export function parseElectionId(url: URL): string {
  const electionId = url.pathname.slice(ELECTION_DETAIL_PATH_PREFIX.length).trim();
  if (electionId.length === 0 || electionId.includes("/")) {
    throw new TypeError("Election detail path must be /api/elections/:election_id");
  }
  if (!isUuid(electionId)) {
    throw new TypeError(`Election detail path contains invalid UUID: ${electionId}`);
  }
  return electionId;
}

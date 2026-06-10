export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
export const BALLOT_LOOKUP_PATH = "/api/ballot";
export const ELECTION_DETAIL_PATH_PREFIX = "/api/elections/";
export const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
export const MAX_BALLOT_DISTRICT_IDS = 50;
export const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export type AddressResolvePayload = {
  address: string;
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
  const invalidId = districtIds.find((id) => !UUID_PATTERN.test(id));
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
  if (!UUID_PATTERN.test(electionId)) {
    throw new TypeError(`Election detail path contains invalid UUID: ${electionId}`);
  }
  return electionId;
}

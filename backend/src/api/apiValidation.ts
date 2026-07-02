import { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
import { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../constants/userResearchAreaPreferences.js";
import type { UserCandidateFollowInput } from "../pipeline/users/userCandidateFollows.js";
import type { UserResearchAreaPreferenceInput } from "../pipeline/users/userResearchAreaPreferences.js";
import { UUID_PATTERN, isUuid } from "../utils/uuid.js";

export { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
export { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../constants/userResearchAreaPreferences.js";
export { UUID_PATTERN } from "../utils/uuid.js";

export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
export const AUTH_FORGOT_PASSWORD_PATH = "/api/auth/forgot-password";
export const AUTH_LOGIN_PATH = "/api/auth/login";
export const AUTH_LOGOUT_PATH = "/api/auth/logout";
export const AUTH_REGISTER_PATH = "/api/auth/register";
export const AUTH_RESET_PASSWORD_PATH = "/api/auth/reset-password";
export const AUTH_RESEND_VERIFICATION_PATH = "/api/auth/resend-verification";
export const AUTH_VERIFY_EMAIL_PATH = "/api/auth/verify-email";
export const BALLOT_LOOKUP_PATH = "/api/ballot";
export const CANDIDATE_DETAIL_PATH_PREFIX = "/api/candidates/";
export const ELECTION_DETAIL_PATH_PREFIX = "/api/elections/";
export const ME_ADDRESS_PATH = "/api/me/address";
export const ME_BALLOT_PATH = "/api/me/ballot";
export const ME_CANDIDATE_FOLLOWS_PATH = "/api/me/candidate-follows";
export const ME_DISTRICTS_INITIALIZE_PATH = "/api/me/districts/initialize";
export const ME_RESEARCH_AREA_PREFERENCES_PATH = "/api/me/research-area-preferences";
export const RESEARCH_AREAS_PATH = "/api/research-areas";
export const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
export const MAX_BALLOT_DISTRICT_IDS = 50;

export type AddressResolvePayload = {
  address: string;
};

export type AuthenticatedAddressPayload = AddressResolvePayload;

export type AuthRegisterPayload = {
  email: string;
  password: string;
  first_name?: string;
};

export type AuthLoginPayload = {
  email: string;
  password: string;
};

export type AuthForgotPasswordPayload = {
  email: string;
};

export type AuthResendVerificationPayload = AuthForgotPasswordPayload;

export type AuthVerifyEmailPayload = {
  token: string;
};

export type AuthResetPasswordPayload = {
  token: string;
  password: string;
};

export type InitializeUserDistrictsPayload = {
  district_ids: string[];
};

export type ResearchAreaPreferencePayloadItem = {
  research_area_id: string;
  rank?: number | null;
};

export type ResearchAreaPreferencesPayload = {
  preferences: UserResearchAreaPreferenceInput[];
};

export type CandidateFollowPayload = UserCandidateFollowInput;

function parseStringField(parsed: unknown, fieldName: string): string {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  const value = (parsed as Record<string, unknown>)[fieldName];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`Request body must include non-empty string field: ${fieldName}`);
  }
  return value.trim();
}

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

export function parseAuthenticatedAddressBodyValue(parsed: unknown): AuthenticatedAddressPayload {
  return parseAddressBodyValue(parsed);
}

export function parseAuthRegisterBodyValue(parsed: unknown): AuthRegisterPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const email = parseStringField(parsed, "email");
  const password = parseStringField(parsed, "password");
  const firstName = (parsed as { first_name?: unknown }).first_name;
  if (firstName !== undefined && (typeof firstName !== "string" || firstName.trim().length === 0)) {
    throw new TypeError("first_name must be a non-empty string when provided");
  }

  return {
    email,
    password,
    ...(firstName === undefined ? {} : { first_name: firstName.trim() }),
  };
}

export function parseAuthLoginBodyValue(parsed: unknown): AuthLoginPayload {
  return {
    email: parseStringField(parsed, "email"),
    password: parseStringField(parsed, "password"),
  };
}

export function parseAuthForgotPasswordBodyValue(parsed: unknown): AuthForgotPasswordPayload {
  return {
    email: parseStringField(parsed, "email"),
  };
}

export function parseAuthResendVerificationBodyValue(parsed: unknown): AuthResendVerificationPayload {
  return parseAuthForgotPasswordBodyValue(parsed);
}

export function parseAuthVerifyEmailBodyValue(parsed: unknown): AuthVerifyEmailPayload {
  return {
    token: parseStringField(parsed, "token"),
  };
}

export function parseAuthResetPasswordBodyValue(parsed: unknown): AuthResetPasswordPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  return {
    token: parseStringField(parsed, "token"),
    password: parseStringField(parsed, "password"),
  };
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

export function parseResearchAreaPreferencesBodyValue(parsed: unknown): ResearchAreaPreferencesPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const preferences = (parsed as { preferences?: unknown }).preferences;
  if (!Array.isArray(preferences)) {
    throw new TypeError("Request body must include array field: preferences");
  }
  if (preferences.length > MAX_USER_RESEARCH_AREA_PREFERENCES) {
    throw new TypeError(`preferences supports at most ${MAX_USER_RESEARCH_AREA_PREFERENCES} research areas`);
  }

  const normalizedPreferences: UserResearchAreaPreferenceInput[] = [];
  const seenResearchAreaIds = new Set<string>();
  const seenRanks = new Set<number>();

  for (const rawPreference of preferences) {
    if (typeof rawPreference !== "object" || rawPreference === null || Array.isArray(rawPreference)) {
      throw new TypeError("preferences must contain only JSON objects");
    }

    const preference = rawPreference as ResearchAreaPreferencePayloadItem;
    if (typeof preference.research_area_id !== "string") {
      throw new TypeError("preferences[].research_area_id must be a UUID string");
    }

    const researchAreaId = preference.research_area_id.trim();
    if (!isUuid(researchAreaId)) {
      throw new TypeError(`preferences contains invalid research_area_id: ${researchAreaId}`);
    }
    const researchAreaDedupeKey = researchAreaId.toLowerCase();
    if (seenResearchAreaIds.has(researchAreaDedupeKey)) {
      throw new TypeError(`preferences contains duplicate research_area_id: ${researchAreaId}`);
    }
    seenResearchAreaIds.add(researchAreaDedupeKey);

    const rank = preference.rank ?? null;
    if (rank !== null && (!Number.isInteger(rank) || rank < 1 || rank > MAX_USER_RESEARCH_AREA_PREFERENCES)) {
      throw new TypeError(`preferences[].rank must be an integer from 1 to ${MAX_USER_RESEARCH_AREA_PREFERENCES}`);
    }
    if (rank !== null) {
      if (seenRanks.has(rank)) {
        throw new TypeError(`preferences contains duplicate rank: ${rank}`);
      }
      seenRanks.add(rank);
    }

    normalizedPreferences.push({ researchAreaId, rank });
  }

  return { preferences: normalizedPreferences };
}

export function parseCandidateFollowBodyValue(parsed: unknown): CandidateFollowPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const payload = parsed as {
    candidate_id?: unknown;
    following?: unknown;
    notify_elections?: unknown;
    notify_updates?: unknown;
  };
  if (typeof payload.candidate_id !== "string") {
    throw new TypeError("Request body must include UUID string field: candidate_id");
  }
  const candidateId = payload.candidate_id.trim();
  if (!isUuid(candidateId)) {
    throw new TypeError(`candidate_id must be a valid UUID: ${candidateId}`);
  }
  if (typeof payload.following !== "boolean") {
    throw new TypeError("Request body must include boolean field: following");
  }
  if (payload.notify_elections !== undefined && typeof payload.notify_elections !== "boolean") {
    throw new TypeError("notify_elections must be a boolean");
  }
  if (payload.notify_updates !== undefined && typeof payload.notify_updates !== "boolean") {
    throw new TypeError("notify_updates must be a boolean");
  }

  return {
    candidateId,
    following: payload.following,
    ...(payload.notify_elections === undefined ? {} : { notifyElections: payload.notify_elections }),
    ...(payload.notify_updates === undefined ? {} : { notifyUpdates: payload.notify_updates }),
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

export function isCandidateDetailPath(pathname: string): boolean {
  return pathname.startsWith(CANDIDATE_DETAIL_PATH_PREFIX);
}

export function parseCandidateId(url: URL): string {
  const candidateId = url.pathname.slice(CANDIDATE_DETAIL_PATH_PREFIX.length).trim();
  if (candidateId.length === 0 || candidateId.includes("/")) {
    throw new TypeError("Candidate detail path must be /api/candidates/:candidate_id");
  }
  if (!isUuid(candidateId)) {
    throw new TypeError(`Candidate detail path contains invalid UUID: ${candidateId}`);
  }
  return candidateId;
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

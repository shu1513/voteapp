import { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
// [ballot-personalized-ordering] see ballotElectionOrdering.ts for removal notes
import {
  BALLOT_SUMMARY_SORTS,
  isBallotSummarySort,
  type BallotSummaryOptions,
  type BallotSummarySort,
} from "../pipeline/address/ballotElectionOrdering.js";
import { GOOGLE_PLACE_ID_PATTERN } from "../pipeline/address/googlePlacesAutocomplete.js";
import { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../constants/userResearchAreaPreferences.js";
import type { UserCandidateFollowInput } from "../pipeline/users/userCandidateFollows.js";
import type { UserElectionChoiceInput } from "../pipeline/users/userElectionChoices.js";
import type { UserResearchAreaPreferenceInput } from "../pipeline/users/userResearchAreaPreferences.js";
import type { UserBallotPreferences } from "../pipeline/users/userBallotPreferences.js";
import type { UserEmailPreferences } from "../pipeline/users/userEmailPreferences.js";
import type { RegisterUserPushTokenInput } from "../pipeline/users/userPushTokens.js";
import { CONTENT_REPORT_ENTITY_TYPES, type ContentReportEntityType } from "../pipeline/reports/contentReports.js";
import { MAX_FIRST_NAME_LENGTH } from "../pipeline/users/userIdentity.js";
import { UUID_PATTERN, isUuid } from "../utils/uuid.js";

export { MAX_INITIALIZE_DISTRICT_IDS } from "../constants/userDistricts.js";
export { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../constants/userResearchAreaPreferences.js";
export { UUID_PATTERN } from "../utils/uuid.js";

export const ADDRESS_AUTOCOMPLETE_PATH = "/api/address/autocomplete";
export const ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH = "/api/address/autocomplete/retrieve";
export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
export const AUTH_FORGOT_PASSWORD_PATH = "/api/auth/forgot-password";
// Sign in with Google (docs/plans/google-sign-in.md): exchanges a verified
// Google ID token for a session. Configured-if-present, like the Places
// proxy — 500 not-configured without GOOGLE_OAUTH_CLIENT_ID.
export const AUTH_GOOGLE_PATH = "/api/auth/google";
export const AUTH_LOGIN_PATH = "/api/auth/login";
export const AUTH_LOGOUT_PATH = "/api/auth/logout";
export const AUTH_REGISTER_PATH = "/api/auth/register";
export const AUTH_RESET_PASSWORD_PATH = "/api/auth/reset-password";
export const AUTH_RESEND_VERIFICATION_PATH = "/api/auth/resend-verification";
export const AUTH_VERIFY_EMAIL_PATH = "/api/auth/verify-email";
export const AUTH_VERIFY_EMAIL_CHANGE_PATH = "/api/auth/verify-email-change";
export const AUTH_LOGOUT_ALL_PATH = "/api/auth/logout-all";
export const BALLOT_LOOKUP_PATH = "/api/ballot";
// Chatbot "Ask" (docs/plans/chatbot-rag.md). The path is always known;
// whether it answers depends on CHATBOT_ENABLED wiring (404 when unwired,
// mirroring the sitemap's not-configured behavior).
export const CHATBOT_ASK_PATH = "/api/chatbot/ask";
export const CONTENT_REPORTS_PATH = "/api/content-reports";
export const CANDIDATE_DETAIL_PATH_PREFIX = "/api/candidates/";
// Shares the candidate-detail prefix, so the router must test this path
// before isCandidateDetailPath (whose UUID parse would reject "search").
export const CANDIDATE_SEARCH_PATH = "/api/candidates/search";
export const ELECTION_DETAIL_PATH_PREFIX = "/api/elections/";
// Session-holder identity (email, first_name, email_verified). Not gated on
// email verification: the frontend needs it to render the unverified state.
export const ME_PATH = "/api/me";
export const ME_ADDRESS_PATH = "/api/me/address";
// Authenticated re-acceptance of the current terms bundle after a version
// bump (registration handles first acceptance). Not gated on email
// verification: acceptance must be recordable before the inbox is confirmed.
export const ME_TERMS_ACCEPTANCE_PATH = "/api/me/terms-acceptance";
export const ME_PASSWORD_PATH = "/api/me/password";
export const ME_EMAIL_PATH = "/api/me/email";
export const ME_BALLOT_PATH = "/api/me/ballot";
export const ME_CANDIDATE_FOLLOWS_PATH = "/api/me/candidate-follows";
// Per-election planned vote ("my choice"). Auth-gated but not
// verification-gated: a choice is private planning, it triggers no
// notifications, so a registered session is enough.
export const ME_ELECTION_CHOICES_PATH = "/api/me/election-choices";
// POST mints (or returns) the share link for one date's pick card. Same
// auth posture as election choices: session required, no verification gate.
export const ME_PICK_CARD_SHARES_PATH = "/api/me/pick-card-shares";
// Public tokenized read of a shared pick card; no session auth — the token
// IS the authorization (see user_pick_card_shares migration).
export const PICK_CARD_PATH_PREFIX = "/api/pick-cards/";
export const ME_DISTRICTS_INITIALIZE_PATH = "/api/me/districts/initialize";
export const ME_RESEARCH_AREA_PREFERENCES_PATH = "/api/me/research-area-preferences";
// [ballot-personalized-ordering]
export const ME_BALLOT_PREFERENCES_PATH = "/api/me/ballot-preferences";
export const ME_EMAIL_PREFERENCES_PATH = "/api/me/email-preferences";
// Mobile device push-token registration (POST registers/refreshes, DELETE
// revokes). Bearer-authed like every other /api/me route.
export const ME_PUSH_TOKENS_PATH = "/api/me/push-tokens";
// Signed-token unsubscribe target linked from notification emails; GET for
// humans, POST for RFC 8058 one-click mailbox buttons. No session auth. The
// optional pref query param picks which opt-in the link disables.
export const EMAIL_UNSUBSCRIBE_PATH = "/api/email/unsubscribe";
export const EMAIL_UNSUBSCRIBE_PREFERENCES = ["digest", "new_election_alerts", "election_reminders", "issue_updates"] as const;
export type EmailUnsubscribePreference = (typeof EMAIL_UNSUBSCRIBE_PREFERENCES)[number];

/** Missing param defaults to digest; an unrecognized value is null (400). */
export function parseEmailUnsubscribePreference(raw: string | null): EmailUnsubscribePreference | null {
  if (raw === null || raw.trim() === "") {
    return "digest";
  }
  const normalized = raw.trim();
  return (EMAIL_UNSUBSCRIBE_PREFERENCES as readonly string[]).includes(normalized)
    ? (normalized as EmailUnsubscribePreference)
    : null;
}
export const RESEARCH_AREAS_PATH = "/api/research-areas";
export const STATE_RESOURCES_PATH = "/api/state-resources";
export const SITE_SITEMAP_PATH = "/sitemap.xml";
export const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
export const MAX_BALLOT_DISTRICT_IDS = 50;

export type AddressResolvePayload = {
  address: string;
};

/**
 * The anonymous search body. Unlike the saved-address payload it carries the
 * clickwrap assertion: the pre-search checkbox used to gate only the frontend
 * button, so the endpoint it guarded accepted callers who had accepted
 * nothing. apiServer checks the version against CURRENT_TERMS_VERSION and
 * refuses the search otherwise. Nothing about the acceptance is stored — the
 * visitor is anonymous, and no row naming their IP address is written.
 */
export type PublicAddressResolvePayload = AddressResolvePayload & {
  accepted_terms_version: string;
};

export type AddressAutocompleteSuggestPayload = {
  input: string;
  session_token: string;
};

export type AddressAutocompleteRetrievePayload = {
  place_id: string;
  session_token: string;
};

// Minimum keystrokes before the proxy forwards to Google: shorter inputs give
// useless suggestions and every forwarded request has a billing cost.
export const MIN_AUTOCOMPLETE_INPUT_LENGTH = 3;
export const MAX_AUTOCOMPLETE_INPUT_LENGTH = 200;
const MIN_AUTOCOMPLETE_SESSION_TOKEN_LENGTH = 8;
const MAX_AUTOCOMPLETE_SESSION_TOKEN_LENGTH = 128;
const MAX_AUTOCOMPLETE_PLACE_ID_LENGTH = 512;
const AUTOCOMPLETE_TOKEN_PATTERN = /^[A-Za-z0-9_-]+$/;

export type AuthenticatedAddressPayload = AddressResolvePayload;

// Practical per-field ceilings so oversized junk is refused at the door
// instead of leaning on the 16 KB body limit: 254 is the longest address
// SMTP can actually deliver to (RFC 5321's 256-octet path limit minus the
// angle brackets — not the 320 the per-component limits naively sum to),
// and the address cap is far above any real street address. The first-name
// cap is not defined here: userIdentity.ts owns it (registration derives
// names under the same cap), and re-exporting keeps it one definition.
export const MAX_AUTH_EMAIL_LENGTH = 254;
export const MAX_ADDRESS_INPUT_LENGTH = 500;
export { MAX_FIRST_NAME_LENGTH };

export type AuthRegisterPayload = {
  email: string;
  password: string;
  accepted_terms_version: string;
  first_name?: string;
};

export type AuthLoginPayload = {
  email: string;
  password: string;
};

export type AuthGooglePayload = {
  /** The GIS credential response: a Google-signed ID-token JWT. */
  credential: string;
  /** Which page the button sat on; only "signup" may create an account. */
  intent: "login" | "signup";
  /** Clickwrap record, required for intent "signup". */
  accepted_terms_version?: string;
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

export type AuthVerifyEmailChangePayload = {
  token: string;
};

export type MePasswordPayload = {
  current_password: string;
  new_password: string;
};

export type MeEmailPayload = {
  new_email: string;
  password: string;
};

export type MeDeletePayload = {
  password: string;
};

export type MeUpdatePayload = {
  first_name: string;
};

export type MeTermsAcceptancePayload = {
  accepted_terms_version: string;
};

export type InitializeUserDistrictsPayload = {
  district_ids: string[];
};

export type ContentReportPayload = {
  entityType: ContentReportEntityType;
  entityId: string;
  message: string;
  suggestedSourceUrl?: string | null;
  reporterEmail?: string | null;
};

const MAX_CONTENT_REPORT_MESSAGE_LENGTH = 2000;
const MAX_CONTENT_REPORT_SOURCE_URL_LENGTH = 2048;
const MAX_CONTENT_REPORT_EMAIL_LENGTH = 320;

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

function parseEmailField(parsed: unknown, fieldName: string): string {
  const email = parseStringField(parsed, fieldName);
  if (email.length > MAX_AUTH_EMAIL_LENGTH) {
    throw new TypeError(`${fieldName} must be at most ${MAX_AUTH_EMAIL_LENGTH} characters`);
  }
  return email;
}

function parseAutocompleteSessionToken(parsed: unknown): string {
  const sessionToken = parseStringField(parsed, "session_token");
  if (
    sessionToken.length < MIN_AUTOCOMPLETE_SESSION_TOKEN_LENGTH ||
    sessionToken.length > MAX_AUTOCOMPLETE_SESSION_TOKEN_LENGTH ||
    !AUTOCOMPLETE_TOKEN_PATTERN.test(sessionToken)
  ) {
    throw new TypeError(
      `session_token must be ${MIN_AUTOCOMPLETE_SESSION_TOKEN_LENGTH}-${MAX_AUTOCOMPLETE_SESSION_TOKEN_LENGTH} characters of letters, digits, hyphens, or underscores`
    );
  }
  return sessionToken;
}

export function parseAutocompleteSuggestBodyValue(parsed: unknown): AddressAutocompleteSuggestPayload {
  const input = parseStringField(parsed, "input");
  if (input.length < MIN_AUTOCOMPLETE_INPUT_LENGTH) {
    throw new TypeError(`input must be at least ${MIN_AUTOCOMPLETE_INPUT_LENGTH} characters`);
  }
  if (input.length > MAX_AUTOCOMPLETE_INPUT_LENGTH) {
    throw new TypeError(`input supports at most ${MAX_AUTOCOMPLETE_INPUT_LENGTH} characters`);
  }
  return {
    input,
    session_token: parseAutocompleteSessionToken(parsed),
  };
}

export function parseAutocompleteRetrieveBodyValue(parsed: unknown): AddressAutocompleteRetrievePayload {
  const placeId = parseStringField(parsed, "place_id");
  if (placeId.length > MAX_AUTOCOMPLETE_PLACE_ID_LENGTH) {
    throw new TypeError(`place_id must be at most ${MAX_AUTOCOMPLETE_PLACE_ID_LENGTH} characters`);
  }
  // Same pattern the suggest step filters on, so any place_id we emitted is
  // accepted here.
  if (!GOOGLE_PLACE_ID_PATTERN.test(placeId)) {
    throw new TypeError("place_id must contain only letters, digits, hyphens, or underscores");
  }
  return {
    place_id: placeId,
    session_token: parseAutocompleteSessionToken(parsed),
  };
}

export function parseAddressBodyValue(parsed: unknown): AddressResolvePayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const address = (parsed as { address?: unknown }).address;
  if (typeof address !== "string" || address.trim().length === 0) {
    throw new TypeError("Request body must include non-empty string field: address");
  }
  const trimmed = address.trim();
  if (trimmed.length > MAX_ADDRESS_INPUT_LENGTH) {
    throw new TypeError(`address must be at most ${MAX_ADDRESS_INPUT_LENGTH} characters`);
  }

  return {
    address: trimmed,
  };
}

export function parsePublicAddressResolveBodyValue(parsed: unknown): PublicAddressResolvePayload {
  const { address } = parseAddressBodyValue(parsed);
  const acceptedTermsVersion = (parsed as { accepted_terms_version?: unknown }).accepted_terms_version;
  if (typeof acceptedTermsVersion !== "string" || acceptedTermsVersion.trim().length === 0) {
    throw new TypeError("Request body must include non-empty string field: accepted_terms_version");
  }

  return {
    address,
    accepted_terms_version: acceptedTermsVersion.trim(),
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

function assertNoUnknownFields(record: Record<string, unknown>, allowedFields: readonly string[]): void {
  const allowed = new Set(allowedFields);
  const unknown = Object.keys(record).filter((field) => !allowed.has(field));
  if (unknown.length > 0) {
    throw new TypeError(`Request body contains unknown field: ${unknown[0]}`);
  }
}

function parseOptionalStringField(record: Record<string, unknown>, fieldName: string): string | null {
  const value = record[fieldName];
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== "string") {
    throw new TypeError(`${fieldName} must be a string when provided`);
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** What the user is (or was last) looking at — the chat widget sends the
 * current or most recent candidate/election page so deictic questions
 * ("tell me more about this candidate") resolve deterministically. */
export type ChatbotAskContext =
  | { kind: "candidate"; id: string }
  | { kind: "election"; id: string };

export type ChatbotAskPayload = {
  question: string;
  /** Previous user turn, for deterministic follow-up scope carry-over. */
  previousQuestion: string | null;
  context: ChatbotAskContext | null;
};

export const MAX_CHATBOT_QUESTION_LENGTH = 500;

function parseChatbotAskContext(value: unknown): ChatbotAskContext | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("context must be an object when provided");
  }
  const record = value as Record<string, unknown>;
  assertNoUnknownFields(record, ["candidate_id", "election_id"]);
  const candidateId = record.candidate_id;
  const electionId = record.election_id;
  if ((candidateId === undefined) === (electionId === undefined)) {
    throw new TypeError("context must include exactly one of candidate_id or election_id");
  }
  const raw = candidateId ?? electionId;
  if (typeof raw !== "string" || !isUuid(raw.trim())) {
    throw new TypeError("context id must be a valid UUID");
  }
  return candidateId !== undefined
    ? { kind: "candidate", id: raw.trim() }
    : { kind: "election", id: raw.trim() };
}

export function parseChatbotAskBodyValue(parsed: unknown): ChatbotAskPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  const record = parsed as Record<string, unknown>;
  assertNoUnknownFields(record, ["question", "previous_question", "context"]);

  const question = parseStringField(parsed, "question");
  if (question.length > MAX_CHATBOT_QUESTION_LENGTH) {
    throw new TypeError(`question must be at most ${MAX_CHATBOT_QUESTION_LENGTH} characters`);
  }

  const previousQuestion = parseOptionalStringField(record, "previous_question");
  if (previousQuestion !== null && previousQuestion.length > MAX_CHATBOT_QUESTION_LENGTH) {
    throw new TypeError(`previous_question must be at most ${MAX_CHATBOT_QUESTION_LENGTH} characters`);
  }

  return { question, previousQuestion, context: parseChatbotAskContext(record.context) };
}

export function parseContentReportBodyValue(parsed: unknown): ContentReportPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const record = parsed as Record<string, unknown>;
  assertNoUnknownFields(record, ["entity_type", "entity_id", "message", "suggested_source_url", "reporter_email"]);

  const rawEntityType = record.entity_type;
  if (typeof rawEntityType !== "string") {
    throw new TypeError("Request body must include string field: entity_type");
  }
  const entityType = rawEntityType.trim();
  if (!(CONTENT_REPORT_ENTITY_TYPES as readonly string[]).includes(entityType)) {
    throw new TypeError(`entity_type must be one of: ${CONTENT_REPORT_ENTITY_TYPES.join(", ")}`);
  }

  const rawEntityId = record.entity_id;
  if (typeof rawEntityId !== "string") {
    throw new TypeError("Request body must include UUID string field: entity_id");
  }
  const entityId = rawEntityId.trim();
  if (!isUuid(entityId)) {
    throw new TypeError(`entity_id must be a valid UUID: ${entityId}`);
  }

  const rawMessage = record.message;
  if (typeof rawMessage !== "string" || rawMessage.trim().length === 0) {
    throw new TypeError("Request body must include non-empty string field: message");
  }
  const message = rawMessage.trim();
  if (message.length > MAX_CONTENT_REPORT_MESSAGE_LENGTH) {
    throw new TypeError(`message must be at most ${MAX_CONTENT_REPORT_MESSAGE_LENGTH} characters`);
  }

  const suggestedSourceUrl = parseOptionalStringField(record, "suggested_source_url");
  if (suggestedSourceUrl !== null) {
    if (suggestedSourceUrl.length > MAX_CONTENT_REPORT_SOURCE_URL_LENGTH) {
      throw new TypeError(`suggested_source_url must be at most ${MAX_CONTENT_REPORT_SOURCE_URL_LENGTH} characters`);
    }
    let parsedUrl: URL;
    try {
      parsedUrl = new URL(suggestedSourceUrl);
    } catch {
      throw new TypeError("suggested_source_url must be a valid http(s) URL");
    }
    if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
      throw new TypeError("suggested_source_url must be a valid http(s) URL");
    }
  }

  const reporterEmail = parseOptionalStringField(record, "reporter_email");
  if (reporterEmail !== null) {
    if (reporterEmail.length > MAX_CONTENT_REPORT_EMAIL_LENGTH) {
      throw new TypeError(`reporter_email must be at most ${MAX_CONTENT_REPORT_EMAIL_LENGTH} characters`);
    }
    if (!/^\S+@\S+\.\S+$/.test(reporterEmail)) {
      throw new TypeError("reporter_email must be a valid email address when provided");
    }
  }

  return {
    entityType: entityType as ContentReportEntityType,
    entityId,
    message,
    ...(suggestedSourceUrl === null ? {} : { suggestedSourceUrl }),
    ...(reporterEmail === null ? {} : { reporterEmail }),
  };
}

export function parseAuthRegisterBodyValue(parsed: unknown): AuthRegisterPayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const email = parseEmailField(parsed, "email");
  const password = parseStringField(parsed, "password");
  // Clickwrap: registration must record which terms version was accepted.
  const acceptedTermsVersion = parseStringField(parsed, "accepted_terms_version");
  const firstName = (parsed as { first_name?: unknown }).first_name;
  if (firstName !== undefined && (typeof firstName !== "string" || firstName.trim().length === 0)) {
    throw new TypeError("first_name must be a non-empty string when provided");
  }
  if (typeof firstName === "string" && firstName.trim().length > MAX_FIRST_NAME_LENGTH) {
    throw new TypeError(`first_name must be at most ${MAX_FIRST_NAME_LENGTH} characters`);
  }

  return {
    email,
    password,
    accepted_terms_version: acceptedTermsVersion,
    ...(firstName === undefined ? {} : { first_name: firstName.trim() }),
  };
}

export function parseAuthLoginBodyValue(parsed: unknown): AuthLoginPayload {
  return {
    email: parseEmailField(parsed, "email"),
    password: parseStringField(parsed, "password"),
  };
}

export function parseAuthGoogleBodyValue(parsed: unknown): AuthGooglePayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const credential = parseStringField(parsed, "credential");
  const intent = (parsed as { intent?: unknown }).intent;
  if (intent !== "login" && intent !== "signup") {
    throw new TypeError('intent must be "login" or "signup"');
  }
  const acceptedTermsVersion = (parsed as { accepted_terms_version?: unknown }).accepted_terms_version;
  if (
    acceptedTermsVersion !== undefined &&
    (typeof acceptedTermsVersion !== "string" || acceptedTermsVersion.trim().length === 0)
  ) {
    throw new TypeError("accepted_terms_version must be a non-empty string when provided");
  }

  return {
    credential,
    intent,
    ...(acceptedTermsVersion === undefined ? {} : { accepted_terms_version: acceptedTermsVersion.trim() }),
  };
}

export function parseAuthForgotPasswordBodyValue(parsed: unknown): AuthForgotPasswordPayload {
  return {
    email: parseEmailField(parsed, "email"),
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

export function parseAuthVerifyEmailChangeBodyValue(parsed: unknown): AuthVerifyEmailChangePayload {
  return {
    token: parseStringField(parsed, "token"),
  };
}

export function parseMePasswordBodyValue(parsed: unknown): MePasswordPayload {
  return {
    current_password: parseStringField(parsed, "current_password"),
    new_password: parseStringField(parsed, "new_password"),
  };
}

export function parseMeEmailBodyValue(parsed: unknown): MeEmailPayload {
  return {
    new_email: parseEmailField(parsed, "new_email"),
    password: parseStringField(parsed, "password"),
  };
}

export function parseMeDeleteBodyValue(parsed: unknown): MeDeletePayload {
  return {
    password: parseStringField(parsed, "password"),
  };
}

export function parseMeUpdateBodyValue(parsed: unknown): MeUpdatePayload {
  const firstName = parseStringField(parsed, "first_name");
  if (firstName.length > MAX_FIRST_NAME_LENGTH) {
    throw new TypeError(`first_name must be at most ${MAX_FIRST_NAME_LENGTH} characters`);
  }
  return {
    first_name: firstName,
  };
}

export function parseMeTermsAcceptanceBodyValue(parsed: unknown): MeTermsAcceptancePayload {
  return {
    accepted_terms_version: parseStringField(parsed, "accepted_terms_version"),
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

// [ballot-personalized-ordering]
// Parses the PUT /api/me/ballot-preferences body: a full replace of both
// fields, mirroring the research-area preferences contract.
export function parseBallotPreferencesBodyValue(parsed: unknown): UserBallotPreferences {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const sort = (parsed as { sort?: unknown }).sort;
  if (typeof sort !== "string" || !isBallotSummarySort(sort.trim())) {
    throw new TypeError(`Body field sort must be one of: ${BALLOT_SUMMARY_SORTS.join(", ")}`);
  }

  const followedFirst = (parsed as { followed_first?: unknown }).followed_first;
  if (typeof followedFirst !== "boolean") {
    throw new TypeError("Body field followed_first must be a boolean");
  }

  return { sort: sort.trim() as BallotSummarySort, followed_first: followedFirst };
}

export function parseEmailPreferencesBodyValue(parsed: unknown): UserEmailPreferences {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const record = parsed as Record<string, unknown>;
  for (const field of ["email_digest", "email_election_reminders", "email_new_election_alerts", "email_issue_updates"] as const) {
    if (typeof record[field] !== "boolean") {
      throw new TypeError(`Body field ${field} must be a boolean`);
    }
  }

  return {
    email_digest: record.email_digest as boolean,
    email_election_reminders: record.email_election_reminders as boolean,
    email_new_election_alerts: record.email_new_election_alerts as boolean,
    email_issue_updates: record.email_issue_updates as boolean,
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

export type ElectionChoicePayload = UserElectionChoiceInput;

// Two mutually exclusive shapes: an office pick ({candidate_id, chosen}) or
// a ballot-measure position ({measure_position: 'yes'|'no'|null}). The
// race-type check against the actual election happens in the writer.
export function parseElectionChoiceBodyValue(parsed: unknown): ElectionChoicePayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const payload = parsed as {
    election_id?: unknown;
    candidate_id?: unknown;
    chosen?: unknown;
    measure_position?: unknown;
  };
  if (typeof payload.election_id !== "string") {
    throw new TypeError("Request body must include UUID string field: election_id");
  }
  const electionId = payload.election_id.trim();
  if (!isUuid(electionId)) {
    throw new TypeError(`election_id must be a valid UUID: ${electionId}`);
  }

  const hasCandidate = payload.candidate_id !== undefined;
  const hasMeasure = payload.measure_position !== undefined;
  if (hasCandidate === hasMeasure) {
    throw new TypeError("Request body must include exactly one of: candidate_id, measure_position");
  }

  if (hasCandidate) {
    if (typeof payload.candidate_id !== "string") {
      throw new TypeError("candidate_id must be a UUID string");
    }
    const candidateId = payload.candidate_id.trim();
    if (!isUuid(candidateId)) {
      throw new TypeError(`candidate_id must be a valid UUID: ${candidateId}`);
    }
    if (typeof payload.chosen !== "boolean") {
      throw new TypeError("Request body must include boolean field: chosen");
    }
    return { electionId, candidateId, chosen: payload.chosen };
  }

  if (payload.chosen !== undefined) {
    throw new TypeError("chosen applies only to candidate choices");
  }
  if (payload.measure_position !== null && payload.measure_position !== "yes" && payload.measure_position !== "no") {
    throw new TypeError("measure_position must be 'yes', 'no', or null");
  }
  return { electionId, measurePosition: payload.measure_position };
}

// Expo push tokens are opaque short strings (ExponentPushToken[…]); the cap
// guards the unique index and storage against garbage, not the format — the
// Expo push API is the format authority and rejects invalid tokens itself.
const MAX_PUSH_TOKEN_LENGTH = 512;

function parsePushTokenField(record: Record<string, unknown>): string {
  const raw = record.expo_push_token;
  if (typeof raw !== "string" || raw.trim().length === 0) {
    throw new TypeError("Request body must include string field: expo_push_token");
  }
  const token = raw.trim();
  if (token.length > MAX_PUSH_TOKEN_LENGTH) {
    throw new TypeError(`expo_push_token must be at most ${MAX_PUSH_TOKEN_LENGTH} characters`);
  }
  return token;
}

// Parses the POST /api/me/push-tokens body.
export function parsePushTokenRegisterBodyValue(parsed: unknown): RegisterUserPushTokenInput {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  const record = parsed as Record<string, unknown>;

  const expoPushToken = parsePushTokenField(record);

  const platform = record.platform;
  if (platform !== "ios" && platform !== "android") {
    throw new TypeError('Body field platform must be "ios" or "android"');
  }

  const rawNativeToken = record.native_token;
  if (rawNativeToken !== undefined && rawNativeToken !== null && typeof rawNativeToken !== "string") {
    throw new TypeError("native_token must be a string or null");
  }
  const nativeToken = typeof rawNativeToken === "string" ? rawNativeToken.trim() : "";
  if (nativeToken.length > MAX_PUSH_TOKEN_LENGTH) {
    throw new TypeError(`native_token must be at most ${MAX_PUSH_TOKEN_LENGTH} characters`);
  }

  return {
    expoPushToken,
    nativeToken: nativeToken.length > 0 ? nativeToken : null,
    platform,
  };
}

// Parses the DELETE /api/me/push-tokens body.
export function parsePushTokenDeleteBodyValue(parsed: unknown): { expoPushToken: string } {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  return { expoPushToken: parsePushTokenField(parsed as Record<string, unknown>) };
}

// [ballot-personalized-ordering]
// Parses the optional `sort` and `followed_first` query parameters shared by
// the ballot endpoints. Throws TypeError (mapped to HTTP 400) on invalid
// values; omitted params leave the reader defaults in place.
export function parseBallotSummaryOptions(url: URL): Pick<BallotSummaryOptions, "sort" | "followedFirst"> {
  const options: Pick<BallotSummaryOptions, "sort" | "followedFirst"> = {};

  const rawSort = url.searchParams.get("sort");
  if (rawSort !== null) {
    const sort = rawSort.trim();
    if (!isBallotSummarySort(sort)) {
      throw new TypeError(`Query parameter sort must be one of: ${BALLOT_SUMMARY_SORTS.join(", ")}`);
    }
    options.sort = sort satisfies BallotSummarySort;
  }

  const rawFollowedFirst = url.searchParams.get("followed_first");
  if (rawFollowedFirst !== null) {
    const value = rawFollowedFirst.trim().toLowerCase();
    if (value !== "true" && value !== "false") {
      throw new TypeError("Query parameter followed_first must be true or false");
    }
    options.followedFirst = value === "true";
  }

  return options;
}

/** ?state=CA — case-insensitive two-letter state/DC abbreviation. */
export function parseStateResourcesState(url: URL): string {
  const raw = (url.searchParams.get("state") ?? "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(raw)) {
    throw new TypeError("Query parameter state must be a two-letter state abbreviation");
  }
  return raw;
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

// Matches the client hook's CANDIDATE_SEARCH_MIN_CHARS: the endpoint is
// public and runs an unindexed scan, so single-character sweeps are refused
// server-side too.
const MIN_CANDIDATE_SEARCH_QUERY_LENGTH = 2;
const MAX_CANDIDATE_SEARCH_QUERY_LENGTH = 100;

export function parseCandidateSearchQuery(url: URL): string {
  const query = (url.searchParams.get("q") ?? "").trim();
  if (query.length < MIN_CANDIDATE_SEARCH_QUERY_LENGTH) {
    throw new TypeError(`Query parameter q requires at least ${MIN_CANDIDATE_SEARCH_QUERY_LENGTH} characters`);
  }
  if (query.length > MAX_CANDIDATE_SEARCH_QUERY_LENGTH) {
    throw new TypeError(`Query parameter q supports at most ${MAX_CANDIDATE_SEARCH_QUERY_LENGTH} characters`);
  }
  return query;
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

// Matches /api/elections/:election_id/candidates/:candidate_id/finance.
// Shares the election-detail prefix, so the router must test this predicate
// before isElectionDetailPath.
const CANDIDATE_ELECTION_FINANCE_PATH_PATTERN = /^\/api\/elections\/([^/]+)\/candidates\/([^/]+)\/finance$/;

export function isCandidateElectionFinancePath(pathname: string): boolean {
  return CANDIDATE_ELECTION_FINANCE_PATH_PATTERN.test(pathname);
}

export function parseCandidateElectionFinancePath(url: URL): { electionId: string; candidateId: string } {
  const match = CANDIDATE_ELECTION_FINANCE_PATH_PATTERN.exec(url.pathname);
  if (!match) {
    throw new TypeError(
      "Candidate election finance path must be /api/elections/:election_id/candidates/:candidate_id/finance"
    );
  }
  const electionId = match[1].trim();
  const candidateId = match[2].trim();
  if (!isUuid(electionId)) {
    throw new TypeError(`Candidate election finance path contains invalid election UUID: ${electionId}`);
  }
  if (!isUuid(candidateId)) {
    throw new TypeError(`Candidate election finance path contains invalid candidate UUID: ${candidateId}`);
  }
  return { electionId, candidateId };
}

export function isPickCardPath(pathname: string): boolean {
  return pathname.startsWith(PICK_CARD_PATH_PREFIX);
}

// Share tokens are 32 random bytes base64url-encoded (43 chars today); the
// bounds leave room for future length changes without admitting junk. Not a
// UUID check — the token is an opaque capability, not a row id.
const PICK_CARD_TOKEN_PATTERN = /^[A-Za-z0-9_-]{20,128}$/;

export function parsePickCardToken(url: URL): string {
  const token = url.pathname.slice(PICK_CARD_PATH_PREFIX.length).trim();
  if (token.length === 0 || token.includes("/")) {
    throw new TypeError("Pick card path must be /api/pick-cards/:token");
  }
  if (!PICK_CARD_TOKEN_PATTERN.test(token)) {
    throw new TypeError("Pick card path contains an invalid token");
  }
  return token;
}

// The share-card preview image: /api/pick-cards/:token/og-image.png. A fixed
// suffix rather than a query parameter because scrapers treat the og:image
// URL as opaque — the path just has to be self-describing.
export const PICK_CARD_IMAGE_SUFFIX = "/og-image.png";

export function isPickCardImagePath(pathname: string): boolean {
  return pathname.startsWith(PICK_CARD_PATH_PREFIX) && pathname.endsWith(PICK_CARD_IMAGE_SUFFIX);
}

export function parsePickCardImageToken(url: URL): string {
  const token = url.pathname.slice(PICK_CARD_PATH_PREFIX.length, -PICK_CARD_IMAGE_SUFFIX.length).trim();
  if (token.length === 0 || token.includes("/")) {
    throw new TypeError("Pick card image path must be /api/pick-cards/:token/og-image.png");
  }
  if (!PICK_CARD_TOKEN_PATTERN.test(token)) {
    throw new TypeError("Pick card image path contains an invalid token");
  }
  return token;
}

export type PickCardSharePayload = {
  electionDate: string;
};

const PICK_CARD_ELECTION_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function parsePickCardShareBodyValue(parsed: unknown): PickCardSharePayload {
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }
  const payload = parsed as { election_date?: unknown };
  if (typeof payload.election_date !== "string") {
    throw new TypeError("Request body must include string field: election_date");
  }
  const electionDate = payload.election_date.trim();
  // Round-trip through UTC instead of trusting Date.parse alone: V8 rolls
  // impossible days over ("2026-02-30" parses as March 2), which would pass
  // a NaN check and later 500 on Postgres's ::date cast instead of 400 here.
  // Year 0000 round-trips unchanged in JS (it means 1 BC there) but does
  // not exist in Postgres's calendar, so it needs its own rejection.
  const parsedDate = new Date(`${electionDate}T00:00:00Z`);
  const isRealDate =
    PICK_CARD_ELECTION_DATE_PATTERN.test(electionDate) &&
    !electionDate.startsWith("0000") &&
    !Number.isNaN(parsedDate.getTime()) &&
    parsedDate.toISOString().slice(0, 10) === electionDate;
  if (!isRealDate) {
    throw new TypeError(`election_date must be a valid YYYY-MM-DD date: ${electionDate}`);
  }
  return { electionDate };
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

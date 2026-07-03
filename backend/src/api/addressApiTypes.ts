import type { BallotLookupElection } from "../pipeline/address/ballotLookup.js";
// [ballot-personalized-ordering] see ballotElectionOrdering.ts for removal notes
import type {
  BallotSummaryOptions,
  OrderedBallotSummaryResult,
} from "../pipeline/address/ballotElectionOrdering.js";
import type { UserBallotPreferences } from "../pipeline/users/userBallotPreferences.js";
import type { UserEmailPreferences } from "../pipeline/users/userEmailPreferences.js";
import type { UserIdentity } from "../pipeline/users/userIdentity.js";
import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";
import type { AddressSuggestion, RetrievedSuggestedAddress } from "../pipeline/address/googlePlacesAutocomplete.js";
import type { CandidateDetailResult } from "../pipeline/candidates/candidateDetailReader.js";
import type { AuthSessionCookieOptions } from "../auth/authCookies.js";
import type { AuthService } from "../auth/authService.js";
import type { AuthenticatedAddressDistrictUpdateResult } from "../pipeline/users/userAddressDistrictUpdater.js";
import type { InitializeUserDistrictsResult } from "../pipeline/users/userDistrictInitializer.js";
import type {
  UserCandidateFollowInput,
  UserCandidateFollowsResult,
  UserCandidateFollowUpdateResult,
} from "../pipeline/users/userCandidateFollows.js";
import type {
  ResearchAreaCatalogResult,
  UserResearchAreaPreferenceInput,
  UserResearchAreaPreferencesResult,
} from "../pipeline/users/userResearchAreaPreferences.js";
import type { AddressApiClientIpInput } from "./addressApiClientIp.js";
import type { AddressResolutionDiagnostics } from "./addressApiResponses.js";

export type { ResearchAreaCatalogItem, ResearchAreaCatalogResult } from "../pipeline/users/userResearchAreaPreferences.js";

export type AuthenticatedAddressUpdateResult = AuthenticatedAddressDistrictUpdateResult;

export type AuthenticatedResearchAreaPreferencesResult = UserResearchAreaPreferencesResult;

export type AuthenticatedCandidateFollowsResult = UserCandidateFollowsResult;

export type AuthenticatedCandidateFollowUpdateResult = UserCandidateFollowUpdateResult;

export type AddressApiRateLimitInput = {
  clientIp: string;
  method: string;
  pathname: string;
};

export type AddressApiRateLimitResult = {
  allowed: boolean;
  retryAfterSeconds?: number;
};

export type AuthApiRateLimitInput = {
  clientIp: string;
  email: string;
  method: string;
  pathname: string;
};

export type AuthApiRateLimitResult = {
  allowed: boolean;
  retryAfterSeconds?: number;
};

export type AddressApiServerOptions = {
  authService?: AuthService;
  resolveAddress: (address: string) => Promise<AddressResolutionResult>;
  suggestAddresses?: (input: { input: string; sessionToken: string }) => Promise<AddressSuggestion[]>;
  retrieveSuggestedAddress?: (input: { placeId: string; sessionToken: string }) => Promise<RetrievedSuggestedAddress>;
  // [ballot-personalized-ordering]: options + ordered result; on feature
  // removal these become (districtIds) => Promise<BallotSummaryResult>.
  lookupBallotSummaries?: (
    districtIds: readonly string[],
    options?: BallotSummaryOptions
  ) => Promise<OrderedBallotSummaryResult>;
  lookupAuthenticatedBallotSummaries?: (
    userId: string,
    options?: BallotSummaryOptions
  ) => Promise<OrderedBallotSummaryResult>;
  lookupAuthenticatedUserEmailVerified?: (userId: string) => Promise<boolean>;
  lookupCandidateDetail?: (candidateId: string, userId?: string | null) => Promise<CandidateDetailResult | null>;
  lookupElectionDetail?: (electionId: string) => Promise<BallotLookupElection | null>;
  listResearchAreas?: () => Promise<ResearchAreaCatalogResult>;
  listAuthenticatedCandidateFollows?: (userId: string) => Promise<AuthenticatedCandidateFollowsResult>;
  setAuthenticatedCandidateFollow?: (
    userId: string,
    input: UserCandidateFollowInput
  ) => Promise<AuthenticatedCandidateFollowUpdateResult>;
  // [ballot-personalized-ordering]
  getAuthenticatedBallotPreferences?: (userId: string) => Promise<UserBallotPreferences>;
  setAuthenticatedBallotPreferences?: (
    userId: string,
    preferences: UserBallotPreferences
  ) => Promise<UserBallotPreferences>;
  /**
   * GET /api/me: identity for the session holder. Unlike the other /api/me
   * handlers this is not gated on email verification — an unverified user
   * must be able to learn that they are unverified.
   */
  getAuthenticatedUser?: (userId: string) => Promise<UserIdentity>;
  getAuthenticatedEmailPreferences?: (userId: string) => Promise<UserEmailPreferences>;
  setAuthenticatedEmailPreferences?: (
    userId: string,
    preferences: UserEmailPreferences
  ) => Promise<UserEmailPreferences>;
  /**
   * Signed-token digest unsubscribe (no session). mode "confirm" only
   * verifies the token (GET renders a confirmation form and must not mutate:
   * mail scanners and prefetchers GET every link in email bodies); mode
   * "execute" turns the digest off. Returns "ok" or "invalid_token"; a valid
   * token for a since-deleted user reports "ok" so the page does not leak
   * account state.
   */
  unsubscribeFromEmailDigest?: (token: string, mode: "confirm" | "execute") => Promise<"ok" | "invalid_token">;
  listAuthenticatedResearchAreaPreferences?: (userId: string) => Promise<AuthenticatedResearchAreaPreferencesResult>;
  replaceAuthenticatedResearchAreaPreferences?: (
    userId: string,
    preferences: readonly UserResearchAreaPreferenceInput[]
  ) => Promise<AuthenticatedResearchAreaPreferencesResult>;
  updateAuthenticatedAddressDistricts?: (userId: string, address: string) => Promise<AuthenticatedAddressUpdateResult>;
  initializeUserDistricts?: (
    input: {
      userId: string;
      districtIds: readonly string[];
    }
  ) => Promise<InitializeUserDistrictsResult>;
  allowedOrigins?: readonly string[];
  authSessionCookieOptions?: Omit<AuthSessionCookieOptions, "maxAgeSeconds">;
  logDiagnostics?: (diagnostics: AddressResolutionDiagnostics) => void;
  rateLimit?: (input: AddressApiRateLimitInput) => AddressApiRateLimitResult;
  authRateLimit?: (input: AuthApiRateLimitInput) => AuthApiRateLimitResult;
  resolveClientIp?: (input: AddressApiClientIpInput) => string;
  resolveAuthenticatedUserId?: (input: {
    headers: AddressApiClientIpInput["headers"];
  }) => string | null | Promise<string | null>;
};

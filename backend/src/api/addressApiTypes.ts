import type { BallotLookupElection, BallotSummaryResult } from "../pipeline/address/ballotLookup.js";
import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";
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
  lookupBallotSummaries?: (districtIds: readonly string[]) => Promise<BallotSummaryResult>;
  lookupAuthenticatedBallotSummaries?: (userId: string) => Promise<BallotSummaryResult>;
  lookupAuthenticatedUserEmailVerified?: (userId: string) => Promise<boolean>;
  lookupCandidateDetail?: (candidateId: string, userId?: string | null) => Promise<CandidateDetailResult | null>;
  lookupElectionDetail?: (electionId: string) => Promise<BallotLookupElection | null>;
  listResearchAreas?: () => Promise<ResearchAreaCatalogResult>;
  listAuthenticatedCandidateFollows?: (userId: string) => Promise<AuthenticatedCandidateFollowsResult>;
  setAuthenticatedCandidateFollow?: (
    userId: string,
    input: UserCandidateFollowInput
  ) => Promise<AuthenticatedCandidateFollowUpdateResult>;
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

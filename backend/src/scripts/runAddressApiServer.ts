import type { Server } from "node:http";
import { Pool } from "pg";
import { createClient } from "redis";
import { SESv2Client } from "@aws-sdk/client-sesv2";

import {
  createInMemoryAuthApiRateLimiter,
  DEFAULT_AUTH_API_RATE_LIMIT_MAX_BUCKETS,
  DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_EMAIL,
  DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_IP,
  DEFAULT_AUTH_API_RATE_LIMIT_WINDOW_MS,
} from "../api/authApiRateLimiter.js";
import type { AuthSessionCookieOptions } from "../auth/authCookies.js";
import { createConsoleAuthMailer, createSesAuthMailer } from "../auth/authMailer.js";
import {
  createAuthService,
  DEFAULT_AUTH_EMAIL_VERIFICATION_TTL_SECONDS,
  DEFAULT_AUTH_PASSWORD_RESET_TTL_SECONDS,
  DEFAULT_AUTH_SESSION_TTL_SECONDS,
} from "../auth/authService.js";
import {
  assertTrustedUserIdHeaderConfigIsSafe,
  createSessionAwareTrustedUserIdResolver,
  createTrustedUserIdResolver,
} from "../api/addressApiAuth.js";
import { createTrustedClientIpResolver } from "../api/addressApiClientIp.js";
import { toAddressResolutionDiagnostics, type AddressResolutionDiagnostics } from "../api/addressApiResponses.js";
import { buildAllowedOrigins } from "../api/apiCors.js";
import { createApiApp } from "../api/apiServer.js";
import {
  createInMemoryContentReportRateLimiter,
  DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS,
  DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_REQUESTS,
  DEFAULT_CONTENT_REPORT_RATE_LIMIT_WINDOW_MS,
} from "../api/contentReportRateLimiter.js";
import { loadProjectEnv } from "../config/env.js";
import { captureError, describeError, flushSentry, initSentryFromEnv } from "../observability/sentry.js";
import {
  createInMemoryAddressApiRateLimiter,
  DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS,
  DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS,
  DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS,
} from "../api/addressApiRateLimiter.js";
import {
  lookupBallotSummariesByDistrictIds,
  lookupCandidateElectionFinanceSummaryById,
  lookupElectionDetailById,
} from "../pipeline/address/ballotLookup.js";
// [ballot-personalized-ordering]
import { applyBallotElectionOrdering } from "../pipeline/address/ballotElectionOrdering.js";
import { resolveAddressToDistricts } from "../pipeline/address/addressResolverService.js";
import { createAutoDistrictResearchTrigger } from "../pipeline/address/autoDistrictResearch.js";
import {
  enqueueManualDistrictResearchRequestsForStaleDistricts,
  type ManualResearchTriggerSource,
} from "../pipeline/address/manualDistrictResearchRequests.js";
import { readAutoDistrictResearchMode } from "../config/featureFlags.js";
import {
  DEFAULT_ELECTIONS_SEARCH_COOLDOWN_DAYS,
  readElectionsSearchCooldownDaysFromEnv,
} from "../pipeline/elections/electionsSearchPolicy.js";
import {
  DEFAULT_GOOGLE_PLACES_TIMEOUT_MS,
  retrieveSuggestedAddressWithGooglePlaces,
  suggestAddressesWithGooglePlaces,
} from "../pipeline/address/googlePlacesAutocomplete.js";
import { lookupCandidateDetailById } from "../pipeline/candidates/candidateDetailReader.js";
import { searchCandidatesByName } from "../pipeline/candidates/candidateSearchReader.js";
import { DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS } from "../pipeline/address/addressResolutionCache.js";
import {
  DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
} from "../pipeline/address/censusAddressGeocoder.js";
import { updateAuthenticatedAddressDistricts } from "../pipeline/users/userAddressDistrictUpdater.js";
import { createContentReport } from "../pipeline/reports/contentReports.js";
import { listUserCandidateFollows, setUserCandidateFollow } from "../pipeline/users/userCandidateFollows.js";
import { initializeUserDistricts } from "../pipeline/users/userDistrictInitializer.js";
import { listUserDistrictIds } from "../pipeline/users/userDistrictReader.js";
import { replaceUserDistricts } from "../pipeline/users/userDistrictReplacer.js";
import {
  listSelectableResearchAreas,
  listUserResearchAreaPreferences,
  replaceUserResearchAreaPreferences,
} from "../pipeline/users/userResearchAreaPreferences.js";
import { getUserBallotPreferences, setUserBallotPreferences } from "../pipeline/users/userBallotPreferences.js";
import {
  disableUserEmailDigest,
  disableUserEmailElectionReminders,
  disableUserEmailIssueUpdates,
  disableUserEmailNewElectionAlerts,
  getUserEmailPreferences,
  setUserEmailPreferences,
  UserEmailPreferencesError,
} from "../pipeline/users/userEmailPreferences.js";
import { registerUserPushToken, revokeUserPushToken } from "../pipeline/users/userPushTokens.js";
import { verifyEmailUnsubscribeToken } from "../pipeline/users/emailUnsubscribeToken.js";
import { getUserIdentity, setUserFirstName } from "../pipeline/users/userIdentity.js";
import { createCachedSiteSitemap } from "../pipeline/sitemap/siteSitemap.js";

function readEnv(name: string, fallback?: string): string {
  const value = process.env[name]?.trim() || fallback;
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function readPort(): number {
  const raw = process.env.ADDRESS_API_PORT ?? "3001";
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    throw new Error(`Invalid ADDRESS_API_PORT: ${raw}`);
  }
  return parsed;
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return parsed;
}

function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }
  if (["1", "true", "yes", "on"].includes(raw)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(raw)) {
    return false;
  }
  throw new Error(`Invalid ${name}: ${process.env[name]}`);
}

function readAllowedOrigins(): string[] {
  // Same-origin browser POSTs still carry an Origin header, so the site's own
  // origin must always be in the allowlist even when no cross-origin frontend
  // exists; derive it from the public base URLs so an unset
  // ADDRESS_API_ALLOWED_ORIGINS cannot 403 all browser mutations.
  return buildAllowedOrigins(process.env.ADDRESS_API_ALLOWED_ORIGINS, [
    process.env.SITE_ORIGIN,
    process.env.AUTH_PUBLIC_BASE_URL,
  ]);
}

function readOptionalEnv(name: string): string | null {
  const value = process.env[name]?.trim();
  return value && value.length > 0 ? value : null;
}

function readAuthCookieSameSiteEnv(name: string, fallback: "lax" | "strict" | "none"): "lax" | "strict" | "none" {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }
  if (raw === "lax" || raw === "strict" || raw === "none") {
    return raw;
  }
  throw new Error(`Invalid ${name}: ${process.env[name]}`);
}

function logAddressResolutionDiagnostics(diagnostics: AddressResolutionDiagnostics, source = "address_resolve"): void {
  if (diagnostics.missing_district_keys.length === 0 && diagnostics.warnings.length === 0) {
    return;
  }
  console.warn(
    JSON.stringify({
      type: "address_resolution_diagnostics",
      ts: new Date().toISOString(),
      source,
      address_match_count: diagnostics.address_match_count,
      district_key_count: diagnostics.district_keys.length,
      missing_district_keys: diagnostics.missing_district_keys,
      warnings: diagnostics.warnings,
    })
  );
}

async function main(): Promise<void> {
  loadProjectEnv();
  // Dark unless SENTRY_DSN is set (plan-error-monitoring.md Phase 2).
  if (initSentryFromEnv("api")) {
    console.log("error monitoring enabled (sentry)");
  }
  const pool = new Pool({ connectionString: readEnv("DATABASE_URL", "postgresql://localhost:5432/voteapp") });
  const host = process.env.ADDRESS_API_HOST?.trim() || "127.0.0.1";
  const port = readPort();
  const allowedOrigins = readAllowedOrigins();
  // Security boundary: only trust a client-IP header when a trusted proxy/gateway
  // owns that header and strips client-supplied copies. Otherwise rate limiting uses
  // the direct socket IP.
  const trustedClientIpHeader = readOptionalEnv("ADDRESS_API_TRUSTED_CLIENT_IP_HEADER");
  if (trustedClientIpHeader) {
    console.warn(
      `address API trusting client IP header "${trustedClientIpHeader}"; ensure the edge proxy strips client-supplied copies`
    );
  }
  // When set, the client-IP header is only trusted on requests carrying the
  // matching X-Edge-Secret header (attached by the Cloudflare Worker and the
  // SSR loaders). Closes the direct *.onrender.com spoofing bypass in
  // docs/deploy-render.md. Fail fast on a weak value, like the unsubscribe
  // secret above.
  const edgeSharedSecret = readOptionalEnv("EDGE_SHARED_SECRET");
  if (edgeSharedSecret && edgeSharedSecret.length < 32) {
    throw new Error("EDGE_SHARED_SECRET must be at least 32 characters");
  }
  if (edgeSharedSecret && !trustedClientIpHeader) {
    console.warn("EDGE_SHARED_SECRET is set but ADDRESS_API_TRUSTED_CLIENT_IP_HEADER is not; the secret has no effect");
  }
  // Security boundary: only trust a user-id header when an authenticated gateway
  // injects it and strips client-supplied copies. If unset, authenticated routes
  // fail closed with 401.
  const trustedUserIdHeader = readOptionalEnv("API_TRUSTED_USER_ID_HEADER");
  if (trustedUserIdHeader) {
    console.warn(
      `API trusting authenticated user header "${trustedUserIdHeader}"; ensure the edge proxy authenticates requests and strips client-supplied copies`
    );
  }
  const trustedUserIdResolver = createTrustedUserIdResolver(trustedUserIdHeader);
  // Enables GET/POST /api/email/unsubscribe. Must match the secret the digest
  // sender signs footer links with. Fail fast on a weak secret rather than
  // 500ing on every unsubscribe click.
  const unsubscribeSecret = readOptionalEnv("NOTIFICATIONS_UNSUBSCRIBE_SECRET");
  if (unsubscribeSecret && unsubscribeSecret.length < 32) {
    throw new Error("NOTIFICATIONS_UNSUBSCRIBE_SECRET must be at least 32 characters");
  }
  const addressCacheEnabled = readBooleanEnv("ADDRESS_LOOKUP_CACHE_ENABLED", true);
  const rateLimitEnabled = readBooleanEnv("ADDRESS_API_RATE_LIMIT_ENABLED", true);
  const rateLimitWindowMs = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_WINDOW_MS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS
  );
  const rateLimitMaxRequests = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_MAX_REQUESTS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS
  );
  const rateLimitMaxBuckets = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_MAX_BUCKETS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS
  );
  const authRateLimitEnabled = readBooleanEnv("AUTH_API_RATE_LIMIT_ENABLED", true);
  const authRateLimitWindowMs = readPositiveIntegerEnv(
    "AUTH_API_RATE_LIMIT_WINDOW_MS",
    DEFAULT_AUTH_API_RATE_LIMIT_WINDOW_MS
  );
  const authRateLimitMaxRequestsPerIp = readPositiveIntegerEnv(
    "AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_IP",
    DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_IP
  );
  const authRateLimitMaxRequestsPerEmail = readPositiveIntegerEnv(
    "AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_EMAIL",
    DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_EMAIL
  );
  const authRateLimitMaxBuckets = readPositiveIntegerEnv(
    "AUTH_API_RATE_LIMIT_MAX_BUCKETS",
    DEFAULT_AUTH_API_RATE_LIMIT_MAX_BUCKETS
  );
  const contentReportRateLimitEnabled = readBooleanEnv("CONTENT_REPORT_RATE_LIMIT_ENABLED", true);
  const contentReportRateLimitWindowMs = readPositiveIntegerEnv(
    "CONTENT_REPORT_RATE_LIMIT_WINDOW_MS",
    DEFAULT_CONTENT_REPORT_RATE_LIMIT_WINDOW_MS
  );
  const contentReportRateLimitMaxRequests = readPositiveIntegerEnv(
    "CONTENT_REPORT_RATE_LIMIT_MAX_REQUESTS",
    DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_REQUESTS
  );
  const contentReportRateLimitMaxBuckets = readPositiveIntegerEnv(
    "CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS",
    DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS
  );
  const siteOrigin = readOptionalEnv("SITE_ORIGIN");
  if (!siteOrigin) {
    console.warn("SITE_ORIGIN is unset; /sitemap.xml will return 404");
  }
  const getSitemapXml = siteOrigin ? createCachedSiteSitemap({ db: pool, siteOrigin }) : undefined;
  const rateLimit = rateLimitEnabled
    ? createInMemoryAddressApiRateLimiter({
        windowMs: rateLimitWindowMs,
        maxRequests: rateLimitMaxRequests,
        maxBuckets: rateLimitMaxBuckets,
      })
    : undefined;
  const contentReportRateLimit = contentReportRateLimitEnabled
    ? createInMemoryContentReportRateLimiter({
        windowMs: contentReportRateLimitWindowMs,
        maxRequests: contentReportRateLimitMaxRequests,
        maxBuckets: contentReportRateLimitMaxBuckets,
      })
    : undefined;
  const addressCacheTtlSeconds = readPositiveIntegerEnv(
    "ADDRESS_LOOKUP_CACHE_TTL_SECONDS",
    DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS
  );
  // Auth needs Redis for sessions independently of the address cache toggle:
  // do not let ADDRESS_LOOKUP_CACHE_ENABLED=false silently disable auth.
  const authConfigured = Boolean(readOptionalEnv("AUTH_PUBLIC_BASE_URL"));
  assertTrustedUserIdHeaderConfigIsSafe({
    sessionAuthIntended: authConfigured,
    trustedUserIdHeader,
    allowTrustedHeaderWithSessions: readBooleanEnv("API_TRUSTED_USER_ID_HEADER_ALLOW_WITH_SESSIONS", false),
  });
  // Exactly one auto district research behavior fires per stale district: "ai"
  // drops an election draft into the staging pipeline (needs Redis for the
  // draft stream), "manual" enqueues an agent research request (Postgres only),
  // "off" no-ops. Mode and cooldown are validated at startup so a malformed
  // value fails fast instead of being swallowed per-request.
  const autoDistrictResearchMode = readAutoDistrictResearchMode();
  const autoDistrictResearchCooldownDays =
    autoDistrictResearchMode === "off"
      ? DEFAULT_ELECTIONS_SEARCH_COOLDOWN_DAYS
      : readElectionsSearchCooldownDaysFromEnv();
  const autoDistrictResearchNeedsRedis = autoDistrictResearchMode === "ai";
  const redis =
    addressCacheEnabled || authConfigured || autoDistrictResearchNeedsRedis
      ? createClient({ url: readEnv("REDIS_URL", "redis://localhost:6379") })
      : null;
  const buildAddressResolverOptions = () => ({
    cache: addressCacheEnabled && redis?.isOpen ? redis : undefined,
    cacheTtlSeconds: addressCacheTtlSeconds,
    geocoderOptions: {
      benchmark: readEnv("CENSUS_ADDRESS_GEOCODER_BENCHMARK", DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK),
      vintage: readEnv("CENSUS_ADDRESS_GEOCODER_VINTAGE", DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE),
      layers: readEnv("CENSUS_ADDRESS_GEOCODER_LAYERS", DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS),
      timeoutMs: readPositiveIntegerEnv("CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS", DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS),
    },
  });
  if (redis) {
    redis.on("error", (error) => {
      console.warn("Redis error (address cache/auth sessions/auto district research); continuing without failing requests", error);
    });
    try {
      // With the default reconnect strategy an unreachable Redis keeps this
      // await pending (retrying) until it connects, so a down Redis delays
      // boot rather than rejecting here.
      await redis.connect();
    } catch (error) {
      // A rejected connect() leaves the client permanently closed — node-redis
      // only auto-reconnects after a successful connect, and nothing re-calls
      // connect() — so continuing would run without auth sessions, the address
      // cache, and auto district research for the life of the process. Fail
      // the boot instead and let the process manager restart into a clean
      // retry.
      throw new Error(
        `Redis connection failed at startup: ${error instanceof Error ? error.message : String(error)}`,
        { cause: error }
      );
    }
  }

  const triggerAutoDistrictResearchAi = createAutoDistrictResearchTrigger({
    db: pool,
    getRedis: () => (redis?.isOpen ? redis : null),
    config: { enabled: autoDistrictResearchMode === "ai", ttlDays: autoDistrictResearchCooldownDays },
  });
  // Fire-and-forget dispatch: research stale districts without delaying or
  // failing the address response. The two destinations are mutually exclusive
  // by mode; both share the same staleness cooldown.
  const dispatchAutoDistrictResearch = (
    districts: Awaited<ReturnType<typeof resolveAddressToDistricts>>["districts"],
    triggerSource: Exclude<ManualResearchTriggerSource, "manual_seed">
  ) => {
    if (autoDistrictResearchMode === "ai") {
      void triggerAutoDistrictResearchAi(districts).catch((error) => {
        console.warn("auto district research (ai) trigger failed; address response unaffected", error);
      });
    } else if (autoDistrictResearchMode === "manual") {
      void enqueueManualDistrictResearchRequestsForStaleDistricts(pool, {
        districts,
        triggerSource,
        cooldownDays: autoDistrictResearchCooldownDays,
      }).catch((error) => {
        console.warn("manual district research enqueue failed; address response unaffected", error);
      });
    }
  };
  const resolveAddressWithAutoResearch = async (
    inputAddress: string,
    triggerSource: Exclude<ManualResearchTriggerSource, "manual_seed"> = "address_resolve"
  ) => {
    const result = await resolveAddressToDistricts(pool, inputAddress, buildAddressResolverOptions());
    dispatchAutoDistrictResearch(result.districts, triggerSource);
    return result;
  };

  const authPublicBaseUrl = readOptionalEnv("AUTH_PUBLIC_BASE_URL");
  const authFromEmailAddress = readOptionalEnv("AUTH_FROM_EMAIL");
  const authReplyToEmailAddress = readOptionalEnv("AUTH_REPLY_TO_EMAIL");
  const authSesRegion = readOptionalEnv("AUTH_SES_REGION") ?? readOptionalEnv("AWS_REGION") ?? readOptionalEnv("AWS_DEFAULT_REGION");
  const authSessionCookieSameSite = readAuthCookieSameSiteEnv("AUTH_SESSION_COOKIE_SAME_SITE", "lax");
  // Default Secure from the public base URL scheme; browsers reject
  // SameSite=None cookies without Secure, so fail fast on that combination.
  const authSessionCookieSecure = readBooleanEnv(
    "AUTH_SESSION_COOKIE_SECURE",
    authPublicBaseUrl?.startsWith("https://") ?? false
  );
  if (authSessionCookieSameSite === "none" && !authSessionCookieSecure) {
    throw new Error("AUTH_SESSION_COOKIE_SECURE=true is required when AUTH_SESSION_COOKIE_SAME_SITE=none");
  }
  const authSessionCookieOptions: Omit<AuthSessionCookieOptions, "maxAgeSeconds"> = {
    sameSite: authSessionCookieSameSite,
    secure: authSessionCookieSecure,
    domain: readOptionalEnv("AUTH_SESSION_COOKIE_DOMAIN"),
    path: readOptionalEnv("AUTH_SESSION_COOKIE_PATH") ?? "/",
  };
  // AUTH_MAILER=console prints verification/reset links to stdout for local
  // development instead of sending email. Anything else (default "ses") uses
  // SES and requires AUTH_FROM_EMAIL plus a region.
  const authMailerKind = (readOptionalEnv("AUTH_MAILER") ?? "ses").toLowerCase();
  const authMailer =
    authMailerKind === "console"
      ? createConsoleAuthMailer()
      : authFromEmailAddress && authSesRegion
        ? createSesAuthMailer({
            sesClient: new SESv2Client({ region: authSesRegion }),
            fromEmailAddress: authFromEmailAddress,
            ...(authReplyToEmailAddress ? { replyToEmailAddress: authReplyToEmailAddress } : {}),
          })
        : null;
  const authService =
    authPublicBaseUrl && authMailer && redis?.isOpen
      ? createAuthService({
          db: pool,
          redis,
          mailer: authMailer,
          publicBaseUrl: authPublicBaseUrl,
          sessionTtlSeconds: DEFAULT_AUTH_SESSION_TTL_SECONDS,
          emailVerificationTtlSeconds: DEFAULT_AUTH_EMAIL_VERIFICATION_TTL_SECONDS,
          passwordResetTtlSeconds: DEFAULT_AUTH_PASSWORD_RESET_TTL_SECONDS,
        })
      : undefined;
  if (!authService && (authPublicBaseUrl || authFromEmailAddress || authSesRegion || authMailerKind === "console")) {
    console.warn(
      "authentication is partially configured but missing required settings or Redis; auth endpoints will return 500 until AUTH_PUBLIC_BASE_URL, Redis, and either AUTH_MAILER=console or AUTH_FROM_EMAIL + AUTH_SES_REGION/AWS_REGION are set"
    );
  }
  if (authService && authMailerKind === "console") {
    console.warn("auth mailer is in console mode: verification/reset links are printed to stdout; never use in production");
  }
  const authRateLimit =
    authService && authRateLimitEnabled
      ? createInMemoryAuthApiRateLimiter({
          windowMs: authRateLimitWindowMs,
          maxRequestsPerIp: authRateLimitMaxRequestsPerIp,
          maxRequestsPerEmail: authRateLimitMaxRequestsPerEmail,
          maxBuckets: authRateLimitMaxBuckets,
        })
      : undefined;

  const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
    redis: redis?.isOpen ? redis : null,
    trustedUserIdResolver,
    // Per-request epoch check: sessions created before a password
    // reset/change or logout-all carry an older epoch and stop
    // authenticating the moment the bump commits, regardless of Redis.
    lookupUserSessionEpoch: async (userId) => {
      const result = await pool.query<{ session_epoch: number }>(
        `
          SELECT session_epoch
          FROM public.users
          WHERE id = $1::uuid
            AND deleted_at IS NULL
        `,
        [userId]
      );
      const epoch = result.rows[0]?.session_epoch;
      return typeof epoch === "number" ? epoch : null;
    },
  });
  const lookupAuthenticatedUserEmailVerified = async (userId: string): Promise<boolean> => {
    const result = await pool.query<{ email_verified: boolean }>(
      `
        SELECT email_verified
        FROM public.users
        WHERE id = $1::uuid
          AND deleted_at IS NULL
      `,
      [userId]
    );
    return result.rows[0]?.email_verified ?? false;
  };

  // Google Places autocomplete proxy: enabled only when the API key is set.
  // Endpoints return 500 not-configured otherwise; the rest of the API is
  // unaffected.
  const googlePlacesApiKey = readOptionalEnv("GOOGLE_PLACES_API_KEY");
  const googlePlacesOptions = googlePlacesApiKey
    ? {
        apiKey: googlePlacesApiKey,
        timeoutMs: readPositiveIntegerEnv("GOOGLE_PLACES_TIMEOUT_MS", DEFAULT_GOOGLE_PLACES_TIMEOUT_MS),
      }
    : null;

  const app = createApiApp({
    allowedOrigins,
    authService,
    captureUnexpectedError: (error, context) =>
      captureError(error, {
        request_id: context.requestId,
        method: context.method,
        path: context.path,
      }),
    ...(googlePlacesOptions
      ? {
          suggestAddresses: (input: { input: string; sessionToken: string }) =>
            suggestAddressesWithGooglePlaces(input, googlePlacesOptions),
          retrieveSuggestedAddress: (input: { placeId: string; sessionToken: string }) =>
            retrieveSuggestedAddressWithGooglePlaces(input, googlePlacesOptions),
        }
      : {}),
    authRateLimit,
    contentReportRateLimit,
    authSessionCookieOptions,
    rateLimit,
    resolveClientIp: createTrustedClientIpResolver(trustedClientIpHeader, edgeSharedSecret),
    resolveAuthenticatedUserId,
    getSitemapXml,
    createContentReport: (input) => createContentReport(pool, input),
    logDiagnostics: logAddressResolutionDiagnostics,
    // [ballot-personalized-ordering]: the plain reader is decorated with the
    // sort/followed-first ordering; on feature removal call the reader alone.
    lookupBallotSummaries: async (districtIds, summaryOptions) =>
      applyBallotElectionOrdering(pool, await lookupBallotSummariesByDistrictIds(pool, districtIds), summaryOptions),
    lookupAuthenticatedBallotSummaries: async (userId, summaryOptions) => {
      const districtIds = await listUserDistrictIds(pool, userId);
      return applyBallotElectionOrdering(pool, await lookupBallotSummariesByDistrictIds(pool, districtIds), {
        ...summaryOptions,
        userId,
      });
    },
    lookupAuthenticatedUserEmailVerified,
    lookupCandidateDetail: (candidateId, userId) => lookupCandidateDetailById(pool, { candidateId, userId }),
    searchCandidates: (searchQuery) => searchCandidatesByName(pool, searchQuery),
    lookupElectionDetail: (electionId) => lookupElectionDetailById(pool, electionId),
    lookupCandidateElectionFinance: (electionId, candidateId) =>
      lookupCandidateElectionFinanceSummaryById(pool, electionId, candidateId),
    listResearchAreas: () => listSelectableResearchAreas(pool),
    listAuthenticatedCandidateFollows: (userId) => listUserCandidateFollows(pool, userId),
    setAuthenticatedCandidateFollow: (userId, input) => setUserCandidateFollow(pool, userId, input),
    // [ballot-personalized-ordering]
    getAuthenticatedBallotPreferences: (userId) => getUserBallotPreferences(pool, userId),
    setAuthenticatedBallotPreferences: (userId, preferences) => setUserBallotPreferences(pool, userId, preferences),
    getAuthenticatedUser: (userId) => getUserIdentity(pool, userId),
    updateAuthenticatedUserFirstName: (userId, firstName) => setUserFirstName(pool, userId, firstName),
    getAuthenticatedEmailPreferences: (userId) => getUserEmailPreferences(pool, userId),
    setAuthenticatedEmailPreferences: (userId, preferences) => setUserEmailPreferences(pool, userId, preferences),
    registerAuthenticatedPushToken: (userId, input) => registerUserPushToken(pool, userId, input),
    revokeAuthenticatedPushToken: (userId, expoPushToken) => revokeUserPushToken(pool, userId, expoPushToken),
    ...(unsubscribeSecret
      ? {
          unsubscribeFromEmailNotifications: async (
            token: string,
            mode: "confirm" | "execute",
            preference: "digest" | "new_election_alerts" | "election_reminders" | "issue_updates"
          ) => {
            const userId = verifyEmailUnsubscribeToken(token, unsubscribeSecret);
            if (!userId) {
              return "invalid_token" as const;
            }
            if (mode === "confirm") {
              return "ok" as const;
            }
            try {
              if (preference === "new_election_alerts") {
                await disableUserEmailNewElectionAlerts(pool, userId);
              } else if (preference === "election_reminders") {
                await disableUserEmailElectionReminders(pool, userId);
              } else if (preference === "issue_updates") {
                await disableUserEmailIssueUpdates(pool, userId);
              } else {
                await disableUserEmailDigest(pool, userId);
              }
            } catch (error) {
              // A valid token for a since-deleted account still reports
              // success so the page does not leak account state.
              if (!(error instanceof UserEmailPreferencesError && error.code === "user_not_found")) {
                throw error;
              }
            }
            return "ok" as const;
          },
        }
      : {}),
    listAuthenticatedResearchAreaPreferences: (userId) => listUserResearchAreaPreferences(pool, userId),
    replaceAuthenticatedResearchAreaPreferences: (userId, preferences) =>
      replaceUserResearchAreaPreferences(pool, userId, preferences),
    updateAuthenticatedAddressDistricts: (userId, address) =>
      updateAuthenticatedAddressDistricts(
        {
          // Unlike the anonymous resolve handler, this path has no
          // logDiagnostics hook, and the partial-resolution 503 it can turn
          // into is an expected error that never reaches Sentry — so log the
          // missing keys and warnings here or operators would have nothing
          // to repair the districts data from.
          resolveAddressToDistricts: async (inputAddress) => {
            const result = await resolveAddressWithAutoResearch(inputAddress, "me_address_update");
            try {
              logAddressResolutionDiagnostics(toAddressResolutionDiagnostics(result), "me_address_update");
            } catch {
              // Diagnostics are best-effort; never fail the update for them.
            }
            return result;
          },
          replaceUserDistricts: (inputUserId, districtIds) => replaceUserDistricts(pool, inputUserId, districtIds),
          lookupBallotSummariesByDistrictIds: (districtIds) =>
            lookupBallotSummariesByDistrictIds(pool, districtIds),
        },
        userId,
        address
      ),
    initializeUserDistricts: ({ userId, districtIds }) => initializeUserDistricts(pool, userId, districtIds),
    resolveAddress: (address) => resolveAddressWithAutoResearch(address),
  });

  let server: Server | null = null;
  await new Promise<void>((resolve, reject) => {
    const startedServer = app.listen(port, host, () => {
      startedServer.off("error", reject);
      resolve();
    });
    startedServer.once("error", reject);
    server = startedServer;
  });

  console.log(
    `address API server listening on http://${host}:${port} allowed_origins=${
      allowedOrigins.length > 0 ? allowedOrigins.join(",") : "none"
    }`
  );

  const shutdown = async (): Promise<void> => {
    await new Promise<void>((resolve, reject) => {
      if (!server) {
        resolve();
        return;
      }
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
    await pool.end();
    if (redis?.isOpen) {
      await redis.quit();
    }
  };

  const handleShutdown = (signal: NodeJS.Signals): void => {
    void shutdown()
      .then(() => {
        console.log(`address API server stopped after ${signal}`);
        process.exit(0);
      })
      .catch((error) => {
        console.error("address API server shutdown failed:", error);
        process.exit(1);
      });
  };

  process.on("SIGINT", handleShutdown);
  process.on("SIGTERM", handleShutdown);

  // Replace Node's default crash handlers with labeled equivalents so the
  // process manager's log shows WHY the server died. Same exit-and-restart
  // semantics as the defaults — state after an uncaught error is unknown,
  // so limping on is worse than restarting.
  // describeError: stack string only with emails/query strings masked —
  // crash logs get the same scrubbing as the middleware and Sentry events.
  const crash = (label: string, cause: unknown): void => {
    console.error(`address API server ${label}:`, describeError(cause));
    captureError(cause, { crash: label.replaceAll(" ", "_") });
    void flushSentry().finally(() => {
      process.exit(1);
    });
  };
  process.on("unhandledRejection", (reason) => {
    crash("unhandled rejection", reason);
  });
  process.on("uncaughtException", (error) => {
    crash("uncaught exception", error);
  });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("address API server failed:", message);
  process.exitCode = 1;
});

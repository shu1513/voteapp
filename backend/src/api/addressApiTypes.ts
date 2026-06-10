import type { BallotLookupElection, BallotSummaryResult } from "../pipeline/address/ballotLookup.js";
import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";
import type { AddressApiClientIpInput } from "./addressApiClientIp.js";
import type { AddressResolutionDiagnostics } from "./addressApiResponses.js";

export type AddressApiRateLimitInput = {
  clientIp: string;
  method: string;
  pathname: string;
};

export type AddressApiRateLimitResult = {
  allowed: boolean;
  retryAfterSeconds?: number;
};

export type AddressApiServerOptions = {
  resolveAddress: (address: string) => Promise<AddressResolutionResult>;
  lookupBallotSummaries?: (districtIds: readonly string[]) => Promise<BallotSummaryResult>;
  lookupElectionDetail?: (electionId: string) => Promise<BallotLookupElection | null>;
  allowedOrigins?: readonly string[];
  logDiagnostics?: (diagnostics: AddressResolutionDiagnostics) => void;
  rateLimit?: (input: AddressApiRateLimitInput) => AddressApiRateLimitResult;
  resolveClientIp?: (input: AddressApiClientIpInput) => string;
};

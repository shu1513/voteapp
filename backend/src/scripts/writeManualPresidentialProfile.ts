import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import {
  validateCandidateProfileAiPayload,
  type CandidateProfilePayloadValidationResult,
} from "../ai/enrichCandidateProfile.js";
import { loadProjectEnv } from "../config/env.js";
import type { CandidateProfilePayload } from "../contracts/candidateProfilePayloadContract.js";
import { enqueueCandidateRecordDrafts } from "../pipeline/candidates/candidateRecordDraftEmitter.js";
import {
  findOrCreateCandidateFromProfile,
  hasAtLeastOneHardIdentifier,
} from "../pipeline/candidates/candidateProfileIdentity.js";
import {
  findPresidentialCycleCandidateIdByFecId,
  markPresidentialCycleCandidateProfileResearched,
  markPresidentialCycleCandidateRunningMateProfileResearched,
  setPresidentialCycleCandidateRunningMate,
  upsertPresidentialCycleCandidate,
} from "../pipeline/candidates/candidateProfileLinks.js";
import {
  loadPresidentialCycleProfileContext,
  type PresidentialCycleProfileContext,
} from "../pipeline/presidential/presidentialProfileContext.js";
import {
  applyConfirmedGaps,
  buildCandidateProfileQualityGaps,
} from "./writeManualCandidateProfile.js";
import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

type RedisClient = ReturnType<typeof createClient>;

export type ManualPresidentialProfileRole = "president" | "vice_president";

export type ManualPresidentialProfileScriptOptions = {
  presidentialCycleId: string;
  presidentialRole: ManualPresidentialProfileRole;
  parentPresidentialCandidateFecId: string | null;
  file: string;
  dryRun: boolean;
  runId: string;
  emitRecordDraft: boolean;
  strictQualityGate: boolean;
  allowNoHardIdentifier: boolean;
  confirmedGapIds: Set<string>;
  repairReportFile: string | null;
};

export type ManualPresidentialProfileWriteResult =
  | {
      ok: true;
      dryRun: boolean;
      manualKey: string;
      runId: string;
      presidentialCycleId: string;
      presidentialRole: ManualPresidentialProfileRole;
      displayName: string;
      hasHardIdentifier: boolean;
      candidateId?: string;
      matchedExisting?: boolean;
      presidentialCycleCandidateLinked?: boolean;
      runningMateLinked?: boolean;
      parentCandidateId?: string;
      parentPresidentialCandidateFecId?: string;
      recordDraft?: { emittedCount: number; skippedCount: number } | null;
      qualityGate: {
        strict: boolean;
        confirmedGaps: string[];
        gaps: ManualResearchRepairGap[];
      };
    }
  | {
      ok: false;
      manualKey: string;
      presidentialCycleId: string;
      presidentialRole: ManualPresidentialProfileRole;
      error: string;
      errorCode: "SCHEMA_MISMATCH" | "QUALITY_GATE" | "MISSING_HARD_IDENTIFIER";
      gaps: ManualResearchRepairGap[];
    };

type ManualPresidentialProfileWriteDeps = {
  validateProfile?: (
    payload: unknown,
    timeoutMs: number
  ) => Promise<CandidateProfilePayloadValidationResult>;
  findOrCreateCandidate?: typeof findOrCreateCandidateFromProfile;
  upsertCycleCandidate?: typeof upsertPresidentialCycleCandidate;
  markCycleCandidateProfileResearched?: typeof markPresidentialCycleCandidateProfileResearched;
  findParentCandidateByFecId?: typeof findPresidentialCycleCandidateIdByFecId;
  setRunningMate?: typeof setPresidentialCycleCandidateRunningMate;
  markRunningMateProfileResearched?: typeof markPresidentialCycleCandidateRunningMateProfileResearched;
  enqueueRecordDrafts?: typeof enqueueCandidateRecordDrafts;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:presidential-profile:write -- --presidential-cycle-id uuid --presidential-role president|vice_president --file profile.json [--parent-presidential-candidate-fec-id P########] [--emit-record-draft] [--allow-no-hard-identifier] [--strict-quality-gate] [--confirmed-gap id] [--repair-report-file file] [--dry-run]",
    "",
    "Payload must match CandidateProfilePayload. Live runs find/create a candidate and link it to the presidential cycle.",
  ].join("\n");
}

function readValueFlag(args: readonly string[], name: string): string | null {
  const prefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(prefix));
  if (inline) {
    const value = inline.slice(prefix.length);
    if (value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const value = args[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }

  return null;
}

function readBooleanFlag(args: readonly string[], name: string): boolean {
  const inline = args.find((arg) => arg.startsWith(`${name}=`));
  if (inline) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  const index = args.indexOf(name);
  if (index < 0) {
    return false;
  }
  const next = args[index + 1];
  if (next && !next.startsWith("--")) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  return true;
}

function readRepeatedFlag(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === name) {
      const value = args[index + 1];
      if (!value || value.startsWith("--") || value.trim().length === 0) {
        throw new Error(`Missing value for ${name}.\n${usage()}`);
      }
      values.push(value.trim());
      index += 1;
      continue;
    }
    if (token?.startsWith(prefix)) {
      const value = token.slice(prefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing value for ${name}.\n${usage()}`);
      }
      values.push(value);
    }
  }
  return values;
}

function normalizeRequiredFlag(raw: string | null, name: string): string {
  const value = raw?.trim() ?? "";
  if (value.length === 0) {
    throw new Error(`Missing ${name}.\n${usage()}`);
  }
  return value;
}

function parsePresidentialRole(raw: string | null): ManualPresidentialProfileRole {
  const value = normalizeRequiredFlag(raw, "--presidential-role");
  if (value === "president" || value === "vice_president") {
    return value;
  }
  throw new Error(`Invalid --presidential-role value: ${value}.\n${usage()}`);
}

function normalizeOptionalPresidentialFecId(raw: string | null): string | null {
  const value = raw?.trim().toUpperCase() ?? "";
  if (value.length === 0) {
    return null;
  }
  if (!/^P\d{8}$/.test(value)) {
    throw new Error(`Invalid --parent-presidential-candidate-fec-id value: ${raw}`);
  }
  return value;
}

function normalizeConfirmedGaps(values: readonly string[]): Set<string> {
  return new Set(values.map((value) => value.trim()).filter((value) => value.length > 0));
}

function buildRunId(input: { presidentialCycleId: string; role: ManualPresidentialProfileRole; now: Date }): string {
  return `manual_presidential_profile:${input.presidentialCycleId}:${input.role}:${input.now.toISOString()}`;
}

export function parseManualPresidentialProfileScriptArgs(
  args: readonly string[],
  now = new Date()
): ManualPresidentialProfileScriptOptions {
  const presidentialCycleId = normalizeRequiredFlag(
    readValueFlag(args, "--presidential-cycle-id"),
    "--presidential-cycle-id"
  );
  const presidentialRole = parsePresidentialRole(readValueFlag(args, "--presidential-role"));
  const parentPresidentialCandidateFecId = normalizeOptionalPresidentialFecId(
    readValueFlag(args, "--parent-presidential-candidate-fec-id")
  );
  if (presidentialRole === "vice_president" && !parentPresidentialCandidateFecId) {
    throw new Error("--parent-presidential-candidate-fec-id is required for vice_president profiles");
  }
  if (presidentialRole === "president" && parentPresidentialCandidateFecId) {
    throw new Error("--parent-presidential-candidate-fec-id is only valid for vice_president profiles");
  }

  const file = normalizeRequiredFlag(readValueFlag(args, "--file"), "--file");
  const runId =
    readValueFlag(args, "--run-id")?.trim() ||
    buildRunId({ presidentialCycleId, role: presidentialRole, now });

  return {
    presidentialCycleId,
    presidentialRole,
    parentPresidentialCandidateFecId,
    file,
    dryRun: readBooleanFlag(args, "--dry-run"),
    runId,
    emitRecordDraft: readBooleanFlag(args, "--emit-record-draft"),
    strictQualityGate: readBooleanFlag(args, "--strict-quality-gate"),
    allowNoHardIdentifier: readBooleanFlag(args, "--allow-no-hard-identifier"),
    confirmedGapIds: normalizeConfirmedGaps(readRepeatedFlag(args, "--confirmed-gap")),
    repairReportFile: readValueFlag(args, "--repair-report-file"),
  };
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual presidential profile write`);
  }
  return value;
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim() || String(fallback);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  return Number(raw);
}

async function connectRedis(redisUrl: string): Promise<RedisClient> {
  const redis = createClient({ url: redisUrl });
  redis.on("error", (error) => {
    console.warn("manual presidential profile redis client error:", error);
  });
  await redis.connect();
  return redis;
}

function profileSlug(profile: CandidateProfilePayload | null): string {
  return (profile?.display_name ?? "payload")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function manualKey(input: {
  presidentialCycleId: string;
  presidentialRole: ManualPresidentialProfileRole;
  profile: CandidateProfilePayload | null;
}): string {
  return `manual:presidential-profile:${input.presidentialCycleId}:${input.presidentialRole}:${profileSlug(input.profile)}`;
}

function hasPresidentialFecId(profile: CandidateProfilePayload): boolean {
  return (profile.fec_ids ?? []).some((fecId) => /^P\d{8}$/.test(fecId.trim().toUpperCase()));
}

function profileHasFecId(profile: CandidateProfilePayload, fecCandidateId: string): boolean {
  const normalized = fecCandidateId.trim().toUpperCase();
  return (profile.fec_ids ?? []).some((fecId) => fecId.trim().toUpperCase() === normalized);
}

function effectiveParty(input: {
  profile: CandidateProfilePayload;
  context: PresidentialCycleProfileContext;
}): string | null {
  const cycleParty = input.context.party?.trim();
  if (input.context.stage === "primary" && cycleParty) {
    return cycleParty;
  }
  const profileParty = input.profile.party?.trim();
  if (profileParty) {
    return profileParty;
  }
  return cycleParty && cycleParty.length > 0 ? cycleParty : null;
}

async function preflightDryRunProfileLink(input: {
  pool: Pool;
  context: PresidentialCycleProfileContext;
  profile: CandidateProfilePayload;
  options: ManualPresidentialProfileScriptOptions;
  deps: Required<Pick<ManualPresidentialProfileWriteDeps, "findParentCandidateByFecId">>;
}): Promise<{
  presidentialCycleCandidateLinked: boolean;
  runningMateLinked: boolean;
  parentCandidateId?: string;
}> {
  if (input.options.presidentialRole === "president") {
    const party = effectiveParty({ profile: input.profile, context: input.context });
    if (!party) {
      throw new Error("Presidential candidate party is required to link candidate to cycle");
    }
    return {
      presidentialCycleCandidateLinked: true,
      runningMateLinked: false,
    };
  }

  const parentFecId = input.options.parentPresidentialCandidateFecId;
  if (!parentFecId) {
    throw new Error("parent presidential candidate FEC ID is required for vice president profile write");
  }
  if (profileHasFecId(input.profile, parentFecId)) {
    throw new Error(`Vice president profile carries the parent presidential candidate FEC ID ${parentFecId}`);
  }
  const parentCandidateId = await input.deps.findParentCandidateByFecId({
    db: input.pool,
    cycleId: input.context.cycleId,
    fecCandidateId: parentFecId,
  });
  if (!parentCandidateId) {
    throw new Error(`Parent presidential cycle candidate not found for FEC ID ${parentFecId}`);
  }
  return {
    presidentialCycleCandidateLinked: false,
    runningMateLinked: true,
    parentCandidateId,
  };
}

function buildProfileValidationGaps(input: {
  reason: string;
  failedCitationUrls?: readonly string[];
}): ManualResearchRepairGap[] {
  const sourceUrls = [...new Set(input.failedCitationUrls ?? [])];
  if (sourceUrls.length > 0) {
    return sourceUrls.map((sourceUrl, index) => ({
      id: `candidate_profile.sources.${index + 1}`,
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      field: "sources",
      sourceUrl,
      failureKind: "source_url",
      reason: input.reason,
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass:
        "Run a focused candidate-profile source repair pass. Replace unreachable or bad profile source URLs with reliable public URLs, then rerun the manual presidential profile writer.",
    }));
  }
  return [
    {
      id: "candidate_profile.payload",
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      failureKind: "schema",
      reason: input.reason,
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass:
        "Run a focused candidate-profile payload repair pass. Fix only the schema issue, then rerun the manual presidential profile writer.",
    },
  ];
}

function buildMissingHardIdentifierGap(reason: string): ManualResearchRepairGap[] {
  return [
    {
      id: "candidate_profile.hard_identifier",
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      field: "hard_identifier",
      failureKind: "quality_gap",
      reason,
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass:
        "Run a focused identifier-only profile pass. Add a source-backed FEC ID, official website, date of birth, social/professional URL, or deliberately rerun with --allow-no-hard-identifier only when identity risk is acceptable.",
    },
  ];
}

async function writeProfileRepairReport(input: {
  reportFile: string | null;
  manualKey: string;
  options: ManualPresidentialProfileScriptOptions;
  displayName?: string | null;
  gaps: ManualResearchRepairGap[];
}): Promise<void> {
  await writeManualResearchRepairReport(
    input.reportFile,
    buildManualResearchRepairReport({
      command: "manual:presidential-profile:write",
      manualKey: input.manualKey,
      target: {
        presidentialCycleId: input.options.presidentialCycleId,
        presidentialRole: input.options.presidentialRole,
        parentPresidentialCandidateFecId: input.options.parentPresidentialCandidateFecId,
        file: input.options.file,
        displayName: input.displayName ?? null,
      },
      gaps: input.gaps,
    })
  );
}

async function writePresidentProfile(input: {
  client: PoolClient;
  context: PresidentialCycleProfileContext;
  profile: CandidateProfilePayload;
  candidateId: string;
  deps: Required<Pick<
    ManualPresidentialProfileWriteDeps,
    "upsertCycleCandidate" | "markCycleCandidateProfileResearched"
  >>;
}): Promise<void> {
  const party = effectiveParty({ profile: input.profile, context: input.context });
  if (!party) {
    throw new Error("Presidential candidate party is required to link candidate to cycle");
  }
  await input.deps.upsertCycleCandidate({
    client: input.client,
    cycleId: input.context.cycleId,
    candidateId: input.candidateId,
    party,
    sources: input.profile.sources,
  });
  await input.deps.markCycleCandidateProfileResearched({
    db: input.client,
    cycleId: input.context.cycleId,
    candidateId: input.candidateId,
  });
}

async function writeVicePresidentProfile(input: {
  client: PoolClient;
  context: PresidentialCycleProfileContext;
  options: ManualPresidentialProfileScriptOptions;
  candidateId: string;
  deps: Required<Pick<
    ManualPresidentialProfileWriteDeps,
    "findParentCandidateByFecId" | "setRunningMate" | "markRunningMateProfileResearched"
  >>;
}): Promise<string> {
  const parentFecId = input.options.parentPresidentialCandidateFecId;
  if (!parentFecId) {
    throw new Error("parent presidential candidate FEC ID is required for vice president profile write");
  }
  const parentCandidateId = await input.deps.findParentCandidateByFecId({
    db: input.client,
    cycleId: input.context.cycleId,
    fecCandidateId: parentFecId,
  });
  if (!parentCandidateId) {
    throw new Error(`Parent presidential cycle candidate not found for FEC ID ${parentFecId}`);
  }
  if (parentCandidateId === input.candidateId) {
    throw new Error(`Vice president profile resolved to the parent presidential candidate for FEC ID ${parentFecId}`);
  }
  await input.deps.setRunningMate({
    db: input.client,
    cycleId: input.context.cycleId,
    candidateId: parentCandidateId,
    runningMateCandidateId: input.candidateId,
  });
  await input.deps.markRunningMateProfileResearched({
    db: input.client,
    cycleId: input.context.cycleId,
    candidateId: parentCandidateId,
    runningMateCandidateId: input.candidateId,
  });
  return parentCandidateId;
}

export async function runManualPresidentialProfileWrite(input: {
  options: ManualPresidentialProfileScriptOptions;
  rawPayload: unknown;
  pool: Pool;
  redis?: RedisSendCommandClient | null;
  deps?: ManualPresidentialProfileWriteDeps;
}): Promise<ManualPresidentialProfileWriteResult> {
  const deps = {
    validateProfile: input.deps?.validateProfile ?? validateCandidateProfileAiPayload,
    findOrCreateCandidate: input.deps?.findOrCreateCandidate ?? findOrCreateCandidateFromProfile,
    upsertCycleCandidate: input.deps?.upsertCycleCandidate ?? upsertPresidentialCycleCandidate,
    markCycleCandidateProfileResearched:
      input.deps?.markCycleCandidateProfileResearched ?? markPresidentialCycleCandidateProfileResearched,
    findParentCandidateByFecId: input.deps?.findParentCandidateByFecId ?? findPresidentialCycleCandidateIdByFecId,
    setRunningMate: input.deps?.setRunningMate ?? setPresidentialCycleCandidateRunningMate,
    markRunningMateProfileResearched:
      input.deps?.markRunningMateProfileResearched ?? markPresidentialCycleCandidateRunningMateProfileResearched,
    enqueueRecordDrafts: input.deps?.enqueueRecordDrafts ?? enqueueCandidateRecordDrafts,
  };

  const fallbackManualKey = manualKey({
    presidentialCycleId: input.options.presidentialCycleId,
    presidentialRole: input.options.presidentialRole,
    profile: null,
  });
  const validatedProfile = await deps.validateProfile(input.rawPayload, readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000));
  if (!validatedProfile.ok) {
    const gaps = buildProfileValidationGaps({
      reason: validatedProfile.reason,
      failedCitationUrls: validatedProfile.failedCitationUrls,
    });
    await writeProfileRepairReport({
      reportFile: input.options.repairReportFile,
      manualKey: fallbackManualKey,
      options: input.options,
      displayName: null,
      gaps,
    });
    return {
      ok: false,
      manualKey: fallbackManualKey,
      presidentialCycleId: input.options.presidentialCycleId,
      presidentialRole: input.options.presidentialRole,
      error: `Candidate profile payload failed validation: ${validatedProfile.reason}`,
      errorCode: "SCHEMA_MISMATCH",
      gaps,
    };
  }

  const profile = validatedProfile.profile;
  const resolvedManualKey = manualKey({
    presidentialCycleId: input.options.presidentialCycleId,
    presidentialRole: input.options.presidentialRole,
    profile,
  });
  if (!input.options.allowNoHardIdentifier && !hasAtLeastOneHardIdentifier(profile)) {
    const gaps = buildMissingHardIdentifierGap("Candidate profile has no hard identifier.");
    await writeProfileRepairReport({
      reportFile: input.options.repairReportFile,
      manualKey: resolvedManualKey,
      options: input.options,
      displayName: profile.display_name,
      gaps,
    });
    return {
      ok: false,
      manualKey: resolvedManualKey,
      presidentialCycleId: input.options.presidentialCycleId,
      presidentialRole: input.options.presidentialRole,
      error:
        "Candidate profile has no hard identifier. Add official_website_url, FEC ID, DOB, Twitter, LinkedIn, or pass --allow-no-hard-identifier deliberately.",
      errorCode: "MISSING_HARD_IDENTIFIER",
      gaps,
    };
  }
  if (input.options.presidentialRole === "president" && !hasPresidentialFecId(profile)) {
    const gaps = buildMissingHardIdentifierGap("President profile must include at least one presidential FEC ID.");
    await writeProfileRepairReport({
      reportFile: input.options.repairReportFile,
      manualKey: resolvedManualKey,
      options: input.options,
      displayName: profile.display_name,
      gaps,
    });
    return {
      ok: false,
      manualKey: resolvedManualKey,
      presidentialCycleId: input.options.presidentialCycleId,
      presidentialRole: input.options.presidentialRole,
      error: "President profile must include at least one presidential FEC ID.",
      errorCode: "MISSING_HARD_IDENTIFIER",
      gaps,
    };
  }

  const context = await loadPresidentialCycleProfileContext(input.pool, input.options.presidentialCycleId);
  if (!context) {
    throw new Error(`Presidential cycle not found for presidential_cycle_id=${input.options.presidentialCycleId}`);
  }
  const qualityGaps = applyConfirmedGaps(
    buildCandidateProfileQualityGaps({ profile, includeParty: true }),
    input.options.confirmedGapIds
  );
  const blockingQualityGaps = qualityGaps.filter((gap) => gap.outcome === "needs_repair");
  if (input.options.repairReportFile && qualityGaps.length > 0) {
    await writeProfileRepairReport({
      reportFile: input.options.repairReportFile,
      manualKey: resolvedManualKey,
      options: input.options,
      displayName: profile.display_name,
      gaps: qualityGaps,
    });
  }
  if (input.options.strictQualityGate && blockingQualityGaps.length > 0) {
    return {
      ok: false,
      manualKey: resolvedManualKey,
      presidentialCycleId: input.options.presidentialCycleId,
      presidentialRole: input.options.presidentialRole,
      error: `Candidate profile quality gate failed; run focused gap-repair pass before import. gaps=${blockingQualityGaps.length}; ${summarizeManualResearchGaps(blockingQualityGaps)}`,
      errorCode: "QUALITY_GATE",
      gaps: blockingQualityGaps,
    };
  }

  if (input.options.dryRun) {
    const linkPreflight = await preflightDryRunProfileLink({
      pool: input.pool,
      context,
      profile,
      options: input.options,
      deps,
    });
    return {
      ok: true,
      dryRun: true,
      manualKey: resolvedManualKey,
      runId: input.options.runId,
      presidentialCycleId: context.cycleId,
      presidentialRole: input.options.presidentialRole,
      displayName: profile.display_name,
      hasHardIdentifier: hasAtLeastOneHardIdentifier(profile),
      presidentialCycleCandidateLinked: linkPreflight.presidentialCycleCandidateLinked,
      runningMateLinked: linkPreflight.runningMateLinked,
      parentCandidateId: linkPreflight.parentCandidateId,
      parentPresidentialCandidateFecId: input.options.parentPresidentialCandidateFecId ?? undefined,
      recordDraft: null,
      qualityGate: {
        strict: input.options.strictQualityGate,
        confirmedGaps: [...input.options.confirmedGapIds].sort(),
        gaps: qualityGaps,
      },
    };
  }

  const client = await input.pool.connect();
  let candidateId: string;
  let matchedExisting: boolean;
  let parentCandidateId: string | undefined;
  try {
    await client.query("BEGIN");
    const candidateResult = await deps.findOrCreateCandidate({
      client,
      profile,
      state: context.state,
      rosterParty: effectiveParty({ profile, context }) ?? undefined,
      includeParty: true,
      allowCrossStateHardIdentifierMatch: true,
    });
    candidateId = candidateResult.candidateId;
    matchedExisting = candidateResult.matchedExisting;

    if (input.options.presidentialRole === "vice_president") {
      parentCandidateId = await writeVicePresidentProfile({
        client,
        context,
        options: input.options,
        candidateId,
        deps,
      });
    } else {
      await writePresidentProfile({
        client,
        context,
        profile,
        candidateId,
        deps,
      });
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }

  let recordDraft: { emittedCount: number; skippedCount: number } | null = null;
  if (input.options.emitRecordDraft) {
    if (!input.redis) {
      throw new Error(
        `Candidate profile DB write committed for candidate_id=${candidateId!} presidential_cycle_id=${context.cycleId}, but record-draft Redis emit was requested without a Redis client. Recovery: re-run this command with --emit-record-draft after Redis is available; candidate/cycle writes are idempotent.`
      );
    }
    try {
      recordDraft = await deps.enqueueRecordDrafts(input.redis, [
        {
          contextType: "presidential_cycle",
          candidateId: candidateId!,
          presidentialCycleId: context.cycleId,
          presidentialRole: input.options.presidentialRole,
          runId: input.options.runId,
        },
      ]);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Candidate profile DB write committed for candidate_id=${candidateId!} presidential_cycle_id=${context.cycleId}, but record-draft Redis emit failed: ${reason}. Recovery: re-run this command with --emit-record-draft after Redis is available; candidate/cycle writes are idempotent.`
      );
    }
  }

  return {
    ok: true,
    dryRun: false,
    manualKey: resolvedManualKey,
    runId: input.options.runId,
    presidentialCycleId: context.cycleId,
    presidentialRole: input.options.presidentialRole,
    displayName: profile.display_name,
    hasHardIdentifier: hasAtLeastOneHardIdentifier(profile),
    candidateId: candidateId!,
    matchedExisting: matchedExisting!,
    presidentialCycleCandidateLinked: input.options.presidentialRole === "president",
    runningMateLinked: input.options.presidentialRole === "vice_president",
    parentCandidateId,
    recordDraft,
    qualityGate: {
      strict: input.options.strictQualityGate,
      confirmedGaps: [...input.options.confirmedGapIds].sort(),
      gaps: qualityGaps,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseManualPresidentialProfileScriptArgs(process.argv.slice(2));
  const rawPayload = await readJsonFile(options.file);
  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  let redis: RedisClient | null = null;

  try {
    if (options.emitRecordDraft && !options.dryRun) {
      redis = await connectRedis(requireEnv("REDIS_URL"));
    }
    const result = await runManualPresidentialProfileWrite({
      options,
      rawPayload,
      pool,
      redis,
    });
    console.log(JSON.stringify(result, null, 2));
    if (!result.ok) {
      process.exitCode = 1;
    }
  } finally {
    if (redis) {
      await redis.quit().catch(() => undefined);
    }
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual presidential profile write failed:", message);
    process.exitCode = 1;
  });
}

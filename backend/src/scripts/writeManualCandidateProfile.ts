import { readFile } from "node:fs/promises";
import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import { resolveIncludePartyForCandidateContest } from "../ai/candidatePartisanship.js";
import { validateCandidateProfileAiPayload } from "../ai/enrichCandidateProfile.js";
import { loadProjectEnv } from "../config/env.js";
import type { CandidateProfilePayload } from "../contracts/candidateProfilePayloadContract.js";
import { enqueueCandidateRecordDrafts } from "../pipeline/candidates/candidateRecordDraftEmitter.js";
import {
  enqueueCandidateProfileFinanceSyncFanoutForLinkedElection,
  type CandidateProfileFinanceSyncFanoutResult,
  type CandidateProfileLinkedElectionContext,
} from "../pipeline/enrichers/candidateProfileEnricher.js";
import {
  findOrCreateCandidateFromProfile,
  hasAtLeastOneHardIdentifier,
} from "../pipeline/candidates/candidateProfileIdentity.js";
import { upsertCandidateElection } from "../pipeline/candidates/candidateProfileLinks.js";
import { createCandidateFutureElectionNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";

type ElectionContextRow = {
  election_id: string;
  state: string;
  district_name: string;
  district_type: string;
  election_date: string;
  official_ballot_title: string;
  election_stage: string | null;
  senate_class: string | null;
  term_end_year: string | null;
  is_partisan: boolean | null;
  race_type: string;
  office_scope: string | null;
  office_canonical_name: string | null;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-profile:write -- --election-id uuid --file profile.json [--run-id id] [--is-incumbent true|false] [--emit-record-draft] [--emit-finance-sync] [--allow-no-hard-identifier] [--strict-quality-gate] [--confirmed-gap id] [--repair-report-file file] [--dry-run]",
    "",
    "Payload must match CandidateProfilePayload. Live runs find/create a candidate and link it to the election.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    const value = process.argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  const prefix = `${name}=`;
  const match = process.argv.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function readBooleanFlag(name: string): boolean | undefined {
  const value = readFlag(name);
  if (value === null) {
    return undefined;
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  throw new Error(`${name} must be true or false.\n${usage()}`);
}

function readRepeatedFlag(name: string): string[] {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < process.argv.length; index += 1) {
    const token = process.argv[index];
    if (token === name) {
      const value = process.argv[index + 1];
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

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

async function loadElectionContext(pool: Pool, electionId: string): Promise<ElectionContextRow | null> {
  const result = await pool.query<ElectionContextRow>(
    `
      SELECT
        e.id::text AS election_id,
        d.state,
        d.name AS district_name,
        d.district_type,
        e.election_date::text AS election_date,
        e.official_ballot_title,
        e.election_stage::text AS election_stage,
        sm.senate_class,
        sm.term_end_year,
        e.is_partisan,
        e.race_type,
        office.scope AS office_scope,
        office.canonical_name AS office_canonical_name
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.offices AS office
        ON office.id = e.office_id
      LEFT JOIN public.election_senate_metadata AS sm
        ON sm.election_id = e.id
      WHERE e.id::text = $1
      LIMIT 1
    `,
    [electionId]
  );
  return result.rows[0] ?? null;
}

async function loadExistingCandidateElectionIncumbency(
  client: Pick<PoolClient, "query">,
  input: { candidateId: string; electionId: string }
): Promise<boolean | null> {
  const result = await client.query<{ is_incumbent: boolean }>(
    `
      SELECT is_incumbent
      FROM public.candidate_elections
      WHERE candidate_id = $1
        AND election_id = $2
      LIMIT 1
    `,
    [input.candidateId, input.electionId]
  );
  return result.rows[0]?.is_incumbent ?? null;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual candidate profile write`);
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

function normalizeConfirmedGaps(values: readonly string[]): Set<string> {
  return new Set(values.map((value) => value.trim()).filter((value) => value.length > 0));
}

function buildLinkedElectionFinanceContext(input: {
  election: ElectionContextRow;
  includeParty: boolean;
  profileParty: string | null | undefined;
  isIncumbent: boolean | undefined;
}): CandidateProfileLinkedElectionContext {
  return {
    type: "election",
    contextId: input.election.election_id,
    state: input.election.state,
    districtName: input.election.district_name,
    districtType: input.election.district_type,
    electionDate: input.election.election_date,
    officialBallotTitle: input.election.official_ballot_title,
    electionStage: input.election.election_stage,
    senateClass: input.election.senate_class,
    termEndYear: input.election.term_end_year,
    electionIsPartisan: input.election.is_partisan,
    officeScope: input.election.office_scope,
    officeCanonicalName: input.election.office_canonical_name,
    includeParty: input.includeParty,
    rosterParty: input.includeParty ? input.profileParty ?? undefined : undefined,
    rosterIncumbent: input.isIncumbent,
    seedUrls: [],
  };
}

function applyConfirmedGaps(
  gaps: readonly ManualResearchRepairGap[],
  confirmedGapIds: ReadonlySet<string>
): ManualResearchRepairGap[] {
  return gaps.map((gap) =>
    gap.outcome === "needs_repair" && confirmedGapIds.has(gap.id)
      ? {
          ...gap,
          outcome: "confirmed_null",
          reason: `${gap.reason} Operator marked this gap confirmed_null after focused repair research.`,
        }
      : gap
  );
}

function buildProfileValidationGap(input: {
  reason: string;
  failedCitationUrls?: readonly string[];
}): ManualResearchRepairGap {
  const sourceUrl = input.failedCitationUrls?.[0];
  return {
    id: sourceUrl ? "candidate_profile.sources" : "candidate_profile.payload",
    stage: "candidate_profile",
    objectType: "candidate_profile",
    outcome: "needs_repair",
    field: sourceUrl ? "sources" : undefined,
    sourceUrl,
    failureKind: sourceUrl ? "source_url" : "schema",
    reason: input.reason,
    promptFile: "src/ai/providers/candidateProfilePrompt.ts",
    focusedResearchPass: sourceUrl
      ? "Run a focused candidate-profile source repair pass. Replace unreachable or bad profile source URLs with reliable public URLs, then rerun the manual profile writer."
      : "Run a focused candidate-profile payload repair pass. Fix only the schema issue, then rerun the manual profile writer.",
  };
}

function buildCandidateProfileQualityGaps(input: {
  profile: CandidateProfilePayload;
  includeParty: boolean;
}): ManualResearchRepairGap[] {
  const gaps: ManualResearchRepairGap[] = [];
  if (!input.profile.summary) {
    gaps.push({
      id: "candidate_profile.summary",
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      field: "summary",
      failureKind: "quality_gap",
      reason: "Candidate profile summary is missing.",
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass: "Run a focused summary-only profile pass for this candidate. Write a short neutral source-backed summary, or mark candidate_profile.summary confirmed_null if no reliable summary can be found.",
    });
  }
  if (!input.profile.official_website_url) {
    gaps.push({
      id: "candidate_profile.official_website_url",
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      field: "official_website_url",
      failureKind: "quality_gap",
      reason: "Candidate official website URL is missing.",
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass: "Run a focused official-website-only profile pass for this candidate. Add the source-backed official website, or mark candidate_profile.official_website_url confirmed_null if none exists.",
    });
  }
  if (input.includeParty && !input.profile.party) {
    gaps.push({
      id: "candidate_profile.party",
      stage: "candidate_profile",
      objectType: "candidate_profile",
      outcome: "needs_repair",
      field: "party",
      failureKind: "quality_gap",
      reason: "Partisan candidate profile is missing party.",
      promptFile: "src/ai/providers/candidateProfilePrompt.ts",
      focusedResearchPass: "Run a focused party-only profile pass using official roster/filing sources. Add party if source-backed, or mark candidate_profile.party confirmed_null if no reliable party exists.",
    });
  }
  gaps.push({
    id: "candidate_profile.current_office",
    stage: "candidate_profile",
    objectType: "candidate_profile",
    outcome: "blocked_by_contract",
    field: "current_office",
    failureKind: "quality_gap",
    reason: "candidates.current_office exists in the database/API, but CandidateProfilePayload and the manual profile writer do not currently support writing it.",
    promptFile: "src/ai/providers/candidateProfilePrompt.ts",
    focusedResearchPass: "Do not place occupation or professional role into current_office. Track this as blocked_by_contract until profile contract/writer support is added.",
  });
  return gaps;
}

async function writeProfileRepairReport(input: {
  reportFile: string | null;
  manualKey: string;
  electionId: string;
  file: string;
  displayName?: string | null;
  gaps: ManualResearchRepairGap[];
}): Promise<void> {
  await writeManualResearchRepairReport(
    input.reportFile,
    buildManualResearchRepairReport({
      command: "manual:candidate-profile:write",
      manualKey: input.manualKey,
      target: {
        electionId: input.electionId,
        file: input.file,
        displayName: input.displayName ?? null,
      },
      gaps: input.gaps,
    })
  );
}

async function main(): Promise<void> {
  loadProjectEnv();

  const file = readFlag("--file");
  const electionId = readFlag("--election-id");
  const repairReportFile = readFlag("--repair-report-file");
  const strictQualityGate = hasFlag("--strict-quality-gate");
  const confirmedGapIds = normalizeConfirmedGaps(readRepeatedFlag("--confirmed-gap"));
  if (!file || !electionId) {
    throw new Error(`Missing --file or --election-id.\n${usage()}`);
  }

  const rawPayload = await readJsonFile(file);
  const fallbackManualKey = `manual:candidate-profile:${electionId}:payload`;
  const validatedProfile = await validateCandidateProfileAiPayload(
    rawPayload,
    readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000)
  );
  if (!validatedProfile.ok) {
    const gaps = [buildProfileValidationGap({
      reason: validatedProfile.reason,
      failedCitationUrls: validatedProfile.failedCitationUrls,
    })];
    await writeProfileRepairReport({
      reportFile: repairReportFile,
      manualKey: fallbackManualKey,
      electionId,
      file,
      displayName: null,
      gaps,
    });
    throw new Error(`Candidate profile payload failed validation: ${validatedProfile.reason}`);
  }

  const profile = validatedProfile.profile;
  if (!hasFlag("--allow-no-hard-identifier") && !hasAtLeastOneHardIdentifier(profile)) {
    const gaps = [buildProfileValidationGap({
      reason: "Candidate profile has no hard identifier.",
    })];
    await writeProfileRepairReport({
      reportFile: repairReportFile,
      manualKey: fallbackManualKey,
      electionId,
      file,
      displayName: profile.display_name,
      gaps,
    });
    throw new Error(
      "Candidate profile has no hard identifier. Add official_website_url, FEC/state filing ID, DOB, Twitter, LinkedIn, or pass --allow-no-hard-identifier deliberately."
    );
  }

  const dryRun = hasFlag("--dry-run");
  const emitRecordDraft = hasFlag("--emit-record-draft");
  const emitFinanceSync = hasFlag("--emit-finance-sync");
  const runId = readFlag("--run-id") ?? `manual_candidate_profile_${new Date().toISOString()}`;
  const isIncumbent = readBooleanFlag("--is-incumbent");
  const manualKey = `manual:candidate-profile:${electionId}:${profile.display_name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;
  const databaseUrl = requireEnv("DATABASE_URL");
  const redisUrl = emitRecordDraft && !dryRun ? requireEnv("REDIS_URL") : null;

  const pool = new Pool({ connectionString: databaseUrl });
  const redis = redisUrl ? createClient({ url: redisUrl }) : null;

  try {
    const election = await loadElectionContext(pool, electionId);
    if (!election) {
      throw new Error(`Election not found for election_id=${electionId}`);
    }
    if (election.race_type !== "office") {
      throw new Error(`Candidate profile write requires an office election; election_id=${electionId} has race_type=${election.race_type}`);
    }
    const includeParty = resolveIncludePartyForCandidateContest({
      districtType: election.district_type,
      state: election.state,
      officialBallotTitle: election.official_ballot_title,
      electionIsPartisan: election.is_partisan,
    });
    const qualityGaps = applyConfirmedGaps(
      buildCandidateProfileQualityGaps({ profile, includeParty }),
      confirmedGapIds
    );
    const blockingQualityGaps = qualityGaps.filter((gap) => gap.outcome === "needs_repair");
    if (repairReportFile && qualityGaps.length > 0) {
      await writeProfileRepairReport({
        reportFile: repairReportFile,
        manualKey,
        electionId,
        file,
        displayName: profile.display_name,
        gaps: qualityGaps,
      });
    }
    if (strictQualityGate && blockingQualityGaps.length > 0) {
      throw new Error(
        `Candidate profile quality gate failed; run focused gap-repair pass before import. gaps=${blockingQualityGaps.length}; ${summarizeManualResearchGaps(blockingQualityGaps)}`
      );
    }

    if (dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            manualKey,
            runId,
            electionId: election.election_id,
            displayName: profile.display_name,
            hasHardIdentifier: hasAtLeastOneHardIdentifier(profile),
            emitRecordDraft,
            emitFinanceSync,
            state: election.state,
            raceType: election.race_type,
            districtType: election.district_type,
            officialBallotTitle: election.official_ballot_title,
            financeSync: {
              wouldEmit: emitFinanceSync,
              officeScope: election.office_scope,
              officeCanonicalName: election.office_canonical_name,
              fecIds: profile.fec_ids ?? [],
            },
            sourceValidation: {
              sourceUrlsReachable: true,
              sourceCount: validatedProfile.sourceCount,
              sources: profile.sources,
            },
            qualityGate: {
              strict: strictQualityGate,
              confirmedGaps: [...confirmedGapIds].sort(),
              gaps: qualityGaps,
            },
          },
          null,
          2
        )
      );
      return;
    }

    const client = await pool.connect();
    let candidateId: string;
    let matchedExisting: boolean;
    let candidateElectionCreated = false;
    try {
      await client.query("BEGIN");
      const candidateResult = await findOrCreateCandidateFromProfile({
        client,
        profile,
        state: election.state,
        rosterParty: profile.party,
        includeParty,
      });
      candidateId = candidateResult.candidateId;
      matchedExisting = candidateResult.matchedExisting;
      const existingIncumbency = await loadExistingCandidateElectionIncumbency(client, {
        candidateId,
        electionId,
      });

      const linkResult = await upsertCandidateElection({
        client,
        candidateId,
        electionId,
        isIncumbent: isIncumbent ?? existingIncumbency ?? false,
      });
      candidateElectionCreated = linkResult.created;
      if (linkResult.created) {
        await createCandidateFutureElectionNotificationEvents(client, {
          candidateId,
          electionId,
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
    let financeSync: CandidateProfileFinanceSyncFanoutResult | null = null;
    if (emitFinanceSync) {
      financeSync = await enqueueCandidateProfileFinanceSyncFanoutForLinkedElection({
        context: buildLinkedElectionFinanceContext({
          election,
          includeParty,
          profileParty: profile.party,
          isIncumbent: isIncumbent ?? undefined,
        }),
        candidateId,
        fecIds: profile.fec_ids,
      });
    }
    if (redis) {
      try {
        await redis.connect();
        recordDraft = await enqueueCandidateRecordDrafts(redis, [{ candidateId, electionId, runId }]);
      } catch (error) {
        throw new Error(
          `Candidate profile DB write committed for candidate_id=${candidateId} election_id=${electionId}, but record-draft Redis emit failed: ${toReason(error)}. Recovery: re-run this command with --emit-record-draft after Redis is available; candidate/election writes are idempotent.`
        );
      }
    }

    console.log(
      JSON.stringify(
        {
          manualKey,
          runId,
          electionId,
          candidateId,
          matchedExisting,
          candidateElectionCreated,
          financeSync,
          recordDraft,
        },
        null,
        2
      )
    );
  } finally {
    if (redis) {
      await redis.quit().catch(() => undefined);
    }
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual candidate profile write failed:", message);
  process.exitCode = 1;
});

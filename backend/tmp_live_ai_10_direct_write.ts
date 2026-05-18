import { performance } from "node:perf_hooks";
import { Pool } from "pg";

import { loadProjectEnv, getPipelineEnv } from "./src/config/env.ts";
import { STATE_RESOURCES_AI_CANDIDATES } from "./src/ai/aiCandidates.ts";
import type { EnrichStateResourcesConfig, EnrichStateResourcesInput, RetryFeedback } from "./src/ai/types.ts";
import { enrichStateResourcesGroup } from "./src/ai/enrichStateResources.ts";
import { STATE_RESOURCE_FIELD_GROUP_ORDER, type StateResourceFieldGroup } from "./src/ai/stateResourceFieldGroups.ts";
import { collectStateResourceEvidence } from "./src/pipeline/evidence/stateResourceEvidenceCollector.ts";
import {
  STATE_RESOURCE_EARLY_VOTING_REFERENCE_SEED,
  STATE_RESOURCE_ID_REQUIREMENTS_REFERENCE_SEED,
  STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_REFERENCE_SEED,
  STATE_RESOURCE_MAIL_REFERENCE_SEED,
  STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEED,
  STATE_RESOURCE_POLLING_HOURS_REFERENCE_SEED,
  STATE_RESOURCE_POLLING_PLACES_REFERENCE_SEEDS,
  STATE_RESOURCE_SAME_DAY_REGISTRATION_DEADLINE_REFERENCE_SEED,
} from "./src/config/stateResourcePipeline.ts";
import { STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL } from "./src/contracts/stateResourceEnrichmentContract.ts";
import type { StateResourcePayload, StateResourceSources } from "./src/types/stateResource.ts";
import { normalizeHttpUrl } from "./src/utils/normalizeHttpUrl.ts";

type StateRow = {
  state_fips: string;
  state_abbreviation: string;
  state_name: string;
};

type AttemptLog = {
  group: StateResourceFieldGroup;
  provider: string;
  model: string;
  promptVariant: "default" | "citation_repair";
  ok: boolean;
  errorCode?: string;
  reason?: string;
  elapsedMs: number;
};

type RowLog = {
  state_fips: string;
  state_abbreviation: string;
  state_name: string;
  ok: boolean;
  elapsedMs: number;
  failureReason: string | null;
  attemptLogs: AttemptLog[];
};

function toStateSlug(stateName: string): string | null {
  const slug = stateName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug.length > 0 ? slug : null;
}

function buildStateScopedReferenceUrl(baseUrl: string, stateName: string): string | null {
  const slug = toStateSlug(stateName);
  if (!slug) {
    return null;
  }
  return normalizeHttpUrl(`${baseUrl.replace(/\/+$/, "")}/${slug}`);
}

function buildGroupSeedSources(group: StateResourceFieldGroup, stateName: string): string[] {
  if (group === "mail") {
    const scoped = buildStateScopedReferenceUrl(STATE_RESOURCE_MAIL_REFERENCE_SEED, stateName);
    return scoped ? [scoped] : [STATE_RESOURCE_MAIL_REFERENCE_SEED];
  }
  if (group === "online_registration") {
    const scoped = buildStateScopedReferenceUrl(STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEED, stateName);
    return scoped ? [scoped] : [STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEED];
  }
  if (group === "early_voting") {
    return [STATE_RESOURCE_EARLY_VOTING_REFERENCE_SEED];
  }
  if (group === "polling_hours") {
    return [STATE_RESOURCE_POLLING_HOURS_REFERENCE_SEED];
  }
  if (group === "polling_place") {
    return [...STATE_RESOURCE_POLLING_PLACES_REFERENCE_SEEDS];
  }
  if (group === "same_day_registration") {
    return [STATE_RESOURCE_SAME_DAY_REGISTRATION_DEADLINE_REFERENCE_SEED];
  }
  if (group === "id_requirements") {
    return [STATE_RESOURCE_ID_REQUIREMENTS_REFERENCE_SEED];
  }
  const scoped = buildStateScopedReferenceUrl(
    STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_REFERENCE_SEED,
    stateName
  );
  return scoped ? [scoped] : [STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_REFERENCE_SEED];
}

function hostAsTitle(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "source";
  }
}

function buildRetryFeedback(reason: string, failureDebug?: Record<string, unknown>): RetryFeedback {
  const failedCitationUrls = Array.isArray(failureDebug?.failed_citation_urls)
    ? (failureDebug?.failed_citation_urls as unknown[])
        .filter((v): v is string => typeof v === "string")
    : [];
  return {
    previousFailureReason: reason,
    failedCitationUrls,
    failedCitationDetails: [],
    retryCount: 1,
    failedAt: new Date().toISOString(),
  };
}

async function upsertStateResource(pool: Pool, payload: StateResourcePayload): Promise<void> {
  await pool.query(
    `
      INSERT INTO state_resources (
        state_fips,
        state_abbreviation,
        state_name,
        polling_place_url,
        voter_registration_url,
        mail_voting_available,
        mail_ballot_request_deadline_rule,
        mail_ballot_return_deadline_rule,
        mail_ballot_return_deadline_type,
        early_voting_available,
        early_voting_start_date_rule,
        early_voting_end_date_rule,
        polling_hours,
        id_requirements,
        same_day_registration_available,
        online_registration_available,
        online_registration_deadline_rule,
        in_person_registration_deadline_rule,
        sources
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb
      )
      ON CONFLICT (state_fips) DO UPDATE SET
        state_abbreviation = EXCLUDED.state_abbreviation,
        state_name = EXCLUDED.state_name,
        polling_place_url = EXCLUDED.polling_place_url,
        voter_registration_url = EXCLUDED.voter_registration_url,
        mail_voting_available = EXCLUDED.mail_voting_available,
        mail_ballot_request_deadline_rule = EXCLUDED.mail_ballot_request_deadline_rule,
        mail_ballot_return_deadline_rule = EXCLUDED.mail_ballot_return_deadline_rule,
        mail_ballot_return_deadline_type = EXCLUDED.mail_ballot_return_deadline_type,
        early_voting_available = EXCLUDED.early_voting_available,
        early_voting_start_date_rule = EXCLUDED.early_voting_start_date_rule,
        early_voting_end_date_rule = EXCLUDED.early_voting_end_date_rule,
        polling_hours = EXCLUDED.polling_hours,
        id_requirements = EXCLUDED.id_requirements,
        same_day_registration_available = EXCLUDED.same_day_registration_available,
        online_registration_available = EXCLUDED.online_registration_available,
        online_registration_deadline_rule = EXCLUDED.online_registration_deadline_rule,
        in_person_registration_deadline_rule = EXCLUDED.in_person_registration_deadline_rule,
        sources = EXCLUDED.sources
    `,
    [
      payload.state_fips,
      payload.state_abbreviation,
      payload.state_name,
      payload.polling_place_url,
      payload.voter_registration_url,
      payload.mail_voting_available,
      payload.mail_ballot_request_deadline_rule,
      payload.mail_ballot_return_deadline_rule,
      payload.mail_ballot_return_deadline_type,
      payload.early_voting_available,
      payload.early_voting_start_date_rule,
      payload.early_voting_end_date_rule,
      payload.polling_hours,
      payload.id_requirements,
      payload.same_day_registration_available,
      payload.online_registration_available,
      payload.online_registration_deadline_rule,
      payload.in_person_registration_deadline_rule,
      JSON.stringify(payload.sources),
    ]
  );
}

async function main(): Promise<void> {
  loadProjectEnv();
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = performance.now();

  const cfgBase: Omit<EnrichStateResourcesConfig, "provider" | "model"> = {
    timeoutMs: env.AI_TIMEOUT_MS,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };

  const rowLogs: RowLog[] = [];
  const modelAttempts = new Map<string, number>();
  const modelSuccesses = new Map<string, number>();
  const modelFailures = new Map<string, number>();

  try {
    const statesRes = await pool.query<StateRow>(
      `
      SELECT state_fips, state_abbreviation, state_name
      FROM state_resources
      ORDER BY state_fips
      LIMIT 10
      `
    );

    for (const state of statesRes.rows) {
      const rowStart = performance.now();
      const attemptLogs: AttemptLog[] = [];
      const mergedFields: Partial<Omit<StateResourcePayload, "sources">> & { sources: Partial<StateResourceSources> } = {
        sources: {},
      };
      let failureReason: string | null = null;

      for (const group of STATE_RESOURCE_FIELD_GROUP_ORDER) {
        const seedSources = buildGroupSeedSources(group, state.state_name);
        const groupDraft = {
          state_fips: state.state_fips,
          state_abbreviation: state.state_abbreviation,
          state_name: state.state_name,
          population_estimate: null,
          census_source_url: null,
          state_abbreviation_reference_url: null,
          seed_sources: seedSources,
        };

        let evidence = await collectStateResourceEvidence(groupDraft);
        if (evidence.length === 0) {
          // URL-only fallback for bot-blocked sources (e.g. 403): still pass explicit seed URLs to AI.
          evidence = seedSources
            .map((url) => normalizeHttpUrl(url))
            .filter((url): url is string => typeof url === "string")
            .map((url) => ({ url, title: hostAsTitle(url) }));
        }

        if (evidence.length === 0) {
          failureReason = `No evidence URLs available for group=${group}`;
          break;
        }

        let groupSucceeded = false;
        let retryFeedback: RetryFeedback | null = null;

        for (const candidate of STATE_RESOURCES_AI_CANDIDATES) {
          for (const promptVariant of ["default", "citation_repair"] as const) {
            const modelKey = `${candidate.provider}:${candidate.model}`;
            modelAttempts.set(modelKey, (modelAttempts.get(modelKey) ?? 0) + 1);

            const attemptStart = performance.now();
            const result = await enrichStateResourcesGroup(
              {
                ingestKey: `direct10:${state.state_fips}:${group}`,
                fieldGroup: group,
                draft: groupDraft,
                evidence,
                promptVersion: env.PROMPT_VERSION,
                promptVariant,
                retryFeedback,
              },
              {
                ...cfgBase,
                provider: candidate.provider,
                model: candidate.model,
              }
            );
            const elapsedMs = Math.round(performance.now() - attemptStart);

            if (result.ok) {
              attemptLogs.push({
                group,
                provider: candidate.provider,
                model: candidate.model,
                promptVariant,
                ok: true,
                elapsedMs,
              });
              modelSuccesses.set(modelKey, (modelSuccesses.get(modelKey) ?? 0) + 1);

              for (const [key, value] of Object.entries(result.payload)) {
                if (key === "sources") {
                  continue;
                }
                (mergedFields as Record<string, unknown>)[key] = value;
              }
              for (const [key, value] of Object.entries(result.payload.sources)) {
                (mergedFields.sources as Record<string, string[]>)[key] = value as string[];
              }

              groupSucceeded = true;
              break;
            }

            attemptLogs.push({
              group,
              provider: candidate.provider,
              model: candidate.model,
              promptVariant,
              ok: false,
              errorCode: result.errorCode,
              reason: result.reason,
              elapsedMs,
            });
            modelFailures.set(modelKey, (modelFailures.get(modelKey) ?? 0) + 1);
            retryFeedback = buildRetryFeedback(result.reason, result.failureDebug);
          }
          if (groupSucceeded) {
            break;
          }
        }

        if (!groupSucceeded) {
          failureReason = `Group ${group} failed across all models/variants`;
          break;
        }
      }

      if (!failureReason) {
        const sources = mergedFields.sources as StateResourceSources;
        const payload: StateResourcePayload = {
          state_fips: state.state_fips,
          state_abbreviation: state.state_abbreviation,
          state_name: state.state_name,
          voter_registration_url: STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
          polling_place_url: mergedFields.polling_place_url as string,
          mail_voting_available: mergedFields.mail_voting_available as boolean,
          mail_ballot_request_deadline_rule: mergedFields.mail_ballot_request_deadline_rule as string | null,
          mail_ballot_return_deadline_rule: mergedFields.mail_ballot_return_deadline_rule as string | null,
          mail_ballot_return_deadline_type: mergedFields.mail_ballot_return_deadline_type as
            | "postmarked_by"
            | "received_by"
            | null,
          early_voting_available: mergedFields.early_voting_available as boolean,
          early_voting_start_date_rule: mergedFields.early_voting_start_date_rule as string | null,
          early_voting_end_date_rule: mergedFields.early_voting_end_date_rule as string | null,
          polling_hours: mergedFields.polling_hours as string,
          id_requirements: mergedFields.id_requirements as
            | "Strict photo ID"
            | "Strict non-photo ID"
            | "Non-strict photo ID"
            | "Non-strict, non-photo ID"
            | "No document required to vote",
          same_day_registration_available: mergedFields.same_day_registration_available as boolean,
          online_registration_available: mergedFields.online_registration_available as boolean,
          online_registration_deadline_rule: mergedFields.online_registration_deadline_rule as string | null,
          in_person_registration_deadline_rule: mergedFields.in_person_registration_deadline_rule as string,
          sources,
        };
        await upsertStateResource(pool, payload);
      }

      rowLogs.push({
        state_fips: state.state_fips,
        state_abbreviation: state.state_abbreviation,
        state_name: state.state_name,
        ok: failureReason === null,
        elapsedMs: Math.round(performance.now() - rowStart),
        failureReason,
        attemptLogs,
      });
    }

    const okRows = rowLogs.filter((r) => r.ok);
    const failedRows = rowLogs.filter((r) => !r.ok);
    const targetFips = rowLogs.map((r) => r.state_fips);
    const populatedRows = await pool.query(
      `
      SELECT
        state_fips,
        state_abbreviation,
        state_name,
        polling_place_url,
        voter_registration_url,
        polling_hours,
        id_requirements,
        sources,
        online_registration_available,
        online_registration_deadline_rule,
        same_day_registration_available,
        mail_voting_available,
        mail_ballot_request_deadline_rule,
        mail_ballot_return_deadline_rule,
        mail_ballot_return_deadline_type,
        in_person_registration_deadline_rule,
        early_voting_available,
        early_voting_start_date_rule,
        early_voting_end_date_rule
      FROM state_resources
      WHERE state_fips = ANY($1::text[])
      ORDER BY state_fips
      `,
      [targetFips]
    );

    const summary = {
      type: "live_ai_10_direct_summary",
      totalElapsedMs: Math.round(performance.now() - startedAt),
      attemptedRows: rowLogs.length,
      writtenRows: okRows.length,
      failedRows: failedRows.length,
      modelAttempts: Object.fromEntries([...modelAttempts.entries()].sort()),
      modelSuccesses: Object.fromEntries([...modelSuccesses.entries()].sort()),
      modelFailures: Object.fromEntries([...modelFailures.entries()].sort()),
      tokenUsage: "not available from current provider wrappers (no usage fields captured)",
      rowLogs,
      populatedRows: populatedRows.rows,
    };

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});


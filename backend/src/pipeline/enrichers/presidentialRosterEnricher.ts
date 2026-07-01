import type { Pool, PoolClient } from "pg";

import {
  buildPresidentialRosterAiConfigFromEnv,
  enrichPresidentialRoster,
  type PresidentialRosterAiConfig,
  type PresidentialRosterAiInput,
  type PresidentialRosterAiResult,
} from "../../ai/enrichPresidentialRoster.js";
import {
  enrichPresidentialRosterStatus,
  type PresidentialRosterStatusAiConfig,
  type PresidentialRosterStatusAiInput,
  type PresidentialRosterStatusAiResult,
} from "../../ai/enrichPresidentialRosterStatus.js";
import type { AiCandidate } from "../../ai/aiCandidates.js";
import type { PresidentialRosterCandidate } from "../../contracts/presidentialRosterPayloadContract.js";
import {
  enqueueCandidateProfileDrafts,
  type CandidateProfileDraftEmitInput,
} from "../candidates/candidateProfileDraftEmitter.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";
import {
  withdrawPresidentialCycleCandidateByCandidateId,
  withdrawPresidentialCycleCandidateByFecId,
} from "../candidates/candidateProfileLinks.js";
import {
  matchPresidentialRosterCandidateToFec,
  type PresidentialCandidateFecMatch,
  type PresidentialCandidateFecMatchInput,
  type PresidentialCandidateFecMatcherOptions,
} from "../presidential/presidentialCandidateFecMatcher.js";
import { readOpenFecApiKeysFromEnv } from "../presidential/openFecClient.js";
import type { PresidentialCycleStage } from "../presidential/presidentialCycles.js";
import {
  loadActivePresidentialCycleCandidatesForReconciliation,
  type ActivePresidentialCycleCandidateForReconciliation,
} from "../presidential/presidentialRosterReconciliation.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

type PresidentialRosterCycleRow = {
  id: string;
  election_year: number;
  stage: PresidentialCycleStage;
  party: string | null;
};

type ExistingPresidentialCycleCandidateRow = {
  candidate_id: string;
  presidential_profile_researched: boolean;
  running_mate_candidate_id: string | null;
  running_mate_profile_researched: boolean;
  running_mate_display_name: string | null;
  running_mate_first_name: string | null;
  running_mate_last_name: string | null;
  running_mate_fec_ids: unknown;
};

type ExistingPresidentialCycleCandidate = {
  candidateId: string;
  presidentialProfileResearched: boolean;
  runningMateCandidateId: string | null;
  runningMateProfileResearched: boolean;
  runningMateDisplayName: string | null;
  runningMateFecIds: string[];
};

export const PRESIDENTIAL_ROSTER_ADMISSION_POLICY = "fec_confirmed_only" as const;

export type PresidentialRosterCycleLookup = {
  cycleId?: string | null;
  electionYear: number;
  stage?: PresidentialCycleStage;
  party?: string | null;
};

export type PresidentialRosterEnricherInput = PresidentialRosterCycleLookup & {
  db: Queryable;
  redis: RedisSendCommandClient;
  runId?: string | null;
  dryRun?: boolean;
  aiConfig?: PresidentialRosterAiConfig;
  aiCandidates?: readonly AiCandidate[];
  fecOptions?: PresidentialCandidateFecMatcherOptions;
  enrichRoster?: (
    input: PresidentialRosterAiInput,
    config: PresidentialRosterAiConfig,
    candidates?: readonly AiCandidate[]
  ) => Promise<PresidentialRosterAiResult>;
  enrichRosterStatus?: (
    input: PresidentialRosterStatusAiInput,
    config: PresidentialRosterStatusAiConfig,
    candidates?: readonly AiCandidate[]
  ) => Promise<PresidentialRosterStatusAiResult>;
  matchCandidate?: (input: PresidentialCandidateFecMatchInput) => Promise<PresidentialCandidateFecMatch>;
  loadActiveCandidatesForReconciliation?: (
    db: Queryable,
    cycleId: string
  ) => Promise<ActivePresidentialCycleCandidateForReconciliation[]>;
};

export type PresidentialRosterStatusVerificationSummary = {
  checkedCount: number;
  withdrawnCount: number;
  activeCount: number;
  skippedCount: number;
  demotedCount: number;
  dryRun: boolean;
  provider?: string;
  model?: string;
  error?: string;
  errorCode?: string;
};

export type PresidentialRosterEnricherResult =
  | {
      ok: true;
      cycleId: string;
      electionYear: number;
      stage: PresidentialCycleStage;
      party: string | null;
      provider: string;
      model: string;
      aiCandidateCount: number;
      matchedCount: number;
      ambiguousCount: number;
      unmatchedCount: number;
      withdrawnSkippedCount: number;
      withdrawnDemotedCount: number;
      emittedCount: number;
      skippedCount: number;
      dryRun: boolean;
      admissionPolicy: typeof PRESIDENTIAL_ROSTER_ADMISSION_POLICY;
      statusVerification: PresidentialRosterStatusVerificationSummary;
      matches: Array<{
        displayName: string;
        status: PresidentialRosterCandidate["status"];
        matchStatus: PresidentialCandidateFecMatch["matchStatus"] | "skipped_withdrawn";
        method: PresidentialCandidateFecMatch["method"] | "skipped_withdrawn";
        admissionStatus: "admitted" | "not_admitted";
        admissionReason: string;
        matchedFecId?: string;
        reason?: string;
      }>;
      aiRawDebug: Record<string, unknown> | null;
    }
  | {
      ok: false;
      cycleId?: string;
      electionYear: number;
      stage: PresidentialCycleStage;
      party: string | null;
      error: string;
      retryable: boolean;
      errorCode: string;
      failureDebug?: Record<string, unknown>;
    };

function normalizeStage(value: PresidentialCycleStage | undefined): PresidentialCycleStage {
  return value ?? "primary";
}

function normalizeParty(value: string | null | undefined, stage: PresidentialCycleStage): string | null {
  const normalized = value?.trim() ?? "";
  if (stage === "primary" && normalized.length === 0) {
    throw new Error("presidential roster primary party is required");
  }
  if (stage === "general") {
    return null;
  }
  return normalized;
}

function mergeSeedUrls(...lists: Array<readonly string[] | undefined>): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();
  for (const list of lists) {
    for (const item of list ?? []) {
      const trimmed = item.trim();
      if (trimmed.length === 0 || seen.has(trimmed)) {
        continue;
      }
      seen.add(trimmed);
      merged.push(trimmed);
    }
  }
  return merged;
}

function cycleName(row: PresidentialRosterCycleRow): string {
  if (row.stage === "general") {
    return `${row.election_year} presidential general election`;
  }
  return `${row.election_year} ${row.party ?? ""} presidential primary`.replace(/\s+/g, " ").trim();
}

function parseFecIds(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const fecId = item.trim().toUpperCase();
    if (!/^P\d{8}$/.test(fecId) || seen.has(fecId)) {
      continue;
    }
    seen.add(fecId);
    normalized.push(fecId);
  }
  return normalized;
}

function displayNameFromParts(input: {
  displayName: string | null;
  firstName: string | null;
  lastName: string | null;
}): string | null {
  const storedDisplayName = input.displayName?.trim();
  if (storedDisplayName) {
    return storedDisplayName;
  }
  const firstLast = `${input.firstName ?? ""} ${input.lastName ?? ""}`.replace(/\s+/g, " ").trim();
  return firstLast.length > 0 ? firstLast : null;
}

async function loadPresidentialRosterCycle(
  db: Queryable,
  input: PresidentialRosterCycleLookup
): Promise<PresidentialRosterCycleRow | null> {
  const stage = normalizeStage(input.stage);
  const party = normalizeParty(input.party, stage);
  const cycleId = input.cycleId?.trim();
  if (cycleId) {
    const result = await db.query<PresidentialRosterCycleRow>(
      `
        SELECT id, election_year, stage, party
        FROM public.presidential_cycles
        WHERE id::text = $1
        LIMIT 1
      `,
      [cycleId]
    );
    const row = result.rows[0] ?? null;
    if (!row) {
      return null;
    }
    if (row.election_year !== input.electionYear || row.stage !== stage || (row.party ?? null) !== party) {
      throw new Error(
        `presidential cycle ${cycleId} does not match requested year=${input.electionYear} stage=${stage} party=${party ?? "null"}`
      );
    }
    return row;
  }

  const result = await db.query<PresidentialRosterCycleRow>(
    `
      SELECT id, election_year, stage, party
      FROM public.presidential_cycles
      WHERE election_year = $1::int
        AND stage = $2
        AND (
          ($2 = 'general' AND party IS NULL)
          OR
          ($2 = 'primary' AND party = $3)
        )
      LIMIT 1
    `,
    [input.electionYear, stage, party]
  );
  return result.rows[0] ?? null;
}

async function loadExistingPresidentialCycleCandidateByFecId(input: {
  db: Queryable;
  cycleId: string;
  fecCandidateId: string;
}): Promise<ExistingPresidentialCycleCandidate | null> {
  const fecCandidateId = input.fecCandidateId.trim().toUpperCase();
  const result = await input.db.query<ExistingPresidentialCycleCandidateRow>(
    `
      SELECT
        cycle_candidate.candidate_id,
        cycle_candidate.presidential_profile_researched,
        cycle_candidate.running_mate_candidate_id,
        cycle_candidate.running_mate_profile_researched,
        running_mate.display_name AS running_mate_display_name,
        running_mate.first_name AS running_mate_first_name,
        running_mate.last_name AS running_mate_last_name,
        running_mate.fec_ids AS running_mate_fec_ids
      FROM public.presidential_cycle_candidates AS cycle_candidate
      JOIN public.candidates AS candidate
        ON candidate.id = cycle_candidate.candidate_id
      LEFT JOIN public.candidates AS running_mate
        ON running_mate.id = cycle_candidate.running_mate_candidate_id
       AND running_mate.deleted_at IS NULL
      WHERE cycle_candidate.cycle_id = $1
        AND candidate.deleted_at IS NULL
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(candidate.fec_ids) AS fec_id(value)
          WHERE upper(trim(fec_id.value)) = $2
        )
      LIMIT 1
    `,
    [input.cycleId, fecCandidateId]
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    candidateId: row.candidate_id,
    presidentialProfileResearched: row.presidential_profile_researched === true,
    runningMateCandidateId: row.running_mate_candidate_id,
    runningMateProfileResearched: row.running_mate_profile_researched === true,
    runningMateDisplayName: displayNameFromParts({
      displayName: row.running_mate_display_name,
      firstName: row.running_mate_first_name,
      lastName: row.running_mate_last_name,
    }),
    runningMateFecIds: parseFecIds(row.running_mate_fec_ids),
  };
}

function toDraftInput(input: {
  cycle: PresidentialRosterCycleRow;
  runId: string | null;
  rosterIndex: number;
  candidate: PresidentialRosterCandidate;
  match: PresidentialCandidateFecMatch;
}) {
  // V1 intentionally admits only OpenFEC-confirmed presidential candidates.
  // AI-only roster claims stay visible in matches[] but do not create profiles.
  if (!input.match.matchedFecId) {
    throw new Error("Cannot create presidential profile draft without matched FEC ID");
  }
  const rosterParty = input.cycle.stage === "primary" ? input.cycle.party : input.candidate.party;
  if (!rosterParty) {
    throw new Error(`Missing presidential roster party for cycle ${input.cycle.id}`);
  }
  return {
    contextType: "presidential_cycle" as const,
    presidentialCycleId: input.cycle.id,
    presidentialRole: "president" as const,
    runId: input.runId,
    displayName: input.candidate.display_name,
    rosterIndex: input.rosterIndex,
    rosterParty,
    fecIds: [input.match.matchedFecId],
    seedUrls: mergeSeedUrls(input.candidate.sources, input.match.fecSourceUrls),
    disambiguationHint: `Candidate reported for ${cycleName(input.cycle)}; matched to OpenFEC candidate ${input.match.matchedFecId}.`,
    dedupeKey: `presidential_cycle:${input.cycle.id}:fec:${input.match.matchedFecId.toUpperCase()}`,
  };
}

function toRunningMateDraftInput(input: {
  cycle: PresidentialRosterCycleRow;
  runId: string | null;
  rosterIndex: number;
  candidate: PresidentialRosterCandidate;
  parentFecCandidateId: string;
}): CandidateProfileDraftEmitInput | null {
  if (!input.candidate.running_mate) {
    return null;
  }
  const rosterParty = input.cycle.stage === "primary" ? input.cycle.party : input.candidate.party;
  if (!rosterParty) {
    throw new Error(`Missing presidential running mate roster party for cycle ${input.cycle.id}`);
  }
  const parentFecCandidateId = input.parentFecCandidateId.trim().toUpperCase();
  const runningMateFecId = input.candidate.running_mate.fec_candidate_id?.trim().toUpperCase();
  const normalizedRunningMateName = normalizeCandidateName(input.candidate.running_mate.display_name);
  return {
    contextType: "presidential_cycle",
    presidentialCycleId: input.cycle.id,
    presidentialRole: "vice_president",
    parentPresidentialCandidateFecId: parentFecCandidateId,
    runId: input.runId,
    displayName: input.candidate.running_mate.display_name,
    rosterIndex: input.rosterIndex,
    rosterParty,
    ...(runningMateFecId ? { fecIds: [runningMateFecId] } : {}),
    seedUrls: input.candidate.running_mate.sources,
    disambiguationHint: `Officially announced running mate for ${input.candidate.display_name} in ${cycleName(input.cycle)}.`,
    dedupeKey: `presidential_cycle:${input.cycle.id}:running_mate:${parentFecCandidateId}:${normalizedRunningMateName}`,
  };
}

function runningMateMatchesExisting(
  runningMate: NonNullable<PresidentialRosterCandidate["running_mate"]>,
  existing: ExistingPresidentialCycleCandidate
): boolean {
  const fecCandidateId = runningMate.fec_candidate_id?.trim().toUpperCase();
  if (fecCandidateId) {
    return existing.runningMateFecIds.includes(fecCandidateId);
  }
  const incomingName = normalizeCandidateName(runningMate.display_name);
  const existingName = existing.runningMateDisplayName ? normalizeCandidateName(existing.runningMateDisplayName) : "";
  return incomingName.length > 0 && incomingName === existingName;
}

function normalizeFecIdSet(values: Iterable<string>): Set<string> {
  const normalized = new Set<string>();
  for (const value of values) {
    const fecId = value.trim().toUpperCase();
    if (fecId.length > 0) {
      normalized.add(fecId);
    }
  }
  return normalized;
}

function hasAnyFecId(candidate: ActivePresidentialCycleCandidateForReconciliation, fecIds: ReadonlySet<string>): boolean {
  return candidate.fecIds.some((fecId) => fecIds.has(fecId.trim().toUpperCase()));
}

function isExactPresidentialFecMatch(match: PresidentialCandidateFecMatch): boolean {
  return match.matchStatus === "matched" && (match.method === "exact_fec_id" || match.method === "exact_name_party");
}

async function verifyOmittedActiveCandidates(input: {
  db: Queryable;
  cycle: PresidentialRosterCycleRow;
  dryRun: boolean;
  currentActiveFecIds: ReadonlySet<string>;
  aiConfig: PresidentialRosterAiConfig;
  aiCandidates?: readonly AiCandidate[];
  loadActiveCandidatesForReconciliation: (
    db: Queryable,
    cycleId: string
  ) => Promise<ActivePresidentialCycleCandidateForReconciliation[]>;
  enrichRosterStatus: (
    input: PresidentialRosterStatusAiInput,
    config: PresidentialRosterStatusAiConfig,
    candidates?: readonly AiCandidate[]
  ) => Promise<PresidentialRosterStatusAiResult>;
}): Promise<PresidentialRosterStatusVerificationSummary> {
  try {
    const activeCandidates = await input.loadActiveCandidatesForReconciliation(input.db, input.cycle.id);
    const omittedCandidates = activeCandidates.filter(
      (candidate) => !hasAnyFecId(candidate, input.currentActiveFecIds)
    );

    if (omittedCandidates.length === 0) {
      return {
        checkedCount: 0,
        withdrawnCount: 0,
        activeCount: 0,
        skippedCount: activeCandidates.length,
        demotedCount: 0,
        dryRun: input.dryRun,
      };
    }

    const statusResult = await input.enrichRosterStatus(
      {
        cycleId: input.cycle.id,
        electionYear: input.cycle.election_year,
        stage: input.cycle.stage,
        party: input.cycle.party,
        candidates: omittedCandidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          displayName: candidate.displayName,
          party: candidate.party,
          fecIds: candidate.fecIds,
          sources: candidate.sources,
        })),
      },
      input.aiConfig,
      input.aiCandidates
    );

    if (!statusResult.ok) {
      return {
        checkedCount: omittedCandidates.length,
        withdrawnCount: 0,
        activeCount: 0,
        skippedCount: activeCandidates.length - omittedCandidates.length,
        demotedCount: 0,
        dryRun: input.dryRun,
        error: statusResult.reason,
        errorCode: statusResult.errorCode,
      };
    }

    const omittedByCandidateId = new Map(omittedCandidates.map((candidate) => [candidate.candidateId, candidate]));
    let withdrawnCount = 0;
    let activeCount = 0;
    let demotedCount = 0;

    for (const candidateStatus of statusResult.candidates) {
      if (candidateStatus.status === "withdrawn") {
        withdrawnCount += 1;
        const omittedCandidate = omittedByCandidateId.get(candidateStatus.candidate_id);
        if (!input.dryRun && omittedCandidate) {
          const demoteResult = await withdrawPresidentialCycleCandidateByCandidateId({
            db: input.db,
            cycleId: input.cycle.id,
            candidateId: omittedCandidate.candidateId,
          });
          demotedCount += demoteResult.updatedCount;
        }
      } else if (candidateStatus.status === "active") {
        activeCount += 1;
      }
    }

    return {
      checkedCount: statusResult.candidates.length,
      withdrawnCount,
      activeCount,
      skippedCount: activeCandidates.length - omittedCandidates.length,
      demotedCount,
      dryRun: input.dryRun,
      provider: statusResult.provider,
      model: statusResult.model,
    };
  } catch (error) {
    return {
      checkedCount: 0,
      withdrawnCount: 0,
      activeCount: 0,
      skippedCount: 0,
      demotedCount: 0,
      dryRun: input.dryRun,
      error: error instanceof Error ? error.message : String(error),
      errorCode: "STATUS_VERIFICATION_ERROR",
    };
  }
}

export async function enrichPresidentialRosterCycle(
  input: PresidentialRosterEnricherInput
): Promise<PresidentialRosterEnricherResult> {
  const stage = normalizeStage(input.stage);
  const party = normalizeParty(input.party, stage);
  const cycle = await loadPresidentialRosterCycle(input.db, {
    cycleId: input.cycleId,
    electionYear: input.electionYear,
    stage,
    party,
  });
  if (!cycle) {
    return {
      ok: false,
      electionYear: input.electionYear,
      stage,
      party,
      error: `presidential cycle not found for year=${input.electionYear} stage=${stage} party=${party ?? "null"}`,
      retryable: false,
      errorCode: "CYCLE_NOT_FOUND",
    };
  }

  const aiConfig = input.aiConfig ?? buildPresidentialRosterAiConfigFromEnv();
  const enrichRoster = input.enrichRoster ?? enrichPresidentialRoster;
  const aiResult = await enrichRoster(
    {
      cycleId: cycle.id,
      electionYear: cycle.election_year,
      stage: cycle.stage,
      party: cycle.party,
    },
    aiConfig,
    input.aiCandidates
  );

  if (!aiResult.ok) {
    return {
      ok: false,
      cycleId: cycle.id,
      electionYear: cycle.election_year,
      stage: cycle.stage,
      party: cycle.party,
      error: aiResult.reason,
      retryable: aiResult.retryable,
      errorCode: aiResult.errorCode,
      failureDebug: aiResult.failureDebug,
    };
  }

  const matchCandidate = input.matchCandidate ?? matchPresidentialRosterCandidateToFec;
  const fecOptions =
    input.fecOptions ??
    ({
      apiKeys: readOpenFecApiKeysFromEnv(process.env),
      timeoutMs: aiConfig.timeoutMs,
    } satisfies PresidentialCandidateFecMatcherOptions);

  let matchedCount = 0;
  let ambiguousCount = 0;
  let unmatchedCount = 0;
  let withdrawnSkippedCount = 0;
  let withdrawnDemotedCount = 0;
  let preSkippedDraftCount = 0;
  const matches: NonNullable<Extract<PresidentialRosterEnricherResult, { ok: true }>["matches"]> = [];
  const drafts: CandidateProfileDraftEmitInput[] = [];
  const currentActiveMatchedFecIds = new Set<string>();

  for (const [rosterIndex, candidate] of aiResult.candidates.entries()) {
    const match = await matchCandidate({
      electionYear: cycle.election_year,
      expectedParty: cycle.stage === "primary" ? cycle.party : candidate.party,
      candidate,
      options: fecOptions,
    });

    if (candidate.status === "withdrawn") {
      withdrawnSkippedCount += 1;
      const demoteResult =
        !input.dryRun && isExactPresidentialFecMatch(match) && match.matchedFecId
          ? await withdrawPresidentialCycleCandidateByFecId({
              db: input.db,
              cycleId: cycle.id,
              fecCandidateId: match.matchedFecId,
            })
          : { updatedCount: 0 };
      withdrawnDemotedCount += demoteResult.updatedCount;
      matches.push({
        displayName: candidate.display_name,
        status: candidate.status,
        matchStatus: match.matchStatus === "matched" ? "matched" : "skipped_withdrawn",
        method: match.matchStatus === "matched" ? match.method : "skipped_withdrawn",
        admissionStatus: "not_admitted",
        admissionReason:
          match.matchStatus === "matched"
            ? isExactPresidentialFecMatch(match)
              ? demoteResult.updatedCount > 0
                ? "OpenFEC-confirmed withdrawn candidate demoted existing presidential roster link"
                : "OpenFEC-confirmed withdrawn candidate has no existing presidential roster link to demote"
              : "withdrawn candidate matched only fuzzily; existing links are not automatically demoted"
            : "withdrawn candidates are not admitted to the presidential roster in v1",
        ...(match.matchedFecId ? { matchedFecId: match.matchedFecId } : {}),
        reason:
          match.matchStatus === "matched"
            ? isExactPresidentialFecMatch(match)
              ? "withdrawn candidates are not emitted as profile drafts; existing links are demoted when present"
              : "automatic withdrawal requires exact_fec_id or exact_name_party match"
            : match.reason ?? "withdrawn candidate was not OpenFEC-confirmed",
      });
      continue;
    }

    matches.push({
      displayName: candidate.display_name,
      status: candidate.status,
      matchStatus: match.matchStatus,
      method: match.method,
      admissionStatus: match.matchStatus === "matched" ? "admitted" : "not_admitted",
      admissionReason:
        match.matchStatus === "matched"
          ? "OpenFEC candidate match confirmed"
          : match.matchStatus === "ambiguous"
            ? "OpenFEC match was ambiguous; no profile draft emitted"
            : "No OpenFEC match; no profile draft emitted",
      ...(match.matchedFecId ? { matchedFecId: match.matchedFecId } : {}),
      ...(match.reason ? { reason: match.reason } : {}),
    });

    if (match.matchStatus === "matched") {
      matchedCount += 1;
      if (match.matchedFecId) {
        currentActiveMatchedFecIds.add(match.matchedFecId);
      }
      const existing =
        !input.dryRun && match.matchedFecId
          ? await loadExistingPresidentialCycleCandidateByFecId({
              db: input.db,
              cycleId: cycle.id,
              fecCandidateId: match.matchedFecId,
            })
          : null;

      if (existing?.presidentialProfileResearched) {
        preSkippedDraftCount += 1;
      } else {
        drafts.push(
          toDraftInput({
            cycle,
            runId: input.runId ?? null,
            rosterIndex,
            candidate,
            match,
          })
        );
      }

      if (candidate.running_mate && match.matchedFecId) {
        if (
          existing?.runningMateCandidateId &&
          existing.runningMateProfileResearched &&
          runningMateMatchesExisting(candidate.running_mate, existing)
        ) {
          preSkippedDraftCount += 1;
        } else {
          const runningMateDraft = toRunningMateDraftInput({
            cycle,
            runId: input.runId ?? null,
            rosterIndex,
            candidate,
            parentFecCandidateId: match.matchedFecId,
          });
          if (runningMateDraft) {
            drafts.push(runningMateDraft);
          }
        }
      }
      continue;
    }
    if (match.matchStatus === "ambiguous") {
      ambiguousCount += 1;
    } else {
      unmatchedCount += 1;
    }
  }

  const emitResult =
    input.dryRun || drafts.length === 0
      ? { emittedCount: 0, skippedCount: 0 }
      : await enqueueCandidateProfileDrafts(input.redis, drafts);
  const statusVerification = await verifyOmittedActiveCandidates({
    db: input.db,
    cycle,
    dryRun: input.dryRun === true,
    currentActiveFecIds: normalizeFecIdSet(currentActiveMatchedFecIds),
    aiConfig,
    aiCandidates: input.aiCandidates,
    loadActiveCandidatesForReconciliation:
      input.loadActiveCandidatesForReconciliation ?? loadActivePresidentialCycleCandidatesForReconciliation,
    enrichRosterStatus: input.enrichRosterStatus ?? enrichPresidentialRosterStatus,
  });

  return {
    ok: true,
    cycleId: cycle.id,
    electionYear: cycle.election_year,
    stage: cycle.stage,
    party: cycle.party,
    provider: aiResult.provider,
    model: aiResult.model,
    aiCandidateCount: aiResult.candidates.length,
    matchedCount,
    ambiguousCount,
    unmatchedCount,
    withdrawnSkippedCount,
    withdrawnDemotedCount,
    emittedCount: emitResult.emittedCount,
    skippedCount: preSkippedDraftCount + emitResult.skippedCount,
    dryRun: input.dryRun === true,
    admissionPolicy: PRESIDENTIAL_ROSTER_ADMISSION_POLICY,
    statusVerification,
    matches,
    aiRawDebug: aiResult.aiRawDebug,
  };
}

import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildCandidateProfileConfigFromEnv,
  enrichCandidateProfile,
} from "../../ai/enrichCandidateProfile.js";
import { PRESIDENTIAL_PROFILE_AI_CANDIDATES } from "../../ai/aiCandidates.js";
import { resolveIncludePartyForCandidateContest } from "../../ai/candidatePartisanship.js";
import { getPipelineEnv } from "../../config/env.js";
import { isFloridaCampaignFinanceSyncEnabled, isPresidentialElectionsEnabled } from "../../config/featureFlags.js";
import {
  buildCaliforniaCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualCaliforniaCandidateFinanceSyncJob,
} from "../../scheduler/californiaCandidateFinanceSyncScheduler.js";
import { enqueueCandidateLinkCandidateFinanceSyncJob } from "../../scheduler/candidateFinanceSyncScheduler.js";
import {
  buildColoradoCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualColoradoCandidateFinanceSyncJob,
} from "../../scheduler/coloradoCandidateFinanceSyncScheduler.js";
import {
  buildConnecticutCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualConnecticutCandidateFinanceSyncJob,
} from "../../scheduler/connecticutCandidateFinanceSyncScheduler.js";
import {
  buildDistrictOfColumbiaCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualDistrictOfColumbiaCandidateFinanceSyncJob,
} from "../../scheduler/districtOfColumbiaCandidateFinanceSyncScheduler.js";
import {
  buildKentuckyCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualKentuckyCandidateFinanceSyncJob,
} from "../../scheduler/kentuckyCandidateFinanceSyncScheduler.js";
import {
  buildNewMexicoCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualNewMexicoCandidateFinanceSyncJob,
} from "../../scheduler/newMexicoCandidateFinanceSyncScheduler.js";
import {
  buildOklahomaCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualOklahomaCandidateFinanceSyncJob,
} from "../../scheduler/oklahomaCandidateFinanceSyncScheduler.js";
import {
  buildTexasCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualTexasCandidateFinanceSyncJob,
} from "../../scheduler/texasCandidateFinanceSyncScheduler.js";
import {
  buildHawaiiCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualHawaiiCandidateFinanceSyncJob,
} from "../../scheduler/hawaiiCandidateFinanceSyncScheduler.js";
import {
  buildWashingtonCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualWashingtonCandidateFinanceSyncJob,
} from "../../scheduler/washingtonCandidateFinanceSyncScheduler.js";
import {
  buildVirginiaCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualVirginiaCandidateFinanceSyncJob,
} from "../../scheduler/virginiaCandidateFinanceSyncScheduler.js";
import {
  buildWisconsinCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualWisconsinCandidateFinanceSyncJob,
} from "../../scheduler/wisconsinCandidateFinanceSyncScheduler.js";
import {
  enqueueManualMassachusettsCandidateFinanceSyncJob,
} from "../../scheduler/massachusettsCandidateFinanceSyncScheduler.js";
import {
  buildMichiganCandidateFinanceLinkedElectionSyncJobId,
  enqueueManualMichiganCandidateFinanceSyncJob,
} from "../../scheduler/michiganCandidateFinanceSyncScheduler.js";
import type {
  MinnesotaCandidateFinanceSyncEnqueueOptions,
  MinnesotaCandidateFinanceSyncJobData,
} from "../../scheduler/minnesotaCandidateFinanceSyncScheduler.js";
import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
  STAGING_CANDIDATE_PROFILE_REJECTED_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
} from "../../config/electionsPipeline.js";
import { enqueueCandidateRecordDrafts } from "../candidates/candidateRecordDraftEmitter.js";
import {
  normalizeCandidateName,
  splitDisplayNameToFirstLast,
} from "../../utils/candidateIdentity.js";
import {
  AmbiguousCandidateIdentityError,
  findOrCreateCandidateFromProfile,
  hasAtLeastOneHardIdentifier,
} from "../candidates/candidateProfileIdentity.js";
import {
  findPresidentialCycleCandidateIdByFecId,
  findTicketLeadCandidateIdByDisplayName,
  markPresidentialCycleCandidateProfileResearched,
  markPresidentialCycleCandidateRunningMateProfileResearched,
  setCandidateElectionRunningMate,
  setPresidentialCycleCandidateRunningMate,
  upsertCandidateElection,
  upsertPresidentialCycleCandidate,
} from "../candidates/candidateProfileLinks.js";
import { createCandidateFutureElectionNotificationEvents } from "../users/candidateFollowNotificationEvents.js";
import { getPresidentialGeneralElectionDate } from "../presidential/presidentialCycles.js";
import {
  loadPresidentialCycleProfileContext,
  type PresidentialCycleProfileContext,
} from "../presidential/presidentialProfileContext.js";
import { isCaliforniaFinanceEligibleOffice } from "../californiaFinance/californiaFinanceEligibleOffices.js";
import { isColoradoFinanceEligibleOffice } from "../coloradoFinance/coloradoFinanceEligibleOffices.js";
import { isConnecticutFinanceEligibleOffice } from "../connecticutFinance/connecticutFinanceEligibleOffices.js";
import { isDistrictOfColumbiaFinanceEligibleOffice } from "../districtOfColumbiaFinance/districtOfColumbiaFinanceEligibleOffices.js";
import { isNewMexicoFinanceEligibleOffice } from "../newMexicoFinance/newMexicoFinanceEligibleOffices.js";
import { isOklahomaFinanceEligibleOffice } from "../oklahomaFinance/oklahomaFinanceEligibleOffices.js";
import { isTexasFinanceEligibleOffice } from "../texasFinance/texasFinanceEligibleOffices.js";
import { isHawaiiFinanceEligibleOffice } from "../hawaiiFinance/hawaiiFinanceEligibleOffices.js";
import { isWashingtonFinanceEligibleOffice } from "../washingtonFinance/washingtonFinanceEligibleOffices.js";
import { isKentuckyFinanceEligibleOffice } from "../kentuckyFinance/kentuckyFinanceEligibleOffices.js";
import { isVirginiaFinanceEligibleOffice } from "../virginiaFinance/virginiaFinanceEligibleOffices.js";
import { isWisconsinFinanceEligibleOffice } from "../wisconsinFinance/wisconsinFinanceEligibleOffices.js";
import { isMassachusettsFinanceEligibleOffice } from "../massachusettsFinance/massachusettsFinanceEligibleOffices.js";
import { isMichiganFinanceEligibleOffice } from "../michiganFinance/michiganFinanceEligibleOffices.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

const OPTIONAL_FLORIDA_FINANCE_ELIGIBLE_MODULE_PATH = "../floridaFinance/floridaFinanceEligibleOffices.js";
const OPTIONAL_FLORIDA_FINANCE_SYNC_SCHEDULER_MODULE_PATH =
  "../../scheduler/floridaCandidateFinanceSyncScheduler.js";

type OptionalFloridaFinanceEligibleModule = {
  isFloridaFinanceEligibleOffice: (input: {
    officeScope: string | null | undefined;
    officeCanonicalName: string | null | undefined;
  }) => boolean;
};

type OptionalFloridaFinanceSyncSchedulerModule = {
  buildFloridaCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string;
  enqueueManualFloridaCandidateFinanceSyncJob: (
    jobData?: {
      dryRun?: boolean;
      force?: boolean;
      aiClassifyIndustries?: boolean;
      aiClassificationMinAmount?: number;
      triggeredBy?: "manual" | "unknown";
      requestedAt?: string;
    },
    options?: { jobId?: string }
  ) => Promise<string>;
};

type ElectionRow = {
  id: string;
  state: string;
  district_name: string;
  district_type: string;
  election_date: string;
  official_ballot_title: string;
  election_stage: string | null;
  senate_class: string | null;
  term_end_year: string | null;
  is_partisan: boolean | null;
  sources: unknown;
  office_scope: string | null;
  office_canonical_name: string | null;
};

type CandidateProfileDraftContextType = "election" | "presidential_cycle";
type PresidentialProfileDraftRole = "president" | "vice_president";

type CandidateProfileResolvedContext =
  | {
      type: "election";
      contextId: string;
      state: string;
      districtName: string;
      districtType: string;
      electionDate: string;
      officialBallotTitle: string;
      electionStage: string | null;
      senateClass: string | null;
      termEndYear: string | null;
      electionIsPartisan: boolean | null;
      officeScope: string | null;
      officeCanonicalName: string | null;
      includeParty: boolean;
      rosterParty: string | undefined;
      rosterIncumbent: boolean | undefined;
      seedUrls: readonly string[];
    }
  | {
      type: "presidential_cycle";
      contextId: string;
      electionYear: number;
      state: "US";
      districtName: "United States";
      districtType: "presidential";
      electionDate: string | null;
      officialBallotTitle: string;
      electionStage: "primary" | "general";
      senateClass: null;
      termEndYear: null;
      electionIsPartisan: true;
      includeParty: true;
      rosterParty: string;
      rosterIncumbent: undefined;
      seedUrls: readonly string[];
    };

export type CandidateProfileLinkedElectionContext = Extract<
  CandidateProfileResolvedContext,
  { type: "election" }
>;

export type CandidateProfileFinanceSyncFanoutResult = {
  candidateId: string;
  electionId: string;
  state: string;
  officeScope: string | null;
  officeCanonicalName: string | null;
  federalFecCandidateId: string | null;
};

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 8;
const MAX_DELIVERY_ATTEMPTS = 8;
const US_SENATE_FEC_ID_PATTERN = /^S[0-9A-Z]{8}$/;
const US_HOUSE_FEC_ID_PATTERN = /^H[0-9A-Z]{8}$/;
const PRESIDENTIAL_FEC_ID_PATTERN = /^P[0-9A-Z]{8}$/;
const POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;

class ParkCandidateProfileDraftError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ParkCandidateProfileDraftError";
  }
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function parseSeedUrls(raw: unknown): string[] {
  if (typeof raw === "string") {
    try {
      return parseSeedUrls(JSON.parse(raw));
    } catch {
      return [];
    }
  }

  if (!Array.isArray(raw)) {
    return [];
  }

  const urls: string[] = [];
  for (const item of raw) {
    if (typeof item !== "string") {
      continue;
    }
    const trimmed = item.trim();
    if (trimmed.length === 0) {
      continue;
    }
    urls.push(trimmed);
  }

  return [...new Set(urls)].slice(0, MAX_SEED_URLS);
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
      if (merged.length >= MAX_SEED_URLS) {
        return merged;
      }
    }
  }
  return merged;
}

function parseOptionalStringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function parseSerializedStringArray(raw: string | undefined): string[] {
  if (!raw || raw.trim().length === 0) {
    return [];
  }
  try {
    return parseOptionalStringArray(JSON.parse(raw));
  } catch {
    return [];
  }
}

function electionYearFromDateText(value: string): number | null {
  const yearText = value.trim().slice(0, 4);
  if (!/^\d{4}$/.test(yearText)) {
    return null;
  }
  const year = Number(yearText);
  return Number.isInteger(year) ? year : null;
}

function utcDateFromIsoDate(value: string): Date {
  return new Date(`${value}T00:00:00.000Z`);
}

function addUtcDays(date: Date, days: number): Date {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function isWithinPresidentialFinanceSyncWindow(electionYear: number, now = new Date()): boolean {
  const generalElectionDate = utcDateFromIsoDate(getPresidentialGeneralElectionDate(electionYear));
  const deadline = addUtcDays(generalElectionDate, POST_ELECTION_FINANCE_SYNC_GRACE_DAYS);
  return now.toISOString().slice(0, 10) <= deadline.toISOString().slice(0, 10);
}

function normalizeFederalFecIds(values: readonly string[] | undefined): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values ?? []) {
    const id = value.trim().toUpperCase();
    if (!/^[HPS][0-9A-Z]{8}$/.test(id) || seen.has(id)) {
      continue;
    }
    seen.add(id);
    normalized.push(id);
  }
  return normalized;
}

function selectElectionFinanceFecId(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  fecIds: readonly string[] | undefined;
}): string | null {
  const ids = normalizeFederalFecIds(input.fecIds);
  if (
    input.context.officeScope === "statewide" &&
    input.context.officeCanonicalName === "United States Senator"
  ) {
    return ids.find((id) => US_SENATE_FEC_ID_PATTERN.test(id)) ?? null;
  }
  if (
    input.context.officeScope === "us_house" &&
    input.context.officeCanonicalName === "United States Representative"
  ) {
    return ids.find((id) => US_HOUSE_FEC_ID_PATTERN.test(id)) ?? null;
  }
  return null;
}

async function enqueueCandidateFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
  fecIds: readonly string[] | undefined;
}): Promise<void> {
  const electionYear = electionYearFromDateText(input.context.electionDate);
  const fecCandidateId = selectElectionFinanceFecId({
    context: input.context,
    fecIds: input.fecIds,
  });
  if (!electionYear || !fecCandidateId) {
    return;
  }

  try {
    await enqueueCandidateLinkCandidateFinanceSyncJob({
      candidateId: input.candidateId,
      fecCandidateId,
      electionYear,
      includeOutside: true,
      aiClassifyIndustries: true,
    });
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueCaliforniaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "CA" ||
    !isCaliforniaFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualCaliforniaCandidateFinanceSyncJob(
      {
        includeOutside: true,
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildCaliforniaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue California finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueColoradoFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "CO" ||
    !isColoradoFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualColoradoCandidateFinanceSyncJob(
      {
        triggeredBy: "manual",
      },
      {
        jobId: buildColoradoCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Colorado finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueConnecticutFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "CT" ||
    !isConnecticutFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualConnecticutCandidateFinanceSyncJob(
      {
        triggeredBy: "manual",
      },
      {
        jobId: buildConnecticutCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Connecticut finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueDistrictOfColumbiaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "DC" ||
    !isDistrictOfColumbiaFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualDistrictOfColumbiaCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildDistrictOfColumbiaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue D.C. finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueKentuckyFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "KY" ||
    !isKentuckyFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualKentuckyCandidateFinanceSyncJob(
      {
        autoLinkMissingLinks: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildKentuckyCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Kentucky finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueNewMexicoFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "NM" ||
    !isNewMexicoFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualNewMexicoCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildNewMexicoCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue New Mexico finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueOklahomaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "OK" ||
    !isOklahomaFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualOklahomaCandidateFinanceSyncJob(
      {
        triggeredBy: "manual",
      },
      {
        jobId: buildOklahomaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Oklahoma finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueTexasFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "TX" ||
    !isTexasFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualTexasCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildTexasCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Texas finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

function isMissingOptionalFloridaModuleError(error: unknown): boolean {
  const code = typeof error === "object" && error !== null && "code" in error ? String(error.code) : "";
  const message = error instanceof Error ? error.message : String(error);
  return (
    (code === "ERR_MODULE_NOT_FOUND" || code === "MODULE_NOT_FOUND") &&
    (message.includes("floridaFinanceEligibleOffices.js") ||
      message.includes("floridaCandidateFinanceSyncScheduler.js"))
  );
}

async function enqueueFloridaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (input.context.state !== "FL" || !isFloridaCampaignFinanceSyncEnabled()) {
    return;
  }

  try {
    const eligibleModule = (await import(OPTIONAL_FLORIDA_FINANCE_ELIGIBLE_MODULE_PATH)) as OptionalFloridaFinanceEligibleModule;
    if (
      !eligibleModule.isFloridaFinanceEligibleOffice({
        officeScope: input.context.officeScope,
        officeCanonicalName: input.context.officeCanonicalName,
      })
    ) {
      return;
    }

    const schedulerModule = (await import(
      OPTIONAL_FLORIDA_FINANCE_SYNC_SCHEDULER_MODULE_PATH
    )) as OptionalFloridaFinanceSyncSchedulerModule;
    await schedulerModule.enqueueManualFloridaCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: schedulerModule.buildFloridaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    if (isMissingOptionalFloridaModuleError(error)) {
      console.warn(
        `candidate-profile enricher skipped Florida finance sync for candidate=${input.candidateId} election=${input.context.contextId}: optional Florida finance module is unavailable`
      );
      return;
    }
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Florida finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueHawaiiFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "HI" ||
    !isHawaiiFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualHawaiiCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildHawaiiCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Hawaii finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueWashingtonFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "WA" ||
    !isWashingtonFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualWashingtonCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildWashingtonCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Washington finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueVirginiaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "VA" ||
    !isVirginiaFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualVirginiaCandidateFinanceSyncJob(
      {
        triggeredBy: "manual",
      },
      {
        jobId: buildVirginiaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Virginia finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueWisconsinFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "WI" ||
    !isWisconsinFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualWisconsinCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildWisconsinCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Wisconsin finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueMassachusettsFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "MA" ||
    !isMassachusettsFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualMassachusettsCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: `massachusetts-candidate-finance-linked-election-sync-${input.context.contextId}-${input.candidateId}`,
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Massachusetts finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

async function enqueueMichiganFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (
    input.context.state !== "MI" ||
    !isMichiganFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await enqueueManualMichiganCandidateFinanceSyncJob(
      {
        aiClassifyIndustries: true,
        triggeredBy: "manual",
      },
      {
        jobId: buildMichiganCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Michigan finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

type MinnesotaFinanceEnqueueModule = {
  isMinnesotaFinanceEligibleOffice: (input: {
    officeScope: string | null;
    officeCanonicalName: string | null;
  }) => boolean;
  buildMinnesotaCandidateFinanceLinkedElectionSyncJobId: () => string;
  enqueueManualMinnesotaCandidateFinanceSyncJob: (
    jobData?: MinnesotaCandidateFinanceSyncJobData,
    options?: MinnesotaCandidateFinanceSyncEnqueueOptions
  ) => Promise<string>;
};

async function loadMinnesotaFinanceEnqueueModule(): Promise<MinnesotaFinanceEnqueueModule | null> {
  try {
    const [eligibleOfficesModule, schedulerModule] = await Promise.all([
      import("../minnesotaFinance/minnesotaFinanceEligibleOffices.js"),
      import("../../scheduler/minnesotaCandidateFinanceSyncScheduler.js"),
    ]);
    return {
      isMinnesotaFinanceEligibleOffice: eligibleOfficesModule.isMinnesotaFinanceEligibleOffice,
      buildMinnesotaCandidateFinanceLinkedElectionSyncJobId:
        schedulerModule.buildMinnesotaCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualMinnesotaCandidateFinanceSyncJob: schedulerModule.enqueueManualMinnesotaCandidateFinanceSyncJob,
    };
  } catch {
    return null;
  }
}

async function enqueueMinnesotaFinanceSyncForLinkedElection(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "election" }>;
  candidateId: string;
}): Promise<void> {
  if (input.context.state !== "MN") {
    return;
  }

  const minnesotaFinance = await loadMinnesotaFinanceEnqueueModule();
  if (!minnesotaFinance) {
    return;
  }

  if (
    !minnesotaFinance.isMinnesotaFinanceEligibleOffice({
      officeScope: input.context.officeScope,
      officeCanonicalName: input.context.officeCanonicalName,
    })
  ) {
    return;
  }

  try {
    await minnesotaFinance.enqueueManualMinnesotaCandidateFinanceSyncJob(
      {
        triggeredBy: "manual",
      },
      {
        jobId: minnesotaFinance.buildMinnesotaCandidateFinanceLinkedElectionSyncJobId(),
      }
    );
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue Minnesota finance sync for candidate=${input.candidateId} election=${input.context.contextId}: ${reason}`
    );
  }
}

export async function enqueueCandidateProfileFinanceSyncFanoutForLinkedElection(input: {
  context: CandidateProfileLinkedElectionContext;
  candidateId: string;
  fecIds: readonly string[] | undefined;
}): Promise<CandidateProfileFinanceSyncFanoutResult> {
  await enqueueCandidateFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
    fecIds: input.fecIds,
  });
  await enqueueCaliforniaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueColoradoFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueConnecticutFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueDistrictOfColumbiaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueKentuckyFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueNewMexicoFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueOklahomaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueTexasFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueFloridaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueHawaiiFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueWashingtonFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueVirginiaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueWisconsinFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueMassachusettsFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueMichiganFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });
  await enqueueMinnesotaFinanceSyncForLinkedElection({
    context: input.context,
    candidateId: input.candidateId,
  });

  return {
    candidateId: input.candidateId,
    electionId: input.context.contextId,
    state: input.context.state,
    officeScope: input.context.officeScope,
    officeCanonicalName: input.context.officeCanonicalName,
    federalFecCandidateId: selectElectionFinanceFecId({
      context: input.context,
      fecIds: input.fecIds,
    }),
  };
}

async function enqueueCandidateFinanceSyncForPresidentialCycle(input: {
  context: Extract<CandidateProfileResolvedContext, { type: "presidential_cycle" }>;
  presidentialRole: PresidentialProfileDraftRole;
  candidateId: string;
  fecIds: readonly string[] | undefined;
}): Promise<void> {
  if (input.presidentialRole !== "president") {
    return;
  }
  if (!isWithinPresidentialFinanceSyncWindow(input.context.electionYear)) {
    return;
  }

  const fecCandidateId = normalizeFederalFecIds(input.fecIds).find((id) =>
    PRESIDENTIAL_FEC_ID_PATTERN.test(id)
  );
  if (!fecCandidateId) {
    return;
  }

  try {
    await enqueueCandidateLinkCandidateFinanceSyncJob({
      candidateId: input.candidateId,
      fecCandidateId,
      electionYear: input.context.electionYear,
      source: "presidential_cycle",
      includeOutside: true,
      aiClassifyIndustries: true,
    });
  } catch (error) {
    const reason = toReason(error);
    console.warn(
      `candidate-profile enricher could not enqueue presidential finance sync for candidate=${input.candidateId} cycle=${input.context.contextId}: ${reason}`
    );
  }
}

function parseBooleanField(raw: string | undefined): boolean | undefined {
  if (raw === "true") {
    return true;
  }
  if (raw === "false") {
    return false;
  }
  return undefined;
}

function draftContextType(message: Record<string, string>): CandidateProfileDraftContextType {
  return message.context_type === "presidential_cycle" ? "presidential_cycle" : "election";
}

function draftContextId(message: Record<string, string>, contextType: CandidateProfileDraftContextType): string {
  return contextType === "presidential_cycle"
    ? (message.presidential_cycle_id ?? "").trim()
    : (message.election_id ?? "").trim();
}

function draftPresidentialRole(
  message: Record<string, string>,
  contextType: CandidateProfileDraftContextType
): PresidentialProfileDraftRole | null {
  if (contextType !== "presidential_cycle") {
    return null;
  }
  const role = (message.presidential_role ?? "").trim();
  if (role.length === 0) {
    return "president";
  }
  return role === "president" || role === "vice_president" ? role : null;
}

function officialBallotTitleForPresidentialRole(
  context: Extract<CandidateProfileResolvedContext, { type: "presidential_cycle" }>,
  role: PresidentialProfileDraftRole
): string {
  if (role === "president") {
    return context.officialBallotTitle;
  }
  const year = context.electionDate ? new Date(context.electionDate).getUTCFullYear() : null;
  const cycleLabel = context.officialBallotTitle.replace(/^President of the United States,\s*/i, "");
  return `Vice President of the United States, ${cycleLabel || (year ? `${year} presidential cycle` : "presidential cycle")}`;
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
      "0",
      { MKSTREAM: true }
    );
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function reclaimPendingEntries(
  redis: ReturnType<typeof createClient>,
  consumerName: string,
  batchSize: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
      consumerName,
      RECLAIM_MIN_IDLE_MS,
      cursor,
      { COUNT: batchSize }
    );
    cursor = claim.nextId;
    if (!claim.messages || claim.messages.length === 0) {
      break;
    }

    reclaimed.push(
      ...claim.messages
        .filter((entry): entry is NonNullable<typeof entry> => entry !== null)
        .map((entry) => ({ id: entry.id, message: entry.message as Record<string, string> }))
    );
  }

  return reclaimed;
}

async function getDeliveryCount(
  redis: ReturnType<typeof createClient>,
  messageId: string
): Promise<number | null> {
  try {
    const raw = await redis.sendCommand([
      "XPENDING",
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
      messageId,
      messageId,
      "1",
    ]);
    if (!Array.isArray(raw) || raw.length === 0) {
      return null;
    }
    const first = raw[0];
    if (!Array.isArray(first) || first.length < 4) {
      return null;
    }
    const deliveriesValue = first[3];
    const deliveries =
      typeof deliveriesValue === "number"
        ? deliveriesValue
        : Number.parseInt(String(deliveriesValue), 10);
    return Number.isFinite(deliveries) ? deliveries : null;
  } catch {
    return null;
  }
}

async function parkMessage(
  redis: ReturnType<typeof createClient>,
  entry: { id: string; message: Record<string, string> },
  reason: string,
  deliveryCount: number | null
): Promise<void> {
  await redis.xAdd(STAGING_CANDIDATE_PROFILE_REJECTED_STREAM, "*", {
    reason,
    delivery_count: deliveryCount === null ? "" : String(deliveryCount),
    original_stream_id: entry.id,
    election_id: entry.message.election_id ?? "",
    context_type: entry.message.context_type ?? "",
    presidential_cycle_id: entry.message.presidential_cycle_id ?? "",
    presidential_role: entry.message.presidential_role ?? "",
    parent_presidential_candidate_fec_id: entry.message.parent_presidential_candidate_fec_id ?? "",
    candidate_display_name: entry.message.candidate_display_name ?? "",
    item_type: entry.message.item_type ?? "",
    run_id: entry.message.run_id ?? "",
  });
  await redis.xAck(STAGING_CANDIDATE_PROFILE_DRAFT_STREAM, STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP, entry.id);
}

async function getElectionRow(pool: Pool, electionId: string): Promise<ElectionRow | null> {
  const result = await pool.query<ElectionRow>(
    `
      SELECT
        e.id,
        d.state,
        d.name AS district_name,
        d.district_type,
        e.election_date::text AS election_date,
        e.official_ballot_title,
        e.election_stage::text AS election_stage,
        sm.senate_class,
        sm.term_end_year,
        e.is_partisan,
        e.sources,
        office.scope AS office_scope,
        office.canonical_name AS office_canonical_name
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.offices AS office
        ON office.id = e.office_id
      LEFT JOIN public.election_senate_metadata AS sm
        ON sm.election_id = e.id
      WHERE e.id = $1
        AND e.race_type = 'office'
      LIMIT 1
    `,
    [electionId]
  );

  return result.rows[0] ?? null;
}

// Idempotency check only: skip a redelivered draft when THIS mate is already
// linked to THIS lead. A draft carrying a different mate name must pass through
// so a re-imported roster with a replacement running mate overwrites the link
// (last write wins by design).
async function electionTicketAlreadyLinksRunningMate(
  pool: Pool,
  electionId: string,
  leadDisplayName: string,
  mateDisplayName: string
): Promise<boolean> {
  const result = await pool.query(
    `
      SELECT 1
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS lead
        ON lead.id = ce.candidate_id
      JOIN public.candidates AS rm
        ON rm.id = ce.running_mate_candidate_id
        AND rm.deleted_at IS NULL
      WHERE ce.election_id = $1
        AND lead.deleted_at IS NULL
        AND lower(trim(coalesce(lead.display_name, lead.first_name || ' ' || lead.last_name))) = lower(trim($2))
        AND lower(trim(coalesce(rm.display_name, rm.first_name || ' ' || rm.last_name))) = lower(trim($3))
      LIMIT 1
    `,
    [electionId, leadDisplayName, mateDisplayName]
  );
  return (result.rowCount ?? 0) > 0;
}

async function findElectionLinkedCandidateByName(
  pool: Pool,
  electionId: string,
  displayName: string
): Promise<{ candidateId: string; fecIds: string[] } | null> {
  const incomingName = splitDisplayNameToFirstLast(displayName);
  const incomingFirstLast = normalizeCandidateName(`${incomingName.firstName} ${incomingName.lastName}`);
  if (incomingFirstLast.length === 0) {
    return null;
  }

  const result = await pool.query<{
    id: string;
    first_name: string;
    last_name: string;
    fec_ids: unknown;
  }>(
    `
      SELECT c.id, c.first_name, c.last_name, c.fec_ids
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = $1
        AND c.deleted_at IS NULL
    `,
    [electionId]
  );

  const matches = result.rows.filter(
    (row) => normalizeCandidateName(`${row.first_name} ${row.last_name}`) === incomingFirstLast
  );
  if (matches.length === 0) {
    return null;
  }
  // Two same-name candidates linked to one election means duplicate rows
  // already exist; replaying the fanout against an arbitrary one would
  // attach records/finance to the wrong duplicate. Park for operator merge
  // (mirrors the identity layer's ambiguity guards).
  if (matches.length > 1) {
    throw new ParkCandidateProfileDraftError(
      `multiple candidates named "${displayName}" are linked to election ${electionId}; merge the duplicate rows before this draft can be retried`
    );
  }
  const match = matches[0]!;
  const fecIds = Array.isArray(match.fec_ids)
    ? match.fec_ids.filter((value): value is string => typeof value === "string")
    : [];
  return { candidateId: match.id, fecIds };
}

function effectivePresidentialParty(
  rosterParty: string | undefined,
  context: PresidentialCycleProfileContext
): string | undefined {
  const cycleParty = context.party?.trim();
  if (context.stage === "primary") {
    return cycleParty && cycleParty.length > 0 ? cycleParty : undefined;
  }
  return rosterParty ?? (cycleParty && cycleParty.length > 0 ? cycleParty : undefined);
}

async function resolveElectionDraftContext(input: {
  pool: Pool;
  electionId: string;
  rosterParty: string | undefined;
  rosterIncumbent: boolean | undefined;
  messageSeedUrls: readonly string[];
}): Promise<CandidateProfileResolvedContext | null> {
  const election = await getElectionRow(input.pool, input.electionId);
  if (!election) {
    return null;
  }

  const includeParty = resolveIncludePartyForCandidateContest({
    districtType: election.district_type,
    state: election.state,
    officialBallotTitle: election.official_ballot_title,
    electionIsPartisan: election.is_partisan,
  });
  const rosterParty = includeParty ? input.rosterParty : undefined;

  return {
    type: "election",
    contextId: input.electionId,
    state: election.state,
    districtName: election.district_name,
    districtType: election.district_type,
    electionDate: election.election_date,
    officialBallotTitle: election.official_ballot_title,
    electionStage: election.election_stage,
    senateClass: election.senate_class,
    termEndYear: election.term_end_year,
    electionIsPartisan: election.is_partisan,
    officeScope: election.office_scope,
    officeCanonicalName: election.office_canonical_name,
    includeParty,
    rosterParty,
    rosterIncumbent: input.rosterIncumbent,
    seedUrls: mergeSeedUrls(input.messageSeedUrls, parseSeedUrls(election.sources)),
  };
}

async function resolvePresidentialCycleDraftContext(input: {
  pool: Pool;
  presidentialCycleId: string;
  rosterParty: string | undefined;
  messageSeedUrls: readonly string[];
}): Promise<CandidateProfileResolvedContext | null> {
  const context = await loadPresidentialCycleProfileContext(input.pool, input.presidentialCycleId);
  if (!context) {
    return null;
  }
  const rosterParty = effectivePresidentialParty(input.rosterParty, context);
  if (!rosterParty) {
    throw new Error(`presidential cycle profile draft is missing party for cycle ${input.presidentialCycleId}`);
  }

  return {
    type: "presidential_cycle",
    contextId: context.cycleId,
    electionYear: context.electionYear,
    state: context.state,
    districtName: context.districtName,
    districtType: context.districtType,
    electionDate: context.electionDate,
    officialBallotTitle: context.officialBallotTitle,
    electionStage: context.electionStage,
    senateClass: null,
    termEndYear: null,
    electionIsPartisan: context.electionIsPartisan,
    includeParty: true,
    rosterParty,
    rosterIncumbent: undefined,
    seedUrls: mergeSeedUrls(input.messageSeedUrls, context.seedUrls),
  };
}

export async function runCandidateProfileEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `candidate_profile_enricher_${process.pid}_${Date.now()}`;
  const aiConfig = buildCandidateProfileConfigFromEnv();

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const contextType = draftContextType(entry.message);
        const contextId = draftContextId(entry.message, contextType);
        const presidentialRole = draftPresidentialRole(entry.message, contextType);
        const contextLabel =
          contextType === "presidential_cycle"
            ? `presidential_cycle_id=${contextId || "unknown"}`
            : `election_id=${contextId || "unknown"}`;
        const itemType = entry.message.item_type;
        const candidateDisplayName = entry.message.candidate_display_name;
        const runId = entry.message.run_id ?? null;
        const disambiguationHint = entry.message.disambiguation_hint?.trim() || undefined;
        const skipPerElectionNameDedupe = parseBooleanField(entry.message.skip_per_election_name_dedupe) === true;
        const rosterFecIds = parseSerializedStringArray(entry.message.roster_fec_ids);
        const rosterStateFilingIds = parseSerializedStringArray(entry.message.roster_state_filing_ids);
        const parentPresidentialCandidateFecId =
          entry.message.parent_presidential_candidate_fec_id?.trim().toUpperCase() || undefined;
        const rawElectionTicketRole = entry.message.election_ticket_role?.trim() || undefined;
        const electionTicketRole = rawElectionTicketRole === "running_mate" ? ("running_mate" as const) : undefined;
        const ticketLeadDisplayName = entry.message.ticket_lead_display_name?.trim() || undefined;
        let deliveryCount: number | null = null;
        const presidentialDisabled =
          contextType === "presidential_cycle" && !isPresidentialElectionsEnabled();

        try {
          if (presidentialDisabled) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            console.log(
              `candidate-profile enricher skipped disabled presidential draft ${contextLabel} candidate=${candidateDisplayName ?? "unknown"}`
            );
            continue;
          }

          deliveryCount = await getDeliveryCount(redis, entry.id);
          if (deliveryCount !== null && deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            await parkMessage(
              redis,
              entry,
              `max delivery attempts exceeded (${MAX_DELIVERY_ATTEMPTS})`,
              deliveryCount
            );
            console.warn(
              `candidate-profile enricher parked stream_id=${entry.id} ${contextLabel} candidate=${candidateDisplayName ?? "unknown"} after ${deliveryCount} deliveries`
            );
            continue;
          }

          if (contextType === "presidential_cycle" && presidentialRole === null) {
            await parkMessage(redis, entry, "invalid presidential_role for presidential profile draft", deliveryCount);
            continue;
          }

          if (
            !contextId ||
            !candidateDisplayName ||
            itemType !== STAGING_ITEM_TYPE_CANDIDATE_PROFILE
          ) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          if (contextType === "presidential_cycle" && presidentialRole === "president" && rosterFecIds.length === 0) {
            await parkMessage(
              redis,
              entry,
              "president profile draft requires at least one roster_fec_ids value",
              deliveryCount
            );
            continue;
          }

          if (
            contextType === "presidential_cycle" &&
            presidentialRole === "vice_president" &&
            !parentPresidentialCandidateFecId
          ) {
            await parkMessage(
              redis,
              entry,
              "vice president profile draft requires parent_presidential_candidate_fec_id",
              deliveryCount
            );
            continue;
          }

          if (contextType === "election" && rawElectionTicketRole && !electionTicketRole) {
            await parkMessage(
              redis,
              entry,
              `invalid election_ticket_role "${rawElectionTicketRole}" for election profile draft`,
              deliveryCount
            );
            continue;
          }

          if (contextType === "election" && electionTicketRole === "running_mate" && !ticketLeadDisplayName) {
            await parkMessage(
              redis,
              entry,
              "running mate profile draft requires ticket_lead_display_name",
              deliveryCount
            );
            continue;
          }

          if (
            contextType === "election" &&
            electionTicketRole === "running_mate" &&
            ticketLeadDisplayName &&
            (await electionTicketAlreadyLinksRunningMate(pool, contextId, ticketLeadDisplayName, candidateDisplayName))
          ) {
            // Unlike the linked-candidate gate below, there is no fanout to
            // replay here: running mates deliberately get no finance sync and
            // no record drafts for the ticket election (see the post-commit
            // fanout guard), so acking a redelivered mate loses nothing.
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const rosterParty = entry.message.roster_party?.trim() || undefined;
          const rosterIncumbent = parseBooleanField(entry.message.roster_is_incumbent);
          const messageSeedUrls = parseSeedUrls(entry.message.seed_urls);

          if (contextType === "election" && electionTicketRole !== "running_mate" && !skipPerElectionNameDedupe) {
            const linkedCandidate = await findElectionLinkedCandidateByName(pool, contextId, candidateDisplayName);
            if (linkedCandidate) {
              // The candidate write committed on an earlier delivery, but the
              // post-commit fanout (finance sync + record drafts) may have
              // failed before the ack — the fanout is not part of the
              // transaction. Acking here without replaying it would lose the
              // candidate's record research permanently (record rollover is
              // flag-gated) and delay finance a day, so replay from persisted
              // state. A redundant replay is cheap: record drafts dedupe on
              // their 24h emit marker and the finance sync overwrites the
              // same candidate/cycle rows it would have written anyway.
              const linkedContext = await resolveElectionDraftContext({
                pool,
                electionId: contextId,
                rosterParty,
                rosterIncumbent,
                messageSeedUrls,
              });
              if (linkedContext && linkedContext.type === "election") {
                await enqueueCandidateProfileFinanceSyncFanoutForLinkedElection({
                  context: linkedContext,
                  candidateId: linkedCandidate.candidateId,
                  fecIds: linkedCandidate.fecIds,
                });
                await enqueueCandidateRecordDrafts(redis, [
                  {
                    candidateId: linkedCandidate.candidateId,
                    electionId: contextId,
                    runId,
                  },
                ]);
              }
              await redis.xAck(
                STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
                STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
                entry.id
              );
              continue;
            }
          }
          const draftContext =
            contextType === "presidential_cycle"
              ? await resolvePresidentialCycleDraftContext({
                  pool,
                  presidentialCycleId: contextId,
                  rosterParty,
                  messageSeedUrls,
                })
              : await resolveElectionDraftContext({
                  pool,
                  electionId: contextId,
                  rosterParty,
                  rosterIncumbent,
                  messageSeedUrls,
                });
          if (!draftContext) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const profileInput = {
            candidateDisplayName,
            districtName: draftContext.districtName,
            districtType: draftContext.districtType,
            state: draftContext.state,
            electionDate: draftContext.electionDate,
            officialBallotTitle:
              draftContext.type === "presidential_cycle"
                ? officialBallotTitleForPresidentialRole(draftContext, presidentialRole ?? "president")
                : draftContext.officialBallotTitle,
            electionStage: draftContext.electionStage,
            senateClass: draftContext.senateClass,
            termEndYear: draftContext.termEndYear,
            electionIsPartisan: draftContext.electionIsPartisan,
            rosterParty: draftContext.rosterParty,
            rosterIncumbent: draftContext.rosterIncumbent,
            rosterFecIds,
            rosterStateFilingIds,
            disambiguationHint,
            seedUrls: draftContext.seedUrls,
            allowMissingFederalFecIds:
              draftContext.type === "presidential_cycle" && presidentialRole === "vice_president",
          };
          const aiResult =
            draftContext.type === "presidential_cycle"
              ? await enrichCandidateProfile(profileInput, aiConfig, PRESIDENTIAL_PROFILE_AI_CANDIDATES)
              : await enrichCandidateProfile(profileInput, aiConfig);

          if (!aiResult.ok) {
            console.warn(
              `candidate-profile enricher retrying ${contextLabel} candidate=${candidateDisplayName}: ${aiResult.errorCode} ${aiResult.reason}`
            );
            // Leave unacked so reclaim retries.
            continue;
          }

          const profile = aiResult.profile;
          if (skipPerElectionNameDedupe && !hasAtLeastOneHardIdentifier(profile)) {
            await parkMessage(
              redis,
              entry,
              "duplicate-name candidate profile lacks hard identifiers; skipped to avoid mismatched person write",
              deliveryCount
            );
            continue;
          }

          const client = await pool.connect();
          let candidateId: string;
          try {
            await client.query("BEGIN");

            const candidateResult = await findOrCreateCandidateFromProfile({
              client,
              profile,
              state: draftContext.state,
              rosterParty: draftContext.rosterParty,
              includeParty: draftContext.includeParty,
              allowCrossStateHardIdentifierMatch: draftContext.type === "presidential_cycle",
            });
            candidateId = candidateResult.candidateId;

            if (draftContext.type === "presidential_cycle") {
              if (presidentialRole === "vice_president") {
                const parentCandidateId = await findPresidentialCycleCandidateIdByFecId({
                  db: client,
                  cycleId: draftContext.contextId,
                  fecCandidateId: parentPresidentialCandidateFecId ?? "",
                });
                if (!parentCandidateId) {
                  throw new Error(
                    `parent presidential cycle candidate not found for FEC ID ${parentPresidentialCandidateFecId ?? ""}`
                  );
                }
                if (parentCandidateId === candidateId) {
                  throw new ParkCandidateProfileDraftError(
                    `vice president profile resolved to the parent presidential candidate for FEC ID ${parentPresidentialCandidateFecId ?? ""}`
                  );
                }
                await setPresidentialCycleCandidateRunningMate({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId: parentCandidateId,
                  runningMateCandidateId: candidateId,
                });
                await markPresidentialCycleCandidateRunningMateProfileResearched({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId: parentCandidateId,
                  runningMateCandidateId: candidateId,
                });
              } else {
                await upsertPresidentialCycleCandidate({
                  client,
                  cycleId: draftContext.contextId,
                  candidateId,
                  party: draftContext.rosterParty,
                  sources: profile.sources,
                });
                await markPresidentialCycleCandidateProfileResearched({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId,
                });
              }
            } else if (electionTicketRole === "running_mate") {
              const lead = await findTicketLeadCandidateIdByDisplayName({
                db: client,
                electionId: draftContext.contextId,
                leadDisplayName: ticketLeadDisplayName ?? "",
              });
              if (!lead.ok) {
                if (lead.reason === "ambiguous") {
                  throw new ParkCandidateProfileDraftError(
                    `multiple ticket lead candidates match display_name "${ticketLeadDisplayName ?? ""}" for this election`
                  );
                }
                // Lead profile not written yet; leave unacked so reclaim retries
                // after the lead draft lands.
                throw new Error(
                  `ticket lead candidate not found for display_name "${ticketLeadDisplayName ?? ""}" in this election`
                );
              }
              if (lead.candidateId === candidateId) {
                throw new ParkCandidateProfileDraftError(
                  `running mate profile resolved to the ticket lead candidate for "${ticketLeadDisplayName ?? ""}"`
                );
              }
              // Last write wins by design: a re-imported roster with a
              // replacement running mate must overwrite the previous link.
              // Emit markers guarantee at most one mate draft per ticket per
              // roster version, so concurrent different-mate writes for one
              // lead cannot be produced by the pipeline.
              await setCandidateElectionRunningMate({
                db: client,
                electionId: draftContext.contextId,
                candidateId: lead.candidateId,
                runningMateCandidateId: candidateId,
              });
            } else {
              const linkResult = await upsertCandidateElection({
                client,
                candidateId,
                electionId: draftContext.contextId,
                isIncumbent: draftContext.rosterIncumbent,
              });
              if (linkResult.created) {
                await createCandidateFutureElectionNotificationEvents(client, {
                  candidateId,
                  electionId: draftContext.contextId,
                });
              }
            }
            await client.query("COMMIT");
          } catch (error) {
            await client.query("ROLLBACK");
            throw error;
          } finally {
            client.release();
          }

          if (draftContext.type === "election") {
            // Running mates are not ballot candidates for this election: no
            // finance sync fanout and no record drafts for the ticket election.
            if (electionTicketRole !== "running_mate") {
              await enqueueCandidateProfileFinanceSyncFanoutForLinkedElection({
                context: draftContext,
                candidateId,
                fecIds: profile.fec_ids,
              });
              await enqueueCandidateRecordDrafts(redis, [
                {
                  candidateId,
                  electionId: draftContext.contextId,
                  runId,
                },
              ]);
            }
          } else {
            await enqueueCandidateFinanceSyncForPresidentialCycle({
              context: draftContext,
              presidentialRole: presidentialRole ?? "president",
              candidateId,
              fecIds: [...(profile.fec_ids ?? []), ...rosterFecIds],
            });
            await enqueueCandidateRecordDrafts(redis, [
              {
                contextType: "presidential_cycle",
                candidateId,
                presidentialCycleId: draftContext.contextId,
                presidentialRole: presidentialRole ?? "president",
                runId,
              },
            ]);
          }

          await redis.xAck(
            STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
            STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
            entry.id
          );
        } catch (error) {
          const reason = toReason(error);
          if (error instanceof ParkCandidateProfileDraftError || error instanceof AmbiguousCandidateIdentityError) {
            // Ambiguous identity means duplicate candidate rows already
            // exist; retrying cannot resolve it — park for operator merge.
            await parkMessage(redis, entry, reason, deliveryCount);
            continue;
          }
          console.warn(
            `candidate-profile enricher retrying ${contextLabel} candidate=${candidateDisplayName ?? "unknown"}: ${reason}`
          );
          // Leave unacked so reclaim retries.
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_CANDIDATE_PROFILE_DRAFT_STREAM, id: ">" }],
        { COUNT: batchSize, BLOCK: blockMs }
      );

      if (!batches || batches.length === 0) {
        if (once) {
          break;
        }
        continue;
      }

      for (const batch of batches) {
        await handleEntries(
          batch.messages.map((message) => ({
            id: message.id,
            message: message.message as Record<string, string>,
          }))
        );
      }

      if (once) {
        break;
      }
    } while (true);
  } finally {
    try {
      await redis.quit();
    } catch (error) {
      console.error("candidate-profile enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("candidate-profile enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}

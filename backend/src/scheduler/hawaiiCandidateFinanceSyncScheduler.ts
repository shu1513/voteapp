import type { Queue, Worker } from "bullmq";

import {
  isHawaiiCampaignFinanceEnabled,
  isHawaiiCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueHawaiiCandidateFinance,
  type HawaiiCandidateFinanceBatchSyncResult,
} from "../pipeline/hawaiiFinance/hawaiiCandidateFinanceBatchSync.js";
import {
  createStateCandidateFinanceSyncScheduler,
  type StateCandidateFinanceSyncEnqueueOptions,
  type StateCandidateFinanceSyncJobData,
} from "./stateCandidateFinanceSyncScheduler.js";

export const HAWAII_CANDIDATE_FINANCE_SYNC_JOB_NAME = "hawaii_candidate_finance_sync_due";
export const HAWAII_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "hawaii_candidate_finance_sync_daily";

export type HawaiiCandidateFinanceSyncJobData = StateCandidateFinanceSyncJobData;

export type HawaiiCandidateFinanceSyncJobResult = HawaiiCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<HawaiiCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type HawaiiCandidateFinanceSyncEnqueueOptions = StateCandidateFinanceSyncEnqueueOptions;

const scheduler = createStateCandidateFinanceSyncScheduler({
  stateLabel: "Hawaii",
  jobName: HAWAII_CANDIDATE_FINANCE_SYNC_JOB_NAME,
  dailySchedulerId: HAWAII_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
  defaultQueueName: "hawaii_candidate_finance_sync_maintenance",
  linkedElectionJobIdPrefix: "hawaii-candidate-finance-linked-election-sync-",
  envPrefix: "HAWAII_CAMPAIGN_FINANCE_SYNC",
  defaultDailyCron: "25 9 * * *",
  isEnabled: isHawaiiCampaignFinanceEnabled,
  isSyncEnabled: isHawaiiCampaignFinanceSyncEnabled,
  syncDue: syncDueHawaiiCandidateFinance,
});

export const createHawaiiCandidateFinanceSyncSchedulerQueue: () => Queue<HawaiiCandidateFinanceSyncJobData> =
  scheduler.createQueue;

export const buildHawaiiCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string =
  scheduler.buildLinkedElectionSyncJobId;

export const upsertRecurringHawaiiCandidateFinanceSyncJobs: (
  jobData?: HawaiiCandidateFinanceSyncJobData
) => Promise<void> = scheduler.upsertRecurringJobs;

export const enqueueManualHawaiiCandidateFinanceSyncJob: (
  jobData?: HawaiiCandidateFinanceSyncJobData,
  options?: HawaiiCandidateFinanceSyncEnqueueOptions
) => Promise<string> = scheduler.enqueueManualJob;

export const runHawaiiCandidateFinanceSyncJob: (
  data?: HawaiiCandidateFinanceSyncJobData
) => Promise<HawaiiCandidateFinanceSyncJobResult> = scheduler.runJob;

export const createHawaiiCandidateFinanceSyncSchedulerWorker: () => Worker<
  HawaiiCandidateFinanceSyncJobData,
  HawaiiCandidateFinanceSyncJobResult
> = scheduler.createWorker;

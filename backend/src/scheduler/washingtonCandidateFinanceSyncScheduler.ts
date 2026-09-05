import type { Queue, Worker } from "bullmq";

import {
  isWashingtonCampaignFinanceEnabled,
  isWashingtonCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueWashingtonCandidateFinance,
  type WashingtonCandidateFinanceBatchSyncResult,
} from "../pipeline/washingtonFinance/washingtonCandidateFinanceBatchSync.js";
import {
  createStateCandidateFinanceSyncScheduler,
  type StateCandidateFinanceSyncEnqueueOptions,
  type StateCandidateFinanceSyncJobData,
} from "./stateCandidateFinanceSyncScheduler.js";

export const WASHINGTON_CANDIDATE_FINANCE_SYNC_JOB_NAME = "washington_candidate_finance_sync_due";
export const WASHINGTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "washington_candidate_finance_sync_daily";

export type WashingtonCandidateFinanceSyncJobData = StateCandidateFinanceSyncJobData;

export type WashingtonCandidateFinanceSyncJobResult = WashingtonCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<WashingtonCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type WashingtonCandidateFinanceSyncEnqueueOptions = StateCandidateFinanceSyncEnqueueOptions;

const scheduler = createStateCandidateFinanceSyncScheduler({
  stateLabel: "Washington",
  jobName: WASHINGTON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
  dailySchedulerId: WASHINGTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
  defaultQueueName: "washington_candidate_finance_sync_maintenance",
  linkedElectionJobIdPrefix: "washington-candidate-finance-linked-election-sync-",
  envPrefix: "WASHINGTON_CAMPAIGN_FINANCE_SYNC",
  defaultDailyCron: "25 9 * * *",
  isEnabled: isWashingtonCampaignFinanceEnabled,
  isSyncEnabled: isWashingtonCampaignFinanceSyncEnabled,
  syncDue: syncDueWashingtonCandidateFinance,
});

export const createWashingtonCandidateFinanceSyncSchedulerQueue: () => Queue<WashingtonCandidateFinanceSyncJobData> =
  scheduler.createQueue;

export const buildWashingtonCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string =
  scheduler.buildLinkedElectionSyncJobId;

export const upsertRecurringWashingtonCandidateFinanceSyncJobs: (
  jobData?: WashingtonCandidateFinanceSyncJobData
) => Promise<void> = scheduler.upsertRecurringJobs;

export const enqueueManualWashingtonCandidateFinanceSyncJob: (
  jobData?: WashingtonCandidateFinanceSyncJobData,
  options?: WashingtonCandidateFinanceSyncEnqueueOptions
) => Promise<string> = scheduler.enqueueManualJob;

export const runWashingtonCandidateFinanceSyncJob: (
  data?: WashingtonCandidateFinanceSyncJobData
) => Promise<WashingtonCandidateFinanceSyncJobResult> = scheduler.runJob;

export const createWashingtonCandidateFinanceSyncSchedulerWorker: () => Worker<
  WashingtonCandidateFinanceSyncJobData,
  WashingtonCandidateFinanceSyncJobResult
> = scheduler.createWorker;

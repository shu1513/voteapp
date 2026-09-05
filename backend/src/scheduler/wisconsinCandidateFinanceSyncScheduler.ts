import type { Queue, Worker } from "bullmq";

import {
  isWisconsinCampaignFinanceEnabled,
  isWisconsinCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueWisconsinCandidateFinance,
  type WisconsinCandidateFinanceBatchSyncResult,
} from "../pipeline/wisconsinFinance/wisconsinCandidateFinanceBatchSync.js";
import {
  createStateCandidateFinanceSyncScheduler,
  type StateCandidateFinanceSyncEnqueueOptions,
  type StateCandidateFinanceSyncJobData,
} from "./stateCandidateFinanceSyncScheduler.js";

export const WISCONSIN_CANDIDATE_FINANCE_SYNC_JOB_NAME = "wisconsin_candidate_finance_sync_due";
export const WISCONSIN_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "wisconsin_candidate_finance_sync_daily";

export type WisconsinCandidateFinanceSyncJobData = StateCandidateFinanceSyncJobData;

export type WisconsinCandidateFinanceSyncJobResult = WisconsinCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<WisconsinCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type WisconsinCandidateFinanceSyncEnqueueOptions = StateCandidateFinanceSyncEnqueueOptions;

const scheduler = createStateCandidateFinanceSyncScheduler({
  stateLabel: "Wisconsin",
  jobName: WISCONSIN_CANDIDATE_FINANCE_SYNC_JOB_NAME,
  dailySchedulerId: WISCONSIN_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
  defaultQueueName: "wisconsin_candidate_finance_sync_maintenance",
  linkedElectionJobIdPrefix: "wisconsin-candidate-finance-linked-election-sync-",
  envPrefix: "WISCONSIN_CAMPAIGN_FINANCE_SYNC",
  defaultDailyCron: "25 9 * * *",
  isEnabled: isWisconsinCampaignFinanceEnabled,
  isSyncEnabled: isWisconsinCampaignFinanceSyncEnabled,
  syncDue: syncDueWisconsinCandidateFinance,
});

export const createWisconsinCandidateFinanceSyncSchedulerQueue: () => Queue<WisconsinCandidateFinanceSyncJobData> =
  scheduler.createQueue;

export const buildWisconsinCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string =
  scheduler.buildLinkedElectionSyncJobId;

export const upsertRecurringWisconsinCandidateFinanceSyncJobs: (
  jobData?: WisconsinCandidateFinanceSyncJobData
) => Promise<void> = scheduler.upsertRecurringJobs;

export const enqueueManualWisconsinCandidateFinanceSyncJob: (
  jobData?: WisconsinCandidateFinanceSyncJobData,
  options?: WisconsinCandidateFinanceSyncEnqueueOptions
) => Promise<string> = scheduler.enqueueManualJob;

export const runWisconsinCandidateFinanceSyncJob: (
  data?: WisconsinCandidateFinanceSyncJobData
) => Promise<WisconsinCandidateFinanceSyncJobResult> = scheduler.runJob;

export const createWisconsinCandidateFinanceSyncSchedulerWorker: () => Worker<
  WisconsinCandidateFinanceSyncJobData,
  WisconsinCandidateFinanceSyncJobResult
> = scheduler.createWorker;

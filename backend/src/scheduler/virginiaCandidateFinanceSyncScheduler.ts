import type { Queue, Worker } from "bullmq";

import {
  isVirginiaCampaignFinanceEnabled,
  isVirginiaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueVirginiaCandidateFinance,
  type VirginiaCandidateFinanceBatchSyncResult,
} from "../pipeline/virginiaFinance/virginiaCandidateFinanceBatchSync.js";
import {
  createStateCandidateFinanceSyncScheduler,
  type StateCandidateFinanceSyncEnqueueOptions,
  type StateCandidateFinanceSyncJobData,
} from "./stateCandidateFinanceSyncScheduler.js";

export const VIRGINIA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "virginia_candidate_finance_sync_due";
export const VIRGINIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "virginia_candidate_finance_sync_daily";

export type VirginiaCandidateFinanceSyncJobData = StateCandidateFinanceSyncJobData;

export type VirginiaCandidateFinanceSyncJobResult = VirginiaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<VirginiaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type VirginiaCandidateFinanceSyncEnqueueOptions = StateCandidateFinanceSyncEnqueueOptions;

const scheduler = createStateCandidateFinanceSyncScheduler({
  stateLabel: "Virginia",
  jobName: VIRGINIA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
  dailySchedulerId: VIRGINIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
  defaultQueueName: "virginia_candidate_finance_sync_maintenance",
  linkedElectionJobIdPrefix: "virginia-candidate-finance-linked-election-sync-",
  envPrefix: "VIRGINIA_CAMPAIGN_FINANCE_SYNC",
  defaultDailyCron: "25 9 * * *",
  isEnabled: isVirginiaCampaignFinanceEnabled,
  isSyncEnabled: isVirginiaCampaignFinanceSyncEnabled,
  syncDue: syncDueVirginiaCandidateFinance,
});

export const createVirginiaCandidateFinanceSyncSchedulerQueue: () => Queue<VirginiaCandidateFinanceSyncJobData> =
  scheduler.createQueue;

export const buildVirginiaCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string =
  scheduler.buildLinkedElectionSyncJobId;

export const upsertRecurringVirginiaCandidateFinanceSyncJobs: (
  jobData?: VirginiaCandidateFinanceSyncJobData
) => Promise<void> = scheduler.upsertRecurringJobs;

export const enqueueManualVirginiaCandidateFinanceSyncJob: (
  jobData?: VirginiaCandidateFinanceSyncJobData,
  options?: VirginiaCandidateFinanceSyncEnqueueOptions
) => Promise<string> = scheduler.enqueueManualJob;

export const runVirginiaCandidateFinanceSyncJob: (
  data?: VirginiaCandidateFinanceSyncJobData
) => Promise<VirginiaCandidateFinanceSyncJobResult> = scheduler.runJob;

export const createVirginiaCandidateFinanceSyncSchedulerWorker: () => Worker<
  VirginiaCandidateFinanceSyncJobData,
  VirginiaCandidateFinanceSyncJobResult
> = scheduler.createWorker;

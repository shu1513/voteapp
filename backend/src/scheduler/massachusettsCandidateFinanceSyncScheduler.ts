import type { Queue, Worker } from "bullmq";

import {
  isMassachusettsCampaignFinanceEnabled,
  isMassachusettsCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueMassachusettsCandidateFinance,
  type MassachusettsCandidateFinanceBatchSyncResult,
} from "../pipeline/massachusettsFinance/massachusettsCandidateFinanceBatchSync.js";
import {
  createStateCandidateFinanceSyncScheduler,
  type StateCandidateFinanceSyncEnqueueOptions,
  type StateCandidateFinanceSyncJobData,
} from "./stateCandidateFinanceSyncScheduler.js";

export const MASSACHUSETTS_CANDIDATE_FINANCE_SYNC_JOB_NAME = "massachusetts_candidate_finance_sync_due";
export const MASSACHUSETTS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "massachusetts_candidate_finance_sync_daily";

export type MassachusettsCandidateFinanceSyncJobData = StateCandidateFinanceSyncJobData;

export type MassachusettsCandidateFinanceSyncJobResult = MassachusettsCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MassachusettsCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type MassachusettsCandidateFinanceSyncEnqueueOptions = StateCandidateFinanceSyncEnqueueOptions;

const scheduler = createStateCandidateFinanceSyncScheduler({
  stateLabel: "Massachusetts",
  jobName: MASSACHUSETTS_CANDIDATE_FINANCE_SYNC_JOB_NAME,
  dailySchedulerId: MASSACHUSETTS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
  defaultQueueName: "massachusetts_candidate_finance_sync_maintenance",
  linkedElectionJobIdPrefix: "massachusetts-candidate-finance-linked-election-sync-",
  envPrefix: "MASSACHUSETTS_CAMPAIGN_FINANCE_SYNC",
  defaultDailyCron: "25 9 * * *",
  isEnabled: isMassachusettsCampaignFinanceEnabled,
  isSyncEnabled: isMassachusettsCampaignFinanceSyncEnabled,
  syncDue: syncDueMassachusettsCandidateFinance,
});

export const createMassachusettsCandidateFinanceSyncSchedulerQueue: () => Queue<MassachusettsCandidateFinanceSyncJobData> =
  scheduler.createQueue;

export const buildMassachusettsCandidateFinanceLinkedElectionSyncJobId: (now?: Date) => string =
  scheduler.buildLinkedElectionSyncJobId;

export const upsertRecurringMassachusettsCandidateFinanceSyncJobs: (
  jobData?: MassachusettsCandidateFinanceSyncJobData
) => Promise<void> = scheduler.upsertRecurringJobs;

export const enqueueManualMassachusettsCandidateFinanceSyncJob: (
  jobData?: MassachusettsCandidateFinanceSyncJobData,
  options?: MassachusettsCandidateFinanceSyncEnqueueOptions
) => Promise<string> = scheduler.enqueueManualJob;

export const runMassachusettsCandidateFinanceSyncJob: (
  data?: MassachusettsCandidateFinanceSyncJobData
) => Promise<MassachusettsCandidateFinanceSyncJobResult> = scheduler.runJob;

export const createMassachusettsCandidateFinanceSyncSchedulerWorker: () => Worker<
  MassachusettsCandidateFinanceSyncJobData,
  MassachusettsCandidateFinanceSyncJobResult
> = scheduler.createWorker;

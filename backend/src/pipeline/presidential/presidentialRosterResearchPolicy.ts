import { getPresidentialGeneralElectionDate } from "./presidentialCycles.js";

export type PresidentialRosterResearchCycleStatus = "active" | "completed";

export type PresidentialRosterResearchPhase =
  | "early_announcement"
  | "active_pre_primary"
  | "ballot_qualification"
  | "primary_season";

export type PresidentialRosterResearchEligibilityReason =
  | "before_research_window"
  | "after_research_window"
  | "cycle_completed"
  | "not_due"
  | "due";

export type PresidentialRosterResearchEligibilityInput = {
  electionYear: number;
  cycleStatus: PresidentialRosterResearchCycleStatus;
  lastAttemptedAt?: string | Date | null;
  nextResearchAt?: string | Date | null;
  now?: Date;
};

export type PresidentialRosterResearchEligibility =
  | {
      eligible: true;
      reason: "due";
      phase: PresidentialRosterResearchPhase;
      researchStartAt: Date;
      researchStopAt: Date;
      nextEligibleAt?: never;
    }
  | {
      eligible: false;
      reason: Exclude<PresidentialRosterResearchEligibilityReason, "due">;
      phase: PresidentialRosterResearchPhase | null;
      researchStartAt: Date;
      researchStopAt: Date;
      nextEligibleAt: Date | null;
    };

export const PRESIDENTIAL_ROSTER_RESEARCH_WEEKLY_DAYS = 7;
export const PRESIDENTIAL_ROSTER_RESEARCH_ACTIVE_PRE_PRIMARY_DAYS = 3;
export const PRESIDENTIAL_ROSTER_RESEARCH_BALLOT_QUALIFICATION_DAYS = 2;
export const PRESIDENTIAL_ROSTER_RESEARCH_PRIMARY_SEASON_DAYS = 2;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential roster research ${label}`);
  }
}

function parseOptionalDate(value: string | Date | null | undefined, label: string): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  assertValidDate(date, label);
  return date;
}

function assertValidCycleStatus(status: string): asserts status is PresidentialRosterResearchCycleStatus {
  if (status !== "active" && status !== "completed") {
    throw new Error(`Invalid presidential roster research cycle status: ${status}`);
  }
}

function utcDateFromIsoDate(value: string): Date {
  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  return new Date(Date.UTC(year, month - 1, day));
}

function addUtcMonths(date: Date, months: number): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, date.getUTCDate()));
}

function addDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function getGeneralElectionDate(electionYear: number): Date {
  return utcDateFromIsoDate(getPresidentialGeneralElectionDate(electionYear));
}

export function getPresidentialRosterResearchStartAt(electionYear: number): Date {
  return addUtcMonths(getGeneralElectionDate(electionYear), -20);
}

export function getPresidentialRosterResearchStopAt(electionYear: number): Date {
  return addUtcMonths(getGeneralElectionDate(electionYear), -5);
}

function getPhaseBoundaries(electionYear: number): {
  startAt: Date;
  activePrePrimaryAt: Date;
  ballotQualificationAt: Date;
  primarySeasonAt: Date;
  stopAt: Date;
} {
  const generalElectionAt = getGeneralElectionDate(electionYear);
  return {
    startAt: addUtcMonths(generalElectionAt, -20),
    activePrePrimaryAt: addUtcMonths(generalElectionAt, -16),
    ballotQualificationAt: addUtcMonths(generalElectionAt, -12),
    primarySeasonAt: addUtcMonths(generalElectionAt, -10),
    stopAt: addUtcMonths(generalElectionAt, -5),
  };
}

export function getPresidentialRosterResearchPhase(
  now: Date,
  electionYear: number
): PresidentialRosterResearchPhase | null {
  assertValidDate(now, "now");
  const boundaries = getPhaseBoundaries(electionYear);
  const timestamp = now.getTime();

  if (timestamp < boundaries.startAt.getTime() || timestamp >= boundaries.stopAt.getTime()) {
    return null;
  }
  if (timestamp < boundaries.activePrePrimaryAt.getTime()) {
    return "early_announcement";
  }
  if (timestamp < boundaries.ballotQualificationAt.getTime()) {
    return "active_pre_primary";
  }
  if (timestamp < boundaries.primarySeasonAt.getTime()) {
    return "ballot_qualification";
  }
  return "primary_season";
}

export function getPresidentialRosterResearchIntervalDays(
  phase: PresidentialRosterResearchPhase
): number {
  switch (phase) {
    case "early_announcement":
      return PRESIDENTIAL_ROSTER_RESEARCH_WEEKLY_DAYS;
    case "active_pre_primary":
      return PRESIDENTIAL_ROSTER_RESEARCH_ACTIVE_PRE_PRIMARY_DAYS;
    case "ballot_qualification":
      return PRESIDENTIAL_ROSTER_RESEARCH_BALLOT_QUALIFICATION_DAYS;
    case "primary_season":
      return PRESIDENTIAL_ROSTER_RESEARCH_PRIMARY_SEASON_DAYS;
  }
}

export function addPresidentialRosterResearchDelay(from: Date, electionYear: number): Date | null {
  assertValidDate(from, "delay source date");
  const phase = getPresidentialRosterResearchPhase(from, electionYear);
  if (!phase) {
    return null;
  }
  const next = addDays(from, getPresidentialRosterResearchIntervalDays(phase));
  const stopAt = getPresidentialRosterResearchStopAt(electionYear);
  return next.getTime() >= stopAt.getTime() ? null : next;
}

export function evaluatePresidentialRosterResearchEligibility(
  input: PresidentialRosterResearchEligibilityInput
): PresidentialRosterResearchEligibility {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  assertValidCycleStatus(input.cycleStatus);

  const researchStartAt = getPresidentialRosterResearchStartAt(input.electionYear);
  const researchStopAt = getPresidentialRosterResearchStopAt(input.electionYear);
  const phase = getPresidentialRosterResearchPhase(now, input.electionYear);

  if (input.cycleStatus === "completed") {
    return {
      eligible: false,
      reason: "cycle_completed",
      phase,
      researchStartAt,
      researchStopAt,
      nextEligibleAt: null,
    };
  }

  if (now.getTime() < researchStartAt.getTime()) {
    return {
      eligible: false,
      reason: "before_research_window",
      phase: null,
      researchStartAt,
      researchStopAt,
      nextEligibleAt: researchStartAt,
    };
  }

  if (!phase) {
    return {
      eligible: false,
      reason: "after_research_window",
      phase: null,
      researchStartAt,
      researchStopAt,
      nextEligibleAt: null,
    };
  }

  const nextResearchAt = parseOptionalDate(input.nextResearchAt, "nextResearchAt");
  if (nextResearchAt && now.getTime() < nextResearchAt.getTime()) {
    return {
      eligible: false,
      reason: "not_due",
      phase,
      researchStartAt,
      researchStopAt,
      nextEligibleAt: nextResearchAt,
    };
  }

  const lastAttemptedAt = parseOptionalDate(input.lastAttemptedAt, "lastAttemptedAt");
  if (!nextResearchAt && lastAttemptedAt) {
    const derivedNextAt = addPresidentialRosterResearchDelay(lastAttemptedAt, input.electionYear);
    if (derivedNextAt && now.getTime() < derivedNextAt.getTime()) {
      return {
        eligible: false,
        reason: "not_due",
        phase,
        researchStartAt,
        researchStopAt,
        nextEligibleAt: derivedNextAt,
      };
    }
  }

  return {
    eligible: true,
    reason: "due",
    phase,
    researchStartAt,
    researchStopAt,
  };
}

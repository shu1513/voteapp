import { getPresidentialGeneralElectionDate } from "./presidentialCycles.js";

export type PresidentialNomineeResearchCycleStatus = "active" | "completed";

export type PresidentialNomineeResearchEligibilityReason =
  | "before_research_window"
  | "after_research_window"
  | "cycle_completed"
  | "not_due"
  | "due";

export type PresidentialNomineeResearchEligibilityInput = {
  electionYear: number;
  cycleStatus: PresidentialNomineeResearchCycleStatus;
  lastAttemptedAt?: string | Date | null;
  nextResearchAt?: string | Date | null;
  now?: Date;
};

export type PresidentialNomineeResearchEligibility =
  | {
      eligible: true;
      reason: "due";
      researchStartAt: Date;
      researchStopAt: Date;
      nextEligibleAt?: never;
    }
  | {
      eligible: false;
      reason: Exclude<PresidentialNomineeResearchEligibilityReason, "due">;
      researchStartAt: Date;
      researchStopAt: Date;
      nextEligibleAt: Date | null;
    };

export const PRESIDENTIAL_NOMINEE_RESEARCH_INTERVAL_DAYS = 2;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential nominee research ${label}`);
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

function assertValidCycleStatus(status: string): asserts status is PresidentialNomineeResearchCycleStatus {
  if (status !== "active" && status !== "completed") {
    throw new Error(`Invalid presidential nominee research cycle status: ${status}`);
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

export function getPresidentialNomineeResearchStartAt(electionYear: number): Date {
  return addUtcMonths(getGeneralElectionDate(electionYear), -9);
}

export function getPresidentialNomineeResearchStopAt(electionYear: number): Date {
  return addUtcMonths(getGeneralElectionDate(electionYear), -5);
}

export function addPresidentialNomineeResearchDelay(from: Date, electionYear: number): Date | null {
  assertValidDate(from, "delay source date");
  const researchStartAt = getPresidentialNomineeResearchStartAt(electionYear);
  const researchStopAt = getPresidentialNomineeResearchStopAt(electionYear);
  if (from.getTime() < researchStartAt.getTime() || from.getTime() >= researchStopAt.getTime()) {
    return null;
  }
  const next = addDays(from, PRESIDENTIAL_NOMINEE_RESEARCH_INTERVAL_DAYS);
  return next.getTime() >= researchStopAt.getTime() ? null : next;
}

export function evaluatePresidentialNomineeResearchEligibility(
  input: PresidentialNomineeResearchEligibilityInput
): PresidentialNomineeResearchEligibility {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  assertValidCycleStatus(input.cycleStatus);

  const researchStartAt = getPresidentialNomineeResearchStartAt(input.electionYear);
  const researchStopAt = getPresidentialNomineeResearchStopAt(input.electionYear);

  if (input.cycleStatus === "completed") {
    return {
      eligible: false,
      reason: "cycle_completed",
      researchStartAt,
      researchStopAt,
      nextEligibleAt: null,
    };
  }

  if (now.getTime() < researchStartAt.getTime()) {
    return {
      eligible: false,
      reason: "before_research_window",
      researchStartAt,
      researchStopAt,
      nextEligibleAt: researchStartAt,
    };
  }

  if (now.getTime() >= researchStopAt.getTime()) {
    return {
      eligible: false,
      reason: "after_research_window",
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
      researchStartAt,
      researchStopAt,
      nextEligibleAt: nextResearchAt,
    };
  }

  const lastAttemptedAt = parseOptionalDate(input.lastAttemptedAt, "lastAttemptedAt");
  if (!nextResearchAt && lastAttemptedAt) {
    const derivedNextAt = addPresidentialNomineeResearchDelay(lastAttemptedAt, input.electionYear);
    if (derivedNextAt && now.getTime() < derivedNextAt.getTime()) {
      return {
        eligible: false,
        reason: "not_due",
        researchStartAt,
        researchStopAt,
        nextEligibleAt: derivedNextAt,
      };
    }
  }

  return {
    eligible: true,
    reason: "due",
    researchStartAt,
    researchStopAt,
  };
}

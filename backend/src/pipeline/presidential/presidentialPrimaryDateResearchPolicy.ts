import { getPresidentialGeneralElectionDate } from "./presidentialCycles.js";

export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_LEAD_MONTHS = 20;
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_BIWEEKLY_START_MONTHS_BEFORE_GENERAL = 16;
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_WEEKLY_START_MONTHS_BEFORE_GENERAL = 12;
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_BIWEEKLY_RETRY_DAYS = 14;
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_WEEKLY_RETRY_DAYS = 7;
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_STOP_GRACE_DAYS = 30;

export type PresidentialPrimaryDateResearchStatus =
  | "pending"
  | "not_official_yet"
  | "official_found"
  | "error";

export type PresidentialPrimaryDateResearchEligibilityReason =
  | "due"
  | "before_research_window"
  | "after_research_window"
  | "already_official"
  | "not_due";

export type PresidentialPrimaryDateResearchEligibility =
  | {
      eligible: true;
      reason: "due";
      researchStartAt: Date;
    }
  | {
      eligible: false;
      reason: Exclude<PresidentialPrimaryDateResearchEligibilityReason, "due">;
      researchStartAt: Date;
      nextEligibleAt: Date | null;
    };

export type PresidentialPrimaryDateResearchEligibilityInput = {
  electionYear: number;
  dateResearchStatus: PresidentialPrimaryDateResearchStatus;
  nextResearchAt?: Date | string | null;
  now?: Date;
};

const VALID_RESEARCH_STATUSES = new Set<PresidentialPrimaryDateResearchStatus>([
  "pending",
  "not_official_yet",
  "official_found",
  "error",
]);

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential primary date research ${label}`);
  }
}

function assertValidResearchStatus(status: string): asserts status is PresidentialPrimaryDateResearchStatus {
  if (!VALID_RESEARCH_STATUSES.has(status as PresidentialPrimaryDateResearchStatus)) {
    throw new Error(`Invalid presidential primary date research status: ${status}`);
  }
}

function parseIsoDateUtc(value: string, label: string): Date {
  const trimmed = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new Error(`Invalid presidential primary date research ${label}: ${value}`);
  }
  const date = new Date(`${trimmed}T00:00:00.000Z`);
  if (date.toISOString().slice(0, 10) !== trimmed) {
    throw new Error(`Invalid presidential primary date research ${label}: ${value}`);
  }
  return date;
}

function parseOptionalTimestampUtc(value: Date | string | null | undefined, label: string): Date | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (value instanceof Date) {
    assertValidDate(value, label);
    return value;
  }
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const date = new Date(trimmed);
  assertValidDate(date, label);
  return date;
}

function daysInUtcMonth(year: number, monthIndex: number): number {
  return new Date(Date.UTC(year, monthIndex + 1, 0)).getUTCDate();
}

function addUtcMonths(date: Date, months: number): Date {
  assertValidDate(date, "date");
  const targetMonthIndex = date.getUTCMonth() + months;
  const targetYear = date.getUTCFullYear() + Math.floor(targetMonthIndex / 12);
  const normalizedMonthIndex = ((targetMonthIndex % 12) + 12) % 12;
  const targetDay = Math.min(date.getUTCDate(), daysInUtcMonth(targetYear, normalizedMonthIndex));

  return new Date(
    Date.UTC(
      targetYear,
      normalizedMonthIndex,
      targetDay,
      date.getUTCHours(),
      date.getUTCMinutes(),
      date.getUTCSeconds(),
      date.getUTCMilliseconds()
    )
  );
}

function addUtcDays(date: Date, days: number): Date {
  assertValidDate(date, "date");
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

export function addPresidentialPrimaryDateResearchRetryDelay(
  fromDate: Date,
  electionYear: number
): Date {
  assertValidDate(fromDate, "retry reference date");
  const generalElectionDate = parseIsoDateUtc(
    getPresidentialGeneralElectionDate(electionYear),
    "general election date"
  );
  const biweeklyStartAt = addUtcMonths(
    generalElectionDate,
    -PRESIDENTIAL_PRIMARY_DATE_RESEARCH_BIWEEKLY_START_MONTHS_BEFORE_GENERAL
  );
  const weeklyStartAt = addUtcMonths(
    generalElectionDate,
    -PRESIDENTIAL_PRIMARY_DATE_RESEARCH_WEEKLY_START_MONTHS_BEFORE_GENERAL
  );

  if (fromDate.getTime() < biweeklyStartAt.getTime()) {
    return addUtcMonths(fromDate, 1);
  }

  const retryDays =
    fromDate.getTime() < weeklyStartAt.getTime()
      ? PRESIDENTIAL_PRIMARY_DATE_RESEARCH_BIWEEKLY_RETRY_DAYS
      : PRESIDENTIAL_PRIMARY_DATE_RESEARCH_WEEKLY_RETRY_DAYS;

  return new Date(
    fromDate.getTime() + retryDays * 24 * 60 * 60 * 1000
  );
}

export function getPresidentialPrimaryDateResearchStartAt(electionYear: number): Date {
  const generalElectionDate = parseIsoDateUtc(
    getPresidentialGeneralElectionDate(electionYear),
    "general election date"
  );
  return addUtcMonths(generalElectionDate, -PRESIDENTIAL_PRIMARY_DATE_RESEARCH_LEAD_MONTHS);
}

export function getPresidentialPrimaryDateResearchStopAt(electionYear: number): Date {
  const generalElectionDate = parseIsoDateUtc(
    getPresidentialGeneralElectionDate(electionYear),
    "general election date"
  );
  return addUtcDays(generalElectionDate, PRESIDENTIAL_PRIMARY_DATE_RESEARCH_STOP_GRACE_DAYS);
}

export function evaluatePresidentialPrimaryDateResearchEligibility(
  input: PresidentialPrimaryDateResearchEligibilityInput
): PresidentialPrimaryDateResearchEligibility {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  assertValidResearchStatus(input.dateResearchStatus);

  const researchStartAt = getPresidentialPrimaryDateResearchStartAt(input.electionYear);
  const researchStopAt = getPresidentialPrimaryDateResearchStopAt(input.electionYear);
  if (input.dateResearchStatus === "official_found") {
    return {
      eligible: false,
      reason: "already_official",
      researchStartAt,
      nextEligibleAt: null,
    };
  }

  if (now.getTime() < researchStartAt.getTime()) {
    return {
      eligible: false,
      reason: "before_research_window",
      researchStartAt,
      nextEligibleAt: researchStartAt,
    };
  }

  if (now.getTime() >= researchStopAt.getTime()) {
    return {
      eligible: false,
      reason: "after_research_window",
      researchStartAt,
      nextEligibleAt: null,
    };
  }

  const nextResearchAt = parseOptionalTimestampUtc(input.nextResearchAt, "next research timestamp");
  if (nextResearchAt && nextResearchAt.getTime() > now.getTime()) {
    return {
      eligible: false,
      reason: "not_due",
      researchStartAt,
      nextEligibleAt: nextResearchAt,
    };
  }

  return {
    eligible: true,
    reason: "due",
    researchStartAt,
  };
}

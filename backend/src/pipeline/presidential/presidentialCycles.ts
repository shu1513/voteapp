import type { Pool, PoolClient } from "pg";

export type PresidentialCycleStage = "primary" | "general";

export type PresidentialCycleSeed = {
  electionYear: number;
  stage: PresidentialCycleStage;
  party: string | null;
  electionDate: string | null;
  status: "upcoming";
  sources: readonly unknown[];
};

export const DEFAULT_PRESIDENTIAL_CYCLE_COUNT = 5;
export const DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES = ["Democratic", "Republican"] as const;

type Queryable = Pick<Pool | PoolClient, "query">;

export type UpsertPresidentialCyclesResult = {
  requested: number;
  changed: number;
  unchanged: number;
};

function assertValidYear(year: number): void {
  if (!Number.isInteger(year) || year < 2000 || year > 2100) {
    throw new Error(`Invalid presidential election year: ${year}`);
  }
}

function assertValidDate(date: Date): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error("Invalid presidential cycle reference date");
  }
}

function assertValidCycleSeed(seed: PresidentialCycleSeed): void {
  assertValidYear(seed.electionYear);
  if (!isPresidentialElectionYear(seed.electionYear)) {
    throw new Error(`Year is not a presidential election year: ${seed.electionYear}`);
  }
  if (seed.status !== "upcoming") {
    throw new Error(`Unsupported presidential cycle seed status: ${seed.status}`);
  }
  if (!Array.isArray(seed.sources)) {
    throw new Error("Presidential cycle seed sources must be an array");
  }
  if (seed.stage === "general") {
    if (seed.party !== null) {
      throw new Error("General presidential cycle seed party must be null");
    }
    if (seed.electionDate !== getPresidentialGeneralElectionDate(seed.electionYear)) {
      throw new Error(`Invalid general presidential election date for ${seed.electionYear}`);
    }
    return;
  }
  if (seed.stage === "primary") {
    if (!seed.party || seed.party.trim().length === 0) {
      throw new Error("Primary presidential cycle seed party is required");
    }
    if (seed.electionDate !== null) {
      throw new Error("Primary presidential cycle seed electionDate must be null");
    }
    return;
  }
  const unsupportedStage: never = seed.stage;
  throw new Error(`Unsupported presidential cycle seed stage: ${unsupportedStage}`);
}

export function isPresidentialElectionYear(year: number): boolean {
  return Number.isInteger(year) && year % 4 === 0;
}

export function getPresidentialGeneralElectionDate(year: number): string {
  assertValidYear(year);
  if (!isPresidentialElectionYear(year)) {
    throw new Error(`Year is not a presidential election year: ${year}`);
  }

  const novemberFirst = new Date(Date.UTC(year, 10, 1));
  const daysUntilFirstMonday = (1 - novemberFirst.getUTCDay() + 7) % 7;
  const firstMondayOfNovember = 1 + daysUntilFirstMonday;
  const electionDay = firstMondayOfNovember + 1;

  return new Date(Date.UTC(year, 10, electionDay)).toISOString().slice(0, 10);
}

export function getUpcomingPresidentialElectionYears(
  fromDate: Date = new Date(),
  count = DEFAULT_PRESIDENTIAL_CYCLE_COUNT
): number[] {
  assertValidDate(fromDate);
  if (!Number.isInteger(count) || count <= 0) {
    throw new Error(`Invalid presidential cycle count: ${count}`);
  }

  const years: number[] = [];
  let year = fromDate.getUTCFullYear();
  while (years.length < count) {
    if (isPresidentialElectionYear(year)) {
      const electionDate = getPresidentialGeneralElectionDate(year);
      const electionHasPassed = fromDate.toISOString().slice(0, 10) > electionDate;
      if (!electionHasPassed) {
        years.push(year);
      }
    }
    year += 1;
  }
  return years;
}

export function buildPresidentialCycleSeeds(
  fromDate: Date = new Date(),
  count = DEFAULT_PRESIDENTIAL_CYCLE_COUNT,
  primaryParties: readonly string[] = DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES
): PresidentialCycleSeed[] {
  assertValidDate(fromDate);
  if (primaryParties.length === 0) {
    throw new Error("At least one presidential primary party is required");
  }

  const normalizedParties = primaryParties.map((party) => party.trim());
  const blankParty = normalizedParties.find((party) => party.length === 0);
  if (blankParty !== undefined) {
    throw new Error("Presidential primary party cannot be blank");
  }

  const seeds: PresidentialCycleSeed[] = [];
  for (const electionYear of getUpcomingPresidentialElectionYears(fromDate, count)) {
    seeds.push({
      electionYear,
      stage: "general",
      party: null,
      electionDate: getPresidentialGeneralElectionDate(electionYear),
      status: "upcoming",
      sources: [],
    });

    for (const party of normalizedParties) {
      seeds.push({
        electionYear,
        stage: "primary",
        party,
        electionDate: null,
        status: "upcoming",
        sources: [],
      });
    }
  }

  return seeds;
}

async function upsertGeneralPresidentialCycle(db: Queryable, seed: PresidentialCycleSeed): Promise<boolean> {
  const result = await db.query<{ id: string }>(
    `
      INSERT INTO public.presidential_cycles (
        election_year,
        stage,
        party,
        election_date,
        status,
        sources
      )
      VALUES ($1, 'general', NULL, $2::date, $3, $4::jsonb)
      ON CONFLICT (election_year) WHERE stage = 'general'
      DO UPDATE SET
        election_date = EXCLUDED.election_date
      WHERE public.presidential_cycles.election_date IS DISTINCT FROM EXCLUDED.election_date
      RETURNING id
    `,
    [seed.electionYear, seed.electionDate, seed.status, JSON.stringify(seed.sources)]
  );

  return result.rowCount === 1;
}

async function upsertPrimaryPresidentialCycle(db: Queryable, seed: PresidentialCycleSeed): Promise<boolean> {
  const result = await db.query<{ id: string }>(
    `
      INSERT INTO public.presidential_cycles (
        election_year,
        stage,
        party,
        election_date,
        status,
        sources
      )
      VALUES ($1, 'primary', $2, NULL, $3, $4::jsonb)
      ON CONFLICT (election_year, party) WHERE stage = 'primary'
      DO NOTHING
      RETURNING id
    `,
    [seed.electionYear, seed.party, seed.status, JSON.stringify(seed.sources)]
  );

  return result.rowCount === 1;
}

export async function upsertPresidentialCycles(
  db: Queryable,
  seeds: readonly PresidentialCycleSeed[]
): Promise<UpsertPresidentialCyclesResult> {
  let changed = 0;

  for (const seed of seeds) {
    assertValidCycleSeed(seed);
    const didChange =
      seed.stage === "general"
        ? await upsertGeneralPresidentialCycle(db, seed)
        : await upsertPrimaryPresidentialCycle(db, seed);
    if (didChange) {
      changed += 1;
    }
  }

  return {
    requested: seeds.length,
    changed,
    unchanged: seeds.length - changed,
  };
}

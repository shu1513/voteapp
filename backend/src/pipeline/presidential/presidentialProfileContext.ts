import type { Pool, PoolClient } from "pg";

import type { PresidentialCycleStage } from "./presidentialCycles.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type PresidentialCycleProfileContextRow = {
  id: string;
  election_year: number;
  stage: PresidentialCycleStage;
  party: string | null;
  election_date: string | null;
  sources: unknown;
};

export type PresidentialCycleProfileContext = {
  cycleId: string;
  electionYear: number;
  stage: PresidentialCycleStage;
  party: string | null;
  districtName: "United States";
  districtType: "presidential";
  state: "US";
  electionDate: string | null;
  officialBallotTitle: string;
  electionStage: PresidentialCycleStage;
  electionIsPartisan: true;
  seedUrls: string[];
};

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
  const seen = new Set<string>();
  for (const item of raw) {
    if (typeof item !== "string") {
      continue;
    }
    const trimmed = item.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    urls.push(trimmed);
  }
  return urls;
}

function officialBallotTitleForCycle(row: PresidentialCycleProfileContextRow): string {
  if (row.stage === "general") {
    return `President of the United States, ${row.election_year} general election`;
  }
  const party = row.party?.trim();
  return party
    ? `President of the United States, ${row.election_year} ${party} primary`
    : `President of the United States, ${row.election_year} primary`;
}

function toPresidentialCycleProfileContext(
  row: PresidentialCycleProfileContextRow
): PresidentialCycleProfileContext {
  return {
    cycleId: row.id,
    electionYear: row.election_year,
    stage: row.stage,
    party: row.party,
    districtName: "United States",
    districtType: "presidential",
    state: "US",
    electionDate: row.election_date,
    officialBallotTitle: officialBallotTitleForCycle(row),
    electionStage: row.stage,
    electionIsPartisan: true,
    seedUrls: parseSeedUrls(row.sources),
  };
}

export async function loadPresidentialCycleProfileContext(
  db: Queryable,
  cycleId: string
): Promise<PresidentialCycleProfileContext | null> {
  const trimmedCycleId = cycleId.trim();
  if (trimmedCycleId.length === 0) {
    return null;
  }

  const result = await db.query<PresidentialCycleProfileContextRow>(
    `
      SELECT
        id,
        election_year,
        stage,
        party,
        election_date::text AS election_date,
        sources
      FROM public.presidential_cycles
      WHERE id = $1
      LIMIT 1
    `,
    [trimmedCycleId]
  );

  const row = result.rows[0];
  return row ? toPresidentialCycleProfileContext(row) : null;
}

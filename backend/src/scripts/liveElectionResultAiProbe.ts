import { Pool } from "pg";

import {
  buildElectionResultAiConfigFromEnv,
  enrichElectionResults,
} from "../ai/enrichElectionResults.js";
import { getPipelineEnv } from "../config/env.js";
import {
  chunkElectionResultContexts,
  loadElectionResultContexts,
} from "../pipeline/electionResults/electionResultContextLoader.js";
import { processElectionResultSearchJob } from "../pipeline/enrichers/electionResultsEnricher.js";
import type { ElectionResultPassType } from "../types/electionResults.js";

type ProbeArgs = {
  state: string;
  date: string;
  passType: ElectionResultPassType;
  limit: number;
  write: boolean;
};

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    return process.argv[index + 1] ?? null;
  }
  const inline = process.argv.find((token) => token.startsWith(`${name}=`));
  return inline ? inline.slice(name.length + 1) : null;
}

function parseArgs(): ProbeArgs {
  const state = (readFlag("--state") ?? "").trim().toUpperCase();
  const date = (readFlag("--date") ?? "").trim();
  const pass = (readFlag("--pass") ?? "election_night").trim();
  const limitRaw = Number.parseInt(readFlag("--limit") ?? "10", 10);
  if (!/^[A-Z]{2}$/.test(state) && state !== "DC") {
    throw new Error("Usage: --state CA --date YYYY-MM-DD --pass election_night|certified [--limit 10] [--write]");
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error("Usage: --date must be YYYY-MM-DD");
  }
  if (pass !== "election_night" && pass !== "certified") {
    throw new Error("--pass must be election_night or certified");
  }
  return {
    state,
    date,
    passType: pass,
    limit: Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 10) : 10,
    write: process.argv.includes("--write"),
  };
}

async function listElectionIds(pool: Pool, args: ProbeArgs): Promise<string[]> {
  const result = await pool.query<{ id: string }>(
    `
      SELECT e.id
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE d.state = $1
        AND e.election_date = $2::date
      ORDER BY e.official_ballot_title ASC, e.id ASC
      LIMIT $3::int
    `,
    [args.state, args.date, args.limit]
  );
  return result.rows.map((row) => row.id);
}

async function main(): Promise<void> {
  const args = parseArgs();
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const scheduledFor = new Date().toISOString();

  try {
    const electionIds = await listElectionIds(pool, args);
    if (electionIds.length === 0) {
      console.log(JSON.stringify({ ok: false, reason: "no matching elections", args }, null, 2));
      return;
    }

    if (args.write) {
      const result = await processElectionResultSearchJob(
        {
          state: args.state,
          election_date: args.date,
          pass_type: args.passType,
          scheduled_for: scheduledFor,
          election_ids: electionIds,
          run_id: `live_election_result_probe_${new Date().toISOString()}`,
        },
        { pool }
      );
      console.log(JSON.stringify({ ok: true, write: true, result }, null, 2));
      return;
    }

    const contexts = await loadElectionResultContexts(pool, electionIds);
    const aiConfig = buildElectionResultAiConfigFromEnv();
    const chunks = chunkElectionResultContexts(contexts, 10);
    const results = [];
    for (const chunk of chunks) {
      results.push(
        await enrichElectionResults(
          {
            passType: args.passType,
            scheduledFor,
            contexts: chunk,
          },
          aiConfig
        )
      );
    }

    console.log(
      JSON.stringify(
        {
          ok: true,
          write: false,
          args,
          election_ids: electionIds,
          loaded_context_count: contexts.length,
          results,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("live election result AI probe failed:", message);
  process.exitCode = 1;
});

import { Pool } from "pg";

import { canonicalizeParty } from "../pipeline/candidates/candidatePartyCanonicalization.js";

/**
 * One-off backfill: rewrites existing candidates.party values to the
 * canonical spelling that resolveStoredCandidateParty now enforces on every
 * new write ("DEM" / "Democrat" / "Democratic Party" → "Democratic").
 *
 * Deterministic and variant-driven: distinct party values are read once, run
 * through the same canonicalizeParty the write path uses, and each variant
 * that changes becomes one UPDATE keyed on the exact stored string. Values
 * the function passes through untouched are left alone — including the
 * parenthesis-mangled import defects ("Independent) (Write-in"), which are
 * an upstream splitting bug, not a spelling; they are reported under
 * `flagged` for manual repair instead of being guessed at.
 *
 * Soft-deleted rows are repaired too: the normalization is cosmetic, and a
 * later un-delete should not resurrect a retired spelling.
 *
 * Usage:
 *   npm run manual:candidates:canonicalize-parties            # dry run
 *   npm run manual:candidates:canonicalize-parties -- --apply
 */

type VariantRow = {
  party: string;
  rows: string;
};

type Change = {
  from: string;
  to: string;
  rows: number;
};

function parseArgs(argv: readonly string[]): { apply: boolean } {
  let apply = false;
  for (const arg of argv) {
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    throw new Error(`unknown flag(s): ${arg}`);
  }
  return { apply };
}

async function main(): Promise<void> {
  const { apply } = parseArgs(process.argv.slice(2));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate party canonicalization");
  }
  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const variants = await pool.query<VariantRow>(
      `SELECT party, count(*)::text AS rows
         FROM public.candidates
        GROUP BY party
        ORDER BY count(*) DESC, party`
    );

    const changes: Change[] = [];
    const flagged: { party: string; rows: number }[] = [];
    for (const variant of variants.rows) {
      const rows = Number(variant.rows);
      // Parenthesis fragments mark the upstream roster-splitting defect;
      // surface them every run until they are manually repaired, and leave
      // the stored value byte-identical (even whitespace) so the manual
      // repair can match the rows exactly.
      if (variant.party.includes("(") || variant.party.includes(")")) {
        flagged.push({ party: variant.party, rows });
        continue;
      }
      const canonical = canonicalizeParty(variant.party);
      if (canonical !== variant.party) {
        changes.push({ from: variant.party, to: canonical, rows });
      }
    }

    let updatedRows = 0;
    if (apply) {
      for (const change of changes) {
        const result = await pool.query(
          `UPDATE public.candidates
              SET party = $2,
                  updated_at = now()
            WHERE party = $1`,
          [change.from, change.to]
        );
        updatedRows += result.rowCount ?? 0;
      }
    }

    console.log(
      JSON.stringify(
        {
          mode: apply ? "apply" : "dry-run",
          distinctVariants: variants.rows.length,
          changes,
          changedVariants: changes.length,
          changedRows: apply ? updatedRows : changes.reduce((sum, change) => sum + change.rows, 0),
          flagged,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

if (process.argv[1] && process.argv[1].endsWith("canonicalizeCandidateParties.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

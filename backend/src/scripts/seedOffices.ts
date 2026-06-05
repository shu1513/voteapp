import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import type { ElectionDistrictType } from "../types/election.js";
import { normalizeElectionTitleKey } from "../utils/normalizeElectionTitleKey.js";

type SeedOffice = {
  scope: ElectionDistrictType;
  canonicalName: string;
  summary: string;
};

type SeedOutcome = "inserted" | "updated" | "unchanged";

type SeedOfficeAlias = {
  scope: ElectionDistrictType;
  officeCanonicalName: string;
  aliasText: string;
};

const SEED_OFFICES: SeedOffice[] = [
  {
    scope: "statewide",
    canonicalName: "United States Senator",
    summary:
      "Represents the state in the U.S. Senate, voting on federal laws, confirmations, treaties, and national policy.",
  },
  {
    scope: "statewide",
    canonicalName: "Governor",
    summary:
      "Leads the state executive branch, proposes budgets, signs or vetoes legislation, and oversees state agencies.",
  },
  {
    scope: "statewide",
    canonicalName: "Lieutenant Governor",
    summary:
      "Performs duties defined by state law, often succeeds the governor if vacant, and may have legislative or administrative responsibilities.",
  },
  {
    scope: "statewide",
    canonicalName: "Secretary of State",
    summary:
      "Administers elections and key state records, including business filings and official state documentation.",
  },
  {
    scope: "statewide",
    canonicalName: "Attorney General",
    summary:
      "Serves as the state's chief legal officer, representing the state in legal matters and enforcing state law.",
  },
  {
    scope: "statewide",
    canonicalName: "State Treasurer",
    summary:
      "Manages state funds, cash operations, investments, and debt administration under state finance rules.",
  },
  {
    scope: "statewide",
    canonicalName: "State Auditor",
    summary:
      "Audits state agencies and programs for financial accuracy, compliance, and performance accountability.",
  },
  {
    scope: "statewide",
    canonicalName: "Comptroller",
    summary:
      "Oversees statewide accounting and fiscal controls, including revenue tracking, reporting, and payments.",
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Agriculture",
    summary:
      "Oversees state agricultural policy, industry regulation, food systems, and related inspections or programs.",
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Insurance",
    summary:
      "Regulates insurance markets, licensing, and consumer protections for insurance products in the state.",
  },
  {
    scope: "statewide",
    canonicalName: "Superintendent of Public Instruction",
    summary:
      "Leads statewide K-12 public education administration and helps implement education policy and standards.",
  },
  {
    scope: "statewide",
    canonicalName: "Public Service Commissioner",
    summary:
      "Regulates public utilities and related services, including rates, service standards, and provider oversight.",
  },
  {
    scope: "statewide",
    canonicalName: "Corporation Commissioner",
    summary:
      "Regulates specific business sectors under state law, often including utilities, securities, or corporations.",
  },
  {
    scope: "statewide",
    canonicalName: "State Level Judge",
    summary:
      "Serves in a statewide judicial role, reviewing cases and applying state constitutional, statutory, and procedural law.",
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Education Member",
    summary:
      "Sets or oversees statewide education policy, standards, and governance for the public school system.",
  },
  {
    scope: "us_house",
    canonicalName: "United States Representative",
    summary:
      "Represents a congressional district in the U.S. House, voting on federal legislation and budget matters.",
  },
  {
    scope: "state_upper",
    canonicalName: "State Senator",
    summary:
      "Represents a district in the state upper legislative chamber and votes on state laws and budget policy.",
  },
  {
    scope: "state_lower",
    canonicalName: "State Lower Chamber Legislator",
    summary:
      "Represents a district in the state lower legislative chamber and votes on state laws and budget policy.",
  },
  {
    scope: "county",
    canonicalName: "County Commissioner",
    summary:
      "Sets county policy and budget priorities, oversees county services, and governs county administration.",
  },
  {
    scope: "county",
    canonicalName: "Board of Supervisors",
    summary:
      "County governing board responsible for county budgets, ordinances, services, and administrative oversight.",
  },
  {
    scope: "county",
    canonicalName: "Sheriff",
    summary:
      "Leads county law enforcement operations, jail administration, and public safety duties assigned by law.",
  },
  {
    scope: "county",
    canonicalName: "District Attorney",
    summary:
      "Serves as the county prosecutor, making charging decisions and representing the public in criminal prosecutions.",
  },
  {
    scope: "county",
    canonicalName: "County Clerk",
    summary:
      "Maintains key county records and may administer elections, filings, and licensing functions.",
  },
  {
    scope: "county",
    canonicalName: "County Assessor",
    summary:
      "Determines property valuations used to calculate local property taxes and maintains assessment records.",
  },
  {
    scope: "county",
    canonicalName: "County Treasurer",
    summary:
      "Manages county funds, receipts, and disbursements, and may oversee tax collection or investment operations.",
  },
  {
    scope: "county",
    canonicalName: "County Recorder",
    summary:
      "Records and preserves public documents such as deeds, liens, and other official county filings.",
  },
  {
    scope: "county",
    canonicalName: "County Coroner",
    summary:
      "Investigates certain deaths under county jurisdiction and issues findings as required by law.",
  },
  {
    scope: "county",
    canonicalName: "County Superintendent of Schools",
    summary:
      "Oversees county-level education administration and support functions for local school systems.",
  },
  {
    scope: "county",
    canonicalName: "County Level Judge",
    summary:
      "Serves in a county-level judicial role, hearing cases and issuing rulings under state and local court procedure.",
  },
  {
    scope: "place",
    canonicalName: "Mayor",
    summary:
      "Leads municipal executive functions, oversees city administration, and helps set city policy priorities.",
  },
  {
    scope: "place",
    canonicalName: "City Council Member",
    summary:
      "Serves on the city legislative body, passing ordinances, approving budgets, and overseeing city governance.",
  },
  {
    scope: "place",
    canonicalName: "City Clerk",
    summary:
      "Maintains official municipal records and may administer local elections, filings, and public notices.",
  },
  {
    scope: "place",
    canonicalName: "City Treasurer",
    summary:
      "Manages municipal financial operations including receipts, disbursements, and fiscal reporting.",
  },
  {
    scope: "place",
    canonicalName: "Place Level Judge",
    summary:
      "Serves in a municipal or place-level judicial role, handling local court matters and applying relevant law and procedure.",
  },
  {
    scope: "place",
    canonicalName: "Alderman",
    summary:
      "Serves on a municipal legislative body in jurisdictions that use alderman titles for local representatives.",
  },
  {
    scope: "place",
    canonicalName: "Town Council Member",
    summary:
      "Serves on the town legislative body, setting policy, budgets, and oversight for municipal operations.",
  },
  {
    scope: "school_elementary",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
  {
    scope: "school_secondary",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
  {
    scope: "school_unified",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
];

const SEED_OFFICE_ALIASES: SeedOfficeAlias[] = [
  {
    scope: "statewide",
    officeCanonicalName: "Lieutenant Governor",
    aliasText: "Lt. Governor",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "State Supreme Court Justice",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "State Court of Appeals Judge",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "Judge",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "Justice",
  },
  {
    scope: "state_upper",
    officeCanonicalName: "State Senator",
    aliasText: "Member of the Legislature",
  },
  {
    scope: "state_upper",
    officeCanonicalName: "State Senator",
    aliasText: "Member of the State Senate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "House Delegate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Assembly Member",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Representative",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Representative in the General Assembly",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Representative in General Court",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Delegate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member, House of Delegates",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member of the Assembly",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member of the State Assembly",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "United States Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "U.S. Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "United States Representative",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "Member, House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "U.S. House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "US House of Representatives",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "District Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "County District Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "Prosecuting Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "County Prosecutor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Superior Court Judge",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Probate Judge",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Judge",
  },
  {
    scope: "place",
    officeCanonicalName: "Place Level Judge",
    aliasText: "Municipal Judge",
  },
  {
    scope: "place",
    officeCanonicalName: "Place Level Judge",
    aliasText: "Judge",
  },
];

function assertNoDuplicateSeedKeys(rows: readonly SeedOffice[]): void {
  const seen = new Set<string>();
  for (const row of rows) {
    const key = `${row.scope}::${row.canonicalName.trim().toLowerCase()}`;
    if (seen.has(key)) {
      throw new Error(`Duplicate office seed key: ${key}`);
    }
    seen.add(key);
  }
}

function assertNoDuplicateSeedAliasKeys(rows: readonly SeedOfficeAlias[]): void {
  const seen = new Set<string>();
  for (const row of rows) {
    const normalizedAlias = normalizeElectionTitleKey(row.aliasText);
    const key = `${row.scope}::${normalizedAlias}`;
    if (seen.has(key)) {
      throw new Error(`Duplicate office alias seed key: ${key}`);
    }
    seen.add(key);
  }
}

async function upsertOffice(client: PoolClient, row: SeedOffice): Promise<SeedOutcome> {
  const updated = await client.query<{ id: string }>(
    `
      UPDATE public.offices
      SET summary = $3,
          updated_at = now()
      WHERE scope = $1
        AND canonical_name = $2
        AND summary IS DISTINCT FROM $3
      RETURNING id
    `,
    [row.scope, row.canonicalName, row.summary]
  );
  if (updated.rowCount === 1) {
    return "updated";
  }

  const inserted = await client.query<{ id: string }>(
    `
      INSERT INTO public.offices (scope, canonical_name, summary)
      VALUES ($1, $2, $3)
      ON CONFLICT (scope, canonical_name) DO NOTHING
      RETURNING id
    `,
    [row.scope, row.canonicalName, row.summary]
  );
  if (inserted.rowCount === 1) {
    return "inserted";
  }

  return "unchanged";
}

async function resolveOfficeIdByScopeAndName(
  client: PoolClient,
  row: SeedOfficeAlias
): Promise<string> {
  const result = await client.query<{ id: string }>(
    `
      SELECT id
      FROM public.offices
      WHERE scope = $1
        AND canonical_name = $2
      LIMIT 1
    `,
    [row.scope, row.officeCanonicalName]
  );

  const officeId = result.rows?.[0]?.id;
  if (!officeId) {
    throw new Error(
      `Missing canonical office for alias seed: scope=${row.scope} canonical_name=${row.officeCanonicalName}`
    );
  }
  return officeId;
}

async function upsertOfficeAlias(client: PoolClient, row: SeedOfficeAlias): Promise<SeedOutcome> {
  const officeId = await resolveOfficeIdByScopeAndName(client, row);
  const normalizedAlias = normalizeElectionTitleKey(row.aliasText);
  if (!normalizedAlias) {
    throw new Error(
      `Alias seed normalized to empty key: scope=${row.scope} alias_text=${JSON.stringify(row.aliasText)}`
    );
  }

  const updated = await client.query<{ id: string }>(
    `
      UPDATE public.office_title_aliases
      SET office_id = $1,
          alias_text = $4,
          updated_at = now()
      WHERE scope = $2
        AND normalized_alias = $3
        AND (
          office_id IS DISTINCT FROM $1
          OR alias_text IS DISTINCT FROM $4
        )
      RETURNING id
    `,
    [officeId, row.scope, normalizedAlias, row.aliasText]
  );
  if (updated.rowCount === 1) {
    return "updated";
  }

  const inserted = await client.query<{ id: string }>(
    `
      INSERT INTO public.office_title_aliases (
        office_id,
        scope,
        alias_text,
        normalized_alias
      )
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (scope, normalized_alias) DO NOTHING
      RETURNING id
    `,
    [officeId, row.scope, row.aliasText, normalizedAlias]
  );
  if (inserted.rowCount === 1) {
    return "inserted";
  }

  return "unchanged";
}

async function main(): Promise<void> {
  assertNoDuplicateSeedKeys(SEED_OFFICES);
  assertNoDuplicateSeedAliasKeys(SEED_OFFICE_ALIASES);

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = new Date();

  const outcomeCounts: Record<SeedOutcome, number> = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
  };
  const scopeCounts = new Map<ElectionDistrictType, number>();
  const aliasOutcomeCounts: Record<SeedOutcome, number> = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
  };
  const aliasScopeCounts = new Map<ElectionDistrictType, number>();

  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    for (const row of SEED_OFFICES) {
      const outcome = await upsertOffice(client, row);
      outcomeCounts[outcome] += 1;
      scopeCounts.set(row.scope, (scopeCounts.get(row.scope) ?? 0) + 1);
    }
    for (const row of SEED_OFFICE_ALIASES) {
      const outcome = await upsertOfficeAlias(client, row);
      aliasOutcomeCounts[outcome] += 1;
      aliasScopeCounts.set(row.scope, (aliasScopeCounts.get(row.scope) ?? 0) + 1);
    }
    await client.query("COMMIT");
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK");
    }
    throw error;
  } finally {
    client?.release();
    await pool.end();
  }

  const output = {
    type: "offices_seed",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    total_seed_rows: SEED_OFFICES.length,
    outcomes: outcomeCounts,
    by_scope: Array.from(scopeCounts.entries()).map(([scope, count]) => ({ scope, count })),
    alias_seed_rows: SEED_OFFICE_ALIASES.length,
    alias_outcomes: aliasOutcomeCounts,
    alias_by_scope: Array.from(aliasScopeCounts.entries()).map(([scope, count]) => ({ scope, count })),
  };

  console.log(JSON.stringify(output, null, 2));
}

main().catch((error) => {
  console.error("offices seed failed:", error);
  process.exit(1);
});

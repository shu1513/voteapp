import { readFileSync } from "node:fs";
import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { normalizeFinanceLabel } from "../../../src/pipeline/finance/financeLabelClassifier.js";

/**
 * Lockstep guard for finance-label normalization.
 *
 * finance_label_classifications.normalized_label is written by the TypeScript
 * normalizeFinanceLabel, and the ballot-lookup evidence queries recompute the
 * key at read time with public.normalize_finance_label (created in migration
 * 185, converged with TypeScript in migration 186). If the two
 * implementations ever disagree, the classification join silently matches
 * nothing — no error, just missing evidence. This test feeds a corpus of
 * messy labels through both and asserts identical output.
 *
 * The SQL function implements the non-occupation branch of the TypeScript
 * function: every evidence query filters to 'donor'/'employer' labels before
 * joining through it, so the corpus runs the TypeScript side as "employer"
 * (the "donor" branch is the same code path). The occupation branch skips
 * suffix stripping entirely and must never be pointed at this function; a
 * divergence test below pins that difference.
 *
 * Needs a live Postgres (DATABASE_URL); it installs the function from the
 * migration file into pg_temp, so it never touches the target schema and does
 * not require migration 186 to be applied. CI runs it in the migrate job,
 * which provides a Postgres service; the unit-test job skips it.
 */

const MIGRATION_URL = new URL(
  "../../../../db/migrations/186_normalize_finance_label_ts_parity.sql",
  import.meta.url
);

const databaseUrl = process.env.DATABASE_URL;

// Messy labels, including accented Latin-1/Latin-Extended-A names (folded by
// migration 186's translate() map before upper(), so no locale dependence)
// and labels made entirely of business suffixes (fallback to the unstripped
// form). Characters with multi-character NFKD forms (ligatures, fullwidth)
// stay out of this corpus — the known-divergence test below pins them.
const PARITY_CORPUS: readonly string[] = [
  // suffix variants, one per BUSINESS_SUFFIX_PATTERN alternative
  "Acme Widgets, Inc.",
  "Acme Widgets Incorporated",
  "Energy Transfer LLC",
  "Energy Transfer, L.L.C.",
  "Energy Transfer LP",
  "Energy Transfer, L.P.",
  "PricewaterhouseCoopers LLP",
  "Baker Botts L.L.P.",
  "Grand Hotels Ltd",
  "Grand Hotels Limited",
  "Smith & Wesson Co",
  "Ford Motor Company",
  "Lockheed Martin Corp.",
  "Exxon Mobil Corporation",
  "Johnson Controls International plc",
  // ampersands, punctuation, casing, spacing
  "AT&T Corp",
  "Ben & Jerry's Homemade Inc",
  "J.P. Morgan Chase & Co.",
  "O'Brien & Sons, Ltd.",
  "Deloitte & Touche LLP",
  "A & B",
  "  Acme   Widgets ,  Inc.  ",
  "acme widgets inc",
  "ACME WIDGETS INC",
  "Acme\tWidgets\nInc",
  // digits and symbol-heavy names
  "3M Company",
  "7-Eleven, Inc.",
  "E*TRADE Financial",
  "Yahoo! Inc.",
  "Re/Max Holdings",
  "Toys \"R\" Us",
  "Macy's Inc",
  "U.S. Steel Corporation",
  "T-Mobile US, Inc.",
  "Blue Cross/Blue Shield",
  "Wal-Mart Stores, Inc.",
  "The Coca-Cola Company",
  "PepsiCo, Inc.",
  "The Home Depot, Inc.",
  "Berkshire Hathaway Inc",
  "Koch Industries, Inc.",
  "Chevron U.S.A. Inc.",
  // suffix token inside a word or split by punctuation
  "Co-Op Grocers",
  "Costco Wholesale",
  "Corporate Travel Partners",
  "Colorado Contractors Assn",
  "C.O.R.P Consulting",
  "The Co.",
  // unions, PACs, employer-field noise
  "SEIU Local 1000",
  "IBEW LOCAL 58",
  "AFL-CIO",
  "Plumbers & Pipefitters Local 189",
  "National Assn. of Realtors",
  "Law Offices of John Smith",
  "Smith, Jones & Associates, P.C.",
  "Self-Employed",
  "Self Employed",
  "Not Employed",
  "Retired",
  "Homemaker",
  "None",
  "N/A",
  // accented names (previously a known divergence, converged by 186)
  "Café Inc",
  "Nestlé S.A.",
  "L'Oréal USA, Inc.",
  "Müller & Söhne GmbH",
  "Peña Nieto Ltd.",
  "São Paulo Investments",
  "Škoda Auto a.s.",
  "Łódź Holdings",
  "Zürich Insurance Group",
  "Crème Brûlée Bakery LLC",
  "Señor Frog's",
  "Björk Seafood Co",
  "ÀÁÂÃÄÅ Test",
  "Straße GmbH",
  // all-suffix labels (previously a known divergence, converged by 186)
  "LLC",
  "L.L.C.",
  "Inc.",
  "CO",
  "Corp Company Inc",
  // degenerate but agreeing inputs
  "",
  "   ",
  "123",
  "&",
];

describe.skipIf(!databaseUrl)("normalize_finance_label parity (requires DATABASE_URL)", () => {
  let client: Client;

  beforeAll(async () => {
    const migrationSql = readFileSync(MIGRATION_URL, "utf8");
    const functionMatch =
      /CREATE (?:OR REPLACE )?FUNCTION public\.normalize_finance_label[\s\S]*?\$\$;/.exec(
        migrationSql
      );
    if (!functionMatch) {
      throw new Error("CREATE FUNCTION public.normalize_finance_label not found in migration 186");
    }

    // pg_temp is per-connection, so a single Client (not a Pool) keeps the
    // temp function visible to every query below without touching the schema.
    client = new Client({ connectionString: databaseUrl });
    await client.connect();
    await client.query(
      functionMatch[0].replace("public.normalize_finance_label", "pg_temp.normalize_finance_label")
    );
  });

  afterAll(async () => {
    await client?.end();
  });

  async function normalizeInPostgres(labels: readonly string[]): Promise<string[]> {
    const result = await client.query<{ normalized: string }>(
      `
        SELECT pg_temp.normalize_finance_label(t.value) AS normalized
        FROM unnest($1::text[]) WITH ORDINALITY AS t(value, ord)
        ORDER BY t.ord
      `,
      [labels]
    );
    return result.rows.map((row) => row.normalized);
  }

  it("matches the TypeScript normalizeFinanceLabel on the parity corpus", async () => {
    const fromPostgres = await normalizeInPostgres(PARITY_CORPUS);
    const fromTypeScript = PARITY_CORPUS.map((label) => normalizeFinanceLabel(label, "employer"));
    expect(fromPostgres).toEqual(fromTypeScript);
  });

  it("returns NULL for NULL input", async () => {
    const result = await client.query<{ normalized: string | null }>(
      "SELECT pg_temp.normalize_finance_label(NULL) AS normalized"
    );
    expect(result.rows[0]?.normalized).toBeNull();
  });

  // Remaining known divergences after migration 186 converged the diacritic
  // and all-suffix cases. These tests pin them down so a future change to
  // either side is deliberate, not an accident.
  it("known divergence: occupation labels keep business suffixes in TypeScript", async () => {
    // The function has no label-type parameter and always strips suffixes,
    // matching only the non-occupation TypeScript branch. Evidence queries
    // filter to 'donor'/'employer' before joining through it; if occupation
    // labels ever need this join, the function needs a label-type parameter.
    const [fromPostgres] = await normalizeInPostgres(["Acme Corp"]);
    expect(normalizeFinanceLabel("Acme Corp", "occupation")).toBe("ACME CORP");
    expect(fromPostgres).toBe("ACME");
  });

  it("known divergence: multi-character NFKD forms fold to letters only in TypeScript", async () => {
    // translate() is strictly one-to-one, so characters whose NFKD form is
    // multi-character (the ﬁ ligature here; also Ĳ ĳ Ŀ ŀ ŉ and fullwidth
    // forms) become a space in SQL while TypeScript folds them to letters.
    // The exact SQL output depends on the database locale's upper(), so only
    // the disagreement itself is pinned.
    const [fromPostgres] = await normalizeInPostgres(["ﬁnance Inc"]);
    expect(normalizeFinanceLabel("ﬁnance Inc", "employer")).toBe("FINANCE");
    expect(fromPostgres).not.toBe("FINANCE");
  });
});

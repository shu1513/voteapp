import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";
import type { APIRequestContext } from "@playwright/test";
import { TERMS_VERSION } from "@voteapp/api-client";

const HERE = path.dirname(fileURLToPath(import.meta.url));

export const API_SERVER_LOG = path.resolve(HERE, ".api-server.log");

/** DATABASE_URL from backend/.env — the same database the backend under test
 * uses. Read-only here (district discovery). */
function databaseUrl(): string {
  const envFile = readFileSync(path.resolve(HERE, "../../backend/.env"), "utf8");
  const line = envFile.split("\n").find((candidate) => candidate.startsWith("DATABASE_URL="));
  if (!line) {
    throw new Error("DATABASE_URL not found in backend/.env");
  }
  return line.slice("DATABASE_URL=".length).trim();
}

/**
 * A district whose ballot has upcoming elections — the smoke tests need real
 * researched data. Returns null when the local database has none (specs skip
 * with a clear message instead of failing on an empty database).
 */
export async function findDistrictWithElections(): Promise<string | null> {
  const client = new Client({ connectionString: databaseUrl() });
  await client.connect();
  try {
    const result = await client.query<{ district_id: string }>(
      `SELECT district_id
       FROM elections
       WHERE election_date >= CURRENT_DATE
       GROUP BY district_id
       ORDER BY count(*) DESC
       LIMIT 1`
    );
    return result.rows[0]?.district_id ?? null;
  } finally {
    await client.end();
  }
}

export function uniqueEmail(): string {
  return `e2e-${Date.now()}-${Math.floor(Math.random() * 10_000)}@example.com`;
}

/**
 * The console auth mailer prints
 * "[auth-mailer:console] verification email for <email>: <url>" to the
 * backend log captured by playwright.config.ts. Polls until the line for
 * this address appears.
 */
export async function readVerificationToken(email: string, timeoutMs = 15_000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  const needle = `verification email for ${email}: `;
  while (Date.now() < deadline) {
    let log = "";
    try {
      log = readFileSync(API_SERVER_LOG, "utf8");
    } catch {
      // Log not written yet.
    }
    const line = log
      .split("\n")
      .reverse()
      .find((candidate) => candidate.includes(needle));
    if (line) {
      const url = new URL(line.slice(line.indexOf(needle) + needle.length).trim());
      const token = url.searchParams.get("token");
      if (token) {
        return token;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`No verification email for ${email} in ${API_SERVER_LOG}`);
}

export const E2E_PASSWORD = "correct horse battery staple";

/** Registers and verifies a throwaway account via the API, then logs the
 * request context in (the session cookie lands on the browser context). */
export async function registerVerifiedUser(request: APIRequestContext): Promise<string> {
  const email = uniqueEmail();
  const register = await request.post("/api/auth/register", {
    data: { email, password: E2E_PASSWORD, first_name: "Smoke", accepted_terms_version: TERMS_VERSION },
  });
  if (!register.ok()) {
    throw new Error(`register failed: ${register.status()} ${await register.text()}`);
  }
  const token = await readVerificationToken(email);
  const verify = await request.post("/api/auth/verify-email", { data: { token } });
  if (!verify.ok()) {
    throw new Error(`verify failed: ${verify.status()}`);
  }
  const login = await request.post("/api/auth/login", { data: { email, password: E2E_PASSWORD } });
  if (!login.ok()) {
    throw new Error(`login failed: ${login.status()}`);
  }
  return email;
}

/** Deletes (soft-deletes, per backend design) the account owning the
 * request context's session. */
export async function deleteAccount(request: APIRequestContext): Promise<void> {
  const response = await request.delete("/api/me", { data: { password: E2E_PASSWORD } });
  if (!response.ok()) {
    throw new Error(`account cleanup failed: ${response.status()} ${await response.text()}`);
  }
}

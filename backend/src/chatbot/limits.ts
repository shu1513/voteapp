// Caps, durable budget, and the exact answer cache — docs/plans/chatbot-rag.md
// component 7. Four cost layers guard the LLM path:
//   1. per-user daily cap (Redis INCR + TTL, ephemeral — losing counters on a
//      restart of the free Key Value store is harmless)
//   2. durable global daily token budget (chatbot.daily_budget, atomic
//      reserve-then-reconcile — survives restarts, concurrency-safe)
//   3. retrieval-only fallback on ANY limiter/LLM failure
//   4. provider dashboard spend limit (outside this codebase)

import { createHash, createHmac } from "node:crypto";
import type { Pool } from "pg";

// ── User identity hashing ────────────────────────────────────────────────
// The HMAC of the user id is used ONLY for the Redis cap key and the
// provider safety_identifier — transient, never logged, never stored in
// Postgres (BEHAVIOR.md rule 11). Keyed with the LLM API key: already
// secret, already present exactly when the LLM path exists, and rotating it
// merely resets day-scoped counters.

export function hashUserId(userId: string, secret: string): string {
  return createHmac("sha256", secret).update(userId).digest("hex");
}

// ── Redis surface ────────────────────────────────────────────────────────
// Minimal structural type so tests fake it and node-redis v4 satisfies it.

export type LimitsRedis = {
  incr: (key: string) => Promise<number>;
  expire: (key: string, seconds: number) => Promise<unknown>;
  get: (key: string) => Promise<string | null>;
  set: (key: string, value: string, options?: { EX?: number }) => Promise<unknown>;
};

/** UTC day string (YYYY-MM-DD): one budget row and one cap window per day. */
export function utcDay(now: Date = new Date()): string {
  return now.toISOString().slice(0, 10);
}

// ── Per-user daily cap ───────────────────────────────────────────────────

const USER_CAP_TTL_SECONDS = 26 * 3600; // day-scoped key + slack; TTL is GC, the day in the key is the boundary

/**
 * Counts this ask against the user's daily LLM allowance; true = under the
 * cap. Fail CLOSED on Redis errors (returns false → retrieval-only): the
 * caller treats any limiter uncertainty as "no LLM", and auth needs Redis
 * anyway so a down Redis already degrades the session path.
 */
export async function consumeUserDailyAllowance(
  redis: LimitsRedis,
  hashedUserId: string,
  limit: number,
  now: Date = new Date()
): Promise<boolean> {
  try {
    const key = `chatbot:usercap:${utcDay(now)}:${hashedUserId}`;
    const count = await redis.incr(key);
    if (count === 1) {
      await redis.expire(key, USER_CAP_TTL_SECONDS);
    }
    return count <= limit;
  } catch (error) {
    console.warn(
      "chatbot user cap check failed; falling back to retrieval-only:",
      error instanceof Error ? error.message : String(error)
    );
    return false;
  }
}

// ── Durable global daily token budget ────────────────────────────────────

export type BudgetReservation = {
  day: string;
  estimatedTokens: number;
};

/**
 * Reserves an estimated worst-case token count against today's budget row,
 * atomically. Returns the reservation on success, null when the budget is
 * exhausted (→ retrieval-only for the rest of the day) or on any DB error
 * (fail closed — never spend unaccounted money).
 *
 * Exactly the plan's two-statement transaction: the INSERT ... ON CONFLICT
 * DO NOTHING guarantees the row exists (a plain UPDATE alone would match no
 * row on a fresh day and wrongly report "exhausted"), then the conditional
 * UPDATE ... RETURNING reserves only if it fits. Concurrent requests
 * serialize on the row lock and cannot overshoot.
 */
export async function reserveDailyBudget(
  db: Pool,
  estimatedTokens: number,
  budgetCap: number,
  now: Date = new Date()
): Promise<BudgetReservation | null> {
  const day = utcDay(now);
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO chatbot.daily_budget (day) VALUES ($1::date) ON CONFLICT (day) DO NOTHING`,
      [day]
    );
    const update = await client.query(
      `
        UPDATE chatbot.daily_budget
        SET tokens_reserved = tokens_reserved + $2
        WHERE day = $1::date
          AND tokens_reserved + $2 <= $3
        RETURNING tokens_reserved
      `,
      [day, estimatedTokens, budgetCap]
    );
    await client.query("COMMIT");
    return update.rowCount === 1 ? { day, estimatedTokens } : null;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    console.warn(
      "chatbot budget reservation failed; falling back to retrieval-only:",
      error instanceof Error ? error.message : String(error)
    );
    return null;
  } finally {
    client.release();
  }
}

/**
 * Replaces the reservation's estimate with the actual billed tokens once the
 * call finished (or failed with known partial usage). Never lets the row go
 * negative; a failed reconcile only leaves the pessimistic estimate in
 * place, which can never over-spend.
 */
export async function reconcileDailyBudget(
  db: Pool,
  reservation: BudgetReservation,
  actualTokens: number
): Promise<void> {
  try {
    await db.query(
      `
        UPDATE chatbot.daily_budget
        SET tokens_reserved = GREATEST(0, tokens_reserved + ($3 - $2))
        WHERE day = $1::date
      `,
      [reservation.day, reservation.estimatedTokens, actualTokens]
    );
  } catch (error) {
    console.warn(
      "chatbot budget reconcile failed; keeping the pessimistic reservation:",
      error instanceof Error ? error.message : String(error)
    );
  }
}

// ── Exact answer cache ───────────────────────────────────────────────────

export const ANSWER_CACHE_TTL_SECONDS = 24 * 3600;

export type AnswerCacheKeyParts = {
  /** Redacted + normalized question text (incl. any carried-over previous
   * turn — it changes the answer). */
  questionNorm: string;
  /** Everything that scopes the answer beyond its text: state and/or page
   * context. Seattle and Boston must never share an entry. */
  scopeKey: string;
  /** Active generation id: a reindex flip instantly invalidates the cache. */
  generationId: string;
  model: string;
  promptVersion: string;
};

export function answerCacheKey(parts: AnswerCacheKeyParts): string {
  // Newline separator: question_norm has collapsed whitespace and the other
  // parts are ids/identifiers, so no part contains one — distinct tuples can
  // never collide into a single digest input.
  const digest = createHash("sha256")
    .update(
      [parts.questionNorm, parts.scopeKey, parts.generationId, parts.model, parts.promptVersion].join("\n")
    )
    .digest("hex");
  return `chatbot:answer:${digest}`;
}

/** Cached JSON value, or null on miss/parse failure/Redis error (a cache
 * problem must never fail the ask). */
export async function getCachedAnswer(redis: LimitsRedis, key: string): Promise<unknown | null> {
  try {
    const raw = await redis.get(key);
    return raw === null ? null : (JSON.parse(raw) as unknown);
  } catch {
    return null;
  }
}

export async function setCachedAnswer(redis: LimitsRedis, key: string, value: unknown): Promise<void> {
  try {
    await redis.set(key, JSON.stringify(value), { EX: ANSWER_CACHE_TTL_SECONDS });
  } catch {
    // Best-effort: a failed cache write costs a future cache miss, nothing else.
  }
}

// Anonymous 👍/👎 answer feedback — docs/plans/chatbot-improvements-2026-08.md
// PR 2.
//
// Every ask response carries an opaque feedback token; POST
// /api/chatbot/feedback exchanges it plus a verdict for one row in
// chatbot.answer_feedback. The token is stateless: an HMAC-signed payload of
// (answered_by, nonce), so nothing is stored at ask time and question logging
// stays fire-and-forget. The signature makes tokens non-enumerable and the
// answered_by value non-forgeable; the UNIQUE nonce makes each token
// one-shot server-side.
//
// The signing secret comes from CHATBOT_FEEDBACK_SECRET (see
// runAddressApiServer wiring): the prod API runs on Render's free plan,
// which spins the process down after ~15 idle minutes, so a per-boot secret
// would routinely invalidate tokens before the vote arrives (the wake-up
// serving the vote POST is a fresh boot). Unset → per-boot random fallback
// (dev/operator runs): tokens then die with the process, dropping those
// votes — the widget surfaces the rejection instead of faking success.

import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type { Pool } from "pg";

export const FEEDBACK_VERDICTS = ["up", "down"] as const;
export type FeedbackVerdict = (typeof FEEDBACK_VERDICTS)[number];

/** Sanity cap for inbound tokens: real ones are well under this, and the
 * verifier must not HMAC unbounded attacker input. */
export const MAX_FEEDBACK_TOKEN_LENGTH = 400;

export type FeedbackTokens = {
  mint: (answeredBy: string) => string;
  /** null = tampered, malformed, or signed by another process boot. */
  verify: (token: string) => { answeredBy: string; nonce: string } | null;
};

function sign(payload: string, secret: Buffer): string {
  return createHmac("sha256", secret).update(payload).digest("base64url");
}

export function createFeedbackTokens(secret: Buffer = randomBytes(32)): FeedbackTokens {
  return {
    mint(answeredBy: string): string {
      const payload = Buffer.from(
        JSON.stringify({ a: answeredBy, n: randomBytes(16).toString("hex") })
      ).toString("base64url");
      return `${payload}.${sign(payload, secret)}`;
    },
    verify(token: string): { answeredBy: string; nonce: string } | null {
      if (token.length > MAX_FEEDBACK_TOKEN_LENGTH) {
        return null;
      }
      const [payload, signature, extra] = token.split(".");
      if (!payload || !signature || extra !== undefined) {
        return null;
      }
      const expected = Buffer.from(sign(payload, secret));
      const actual = Buffer.from(signature);
      if (expected.length !== actual.length || !timingSafeEqual(expected, actual)) {
        return null;
      }
      // Parse AFTER the signature check: only self-minted JSON is ever parsed.
      let parsed: unknown;
      try {
        parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
      } catch {
        return null;
      }
      if (typeof parsed !== "object" || parsed === null) {
        return null;
      }
      const record = parsed as Record<string, unknown>;
      if (typeof record.a !== "string" || typeof record.n !== "string" || record.n.length === 0) {
        return null;
      }
      return { answeredBy: record.a, nonce: record.n };
    },
  };
}

/**
 * Records one vote. "ok" covers the duplicate case too (ON CONFLICT DO
 * NOTHING): a re-vote on an already-used token is idempotent, not an error —
 * the first verdict stands. "invalid_token" → the caller 400s.
 */
export async function submitAnswerFeedback(
  db: Pool,
  tokens: FeedbackTokens,
  token: string,
  verdict: FeedbackVerdict
): Promise<"ok" | "invalid_token"> {
  const payload = tokens.verify(token);
  if (!payload) {
    return "invalid_token";
  }
  // Targetless ON CONFLICT on purpose: naming the arbiter column
  // (`ON CONFLICT (token_nonce)`) makes Postgres require SELECT on that
  // column, which the INSERT-only voteapp_api role does not have — prod
  // votes 500'd with "permission denied for table answer_feedback". The
  // only realistic conflict is the token_nonce UNIQUE anyway (the PK is a
  // bigserial).
  await db.query(
    `
      INSERT INTO chatbot.answer_feedback (answered_by, verdict, token_nonce)
      VALUES ($1, $2, $3)
      ON CONFLICT DO NOTHING
    `,
    [payload.answeredBy, verdict, payload.nonce]
  );
  return "ok";
}

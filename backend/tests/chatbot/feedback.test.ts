import { describe, expect, it } from "vitest";
import { randomBytes } from "node:crypto";
import type { Pool } from "pg";

import {
  MAX_FEEDBACK_TOKEN_LENGTH,
  createFeedbackTokens,
  submitAnswerFeedback,
} from "../../src/chatbot/feedback.js";

describe("feedback tokens", () => {
  it("round-trips answered_by through mint/verify with a fresh nonce per token", () => {
    const tokens = createFeedbackTokens();
    const first = tokens.mint("llm");
    const second = tokens.mint("llm");
    expect(first).not.toBe(second);
    const payload = tokens.verify(first);
    expect(payload?.answeredBy).toBe("llm");
    expect(payload?.nonce).toMatch(/^[0-9a-f]{32}$/);
    expect(tokens.verify(second)?.nonce).not.toBe(payload?.nonce);
  });

  it("rejects tampered payloads and signatures", () => {
    const tokens = createFeedbackTokens();
    const token = tokens.mint("retrieval");
    const [payload, signature] = token.split(".") as [string, string];
    // Forge a payload claiming a different answer path; keep the old signature.
    const forged = Buffer.from(JSON.stringify({ a: "llm", n: "0".repeat(32) })).toString("base64url");
    expect(tokens.verify(`${forged}.${signature}`)).toBeNull();
    // Flip the signature.
    const flipped = signature.endsWith("A") ? `${signature.slice(0, -1)}B` : `${signature.slice(0, -1)}A`;
    expect(tokens.verify(`${payload}.${flipped}`)).toBeNull();
  });

  it("rejects tokens signed by a different process boot (secret mismatch)", () => {
    const minter = createFeedbackTokens(randomBytes(32));
    const verifier = createFeedbackTokens(randomBytes(32));
    expect(verifier.verify(minter.mint("cache"))).toBeNull();
  });

  it("rejects malformed input without throwing", () => {
    const tokens = createFeedbackTokens();
    expect(tokens.verify("")).toBeNull();
    expect(tokens.verify("no-dot")).toBeNull();
    expect(tokens.verify("a.b.c")).toBeNull();
    expect(tokens.verify(`${Buffer.from("not json").toString("base64url")}.sig`)).toBeNull();
    expect(tokens.verify("x".repeat(MAX_FEEDBACK_TOKEN_LENGTH + 1))).toBeNull();
  });

  it("caps minted tokens well under the inbound length limit", () => {
    const tokens = createFeedbackTokens();
    // Longest real answered_by values are 'intent:<name>' strings.
    expect(tokens.mint("intent:my_issues_ballot").length).toBeLessThan(MAX_FEEDBACK_TOKEN_LENGTH);
  });
});

describe("submitAnswerFeedback", () => {
  function fakeDb() {
    const inserts: unknown[][] = [];
    const statements: string[] = [];
    const db = {
      query: (sql: string, values: unknown[]) => {
        statements.push(sql);
        inserts.push(values);
        return Promise.resolve({ rowCount: 1, rows: [] });
      },
    } as unknown as Pool;
    return { db, inserts, statements };
  }

  it("inserts the token's answered_by with the caller's verdict", async () => {
    const tokens = createFeedbackTokens();
    const { db, inserts } = fakeDb();
    const result = await submitAnswerFeedback(db, tokens, tokens.mint("llm"), "down");
    expect(result).toBe("ok");
    expect(inserts).toHaveLength(1);
    expect(inserts[0]?.[0]).toBe("llm");
    expect(inserts[0]?.[1]).toBe("down");
    expect(inserts[0]?.[2]).toMatch(/^[0-9a-f]{32}$/);
  });

  it("names no conflict target, which the INSERT-only API role cannot read", async () => {
    // Regression (prod, 2026-08-19): `ON CONFLICT (token_nonce)` makes
    // Postgres require SELECT on the arbiter column, so every vote 500'd
    // with "permission denied for table answer_feedback" under voteapp_api's
    // INSERT-only grant. The targetless form needs no SELECT.
    const tokens = createFeedbackTokens();
    const { db, statements } = fakeDb();
    await submitAnswerFeedback(db, tokens, tokens.mint("llm"), "up");
    expect(statements[0]).toMatch(/ON CONFLICT\s+DO NOTHING/i);
    expect(statements[0]).not.toMatch(/ON CONFLICT\s*\(/i);
  });

  it("returns invalid_token without touching the DB on a bad token", async () => {
    const tokens = createFeedbackTokens();
    const { db, inserts } = fakeDb();
    expect(await submitAnswerFeedback(db, tokens, "garbage", "up")).toBe("invalid_token");
    expect(inserts).toHaveLength(0);
  });
});

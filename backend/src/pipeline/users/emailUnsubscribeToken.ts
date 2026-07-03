import { createHmac, timingSafeEqual } from "node:crypto";

import { isUuid } from "../../utils/uuid.js";

// Stateless signed unsubscribe token: `v1.<userId>.<base64url hmac>`.
// HMAC-SHA256 over "v1.<userId>" with a server-side secret — no storage, no
// expiry. Deliberately long-lived: unsubscribe links sit in inboxes for
// months, and the action they authorize (turning the digest off for the
// token's own user) is idempotent and harmless to replay.

const TOKEN_VERSION = "v1";
const MIN_SECRET_LENGTH = 32;

function assertSecret(secret: string): string {
  const normalized = secret.trim();
  if (normalized.length < MIN_SECRET_LENGTH) {
    throw new Error(`Unsubscribe token secret must be at least ${MIN_SECRET_LENGTH} characters`);
  }
  return normalized;
}

function signPayload(payload: string, secret: string): Buffer {
  return createHmac("sha256", secret).update(payload).digest();
}

export function createEmailUnsubscribeToken(userId: string, secret: string): string {
  const normalizedUserId = userId.trim();
  if (!isUuid(normalizedUserId)) {
    throw new Error("userId must be a UUID");
  }
  const normalizedSecret = assertSecret(secret);
  const payload = `${TOKEN_VERSION}.${normalizedUserId}`;
  const signature = signPayload(payload, normalizedSecret).toString("base64url");
  return `${payload}.${signature}`;
}

/** Returns the token's userId when the signature verifies, otherwise null. */
export function verifyEmailUnsubscribeToken(token: string, secret: string): string | null {
  const normalizedSecret = assertSecret(secret);
  const parts = token.trim().split(".");
  if (parts.length !== 3) {
    return null;
  }
  const [version, userId, signature] = parts;
  if (version !== TOKEN_VERSION || !userId || !isUuid(userId) || !signature) {
    return null;
  }

  const expected = signPayload(`${version}.${userId}`, normalizedSecret);
  let provided: Buffer;
  try {
    provided = Buffer.from(signature, "base64url");
  } catch {
    return null;
  }
  if (provided.length !== expected.length || !timingSafeEqual(provided, expected)) {
    return null;
  }
  return userId;
}

import { createHash, randomBytes } from "node:crypto";

import { Algorithm, Version, hash as argon2Hash, verify as argon2Verify } from "@node-rs/argon2";

export const AUTH_PASSWORD_MIN_LENGTH = 12;
export const AUTH_PASSWORD_MAX_LENGTH = 1024;
export const AUTH_TOKEN_BYTE_LENGTH = 32;
export const AUTH_SESSION_ID_BYTE_LENGTH = 32;

export const AUTH_TOKEN_PURPOSES = ["email_verify", "password_reset"] as const;

export type AuthTokenPurpose = (typeof AUTH_TOKEN_PURPOSES)[number];

const ARGON2_PASSWORD_OPTIONS = {
  algorithm: Algorithm.Argon2id,
  version: Version.V0x13,
  memoryCost: 19_456,
  timeCost: 3,
  parallelism: 1,
  outputLen: 32,
} as const;

function sha256Hex(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function assertPasswordPolicy(password: string): void {
  if (typeof password !== "string") {
    throw new TypeError("Password must be a string");
  }
  if (password.length < AUTH_PASSWORD_MIN_LENGTH) {
    throw new TypeError(`Password must be at least ${AUTH_PASSWORD_MIN_LENGTH} characters long`);
  }
  if (password.length > AUTH_PASSWORD_MAX_LENGTH) {
    throw new TypeError(`Password must be at most ${AUTH_PASSWORD_MAX_LENGTH} characters long`);
  }
  if (password.trim().length === 0) {
    throw new TypeError("Password must include at least one non-whitespace character");
  }
}

function assertPasswordVerifiable(password: string): void {
  if (typeof password !== "string") {
    throw new TypeError("Password must be a string");
  }
  if (password.length > AUTH_PASSWORD_MAX_LENGTH) {
    throw new TypeError(`Password must be at most ${AUTH_PASSWORD_MAX_LENGTH} characters long`);
  }
}

export function validatePasswordPolicy(password: string): void {
  assertPasswordPolicy(password);
}

export async function hashPassword(password: string): Promise<string> {
  assertPasswordPolicy(password);
  return argon2Hash(password, ARGON2_PASSWORD_OPTIONS);
}

export async function verifyPassword(passwordHash: string, password: string): Promise<boolean> {
  assertPasswordVerifiable(password);
  if (password.trim().length === 0) {
    return false;
  }
  try {
    return await argon2Verify(passwordHash, password, {
      algorithm: Algorithm.Argon2id,
      version: Version.V0x13,
    });
  } catch {
    return false;
  }
}

export function generateAuthToken(): { rawToken: string; tokenHash: string } {
  const rawToken = randomBytes(AUTH_TOKEN_BYTE_LENGTH).toString("base64url");
  return {
    rawToken,
    tokenHash: sha256Hex(rawToken),
  };
}

export function hashAuthToken(token: string): string {
  if (typeof token !== "string") {
    throw new TypeError("Token must be a string");
  }
  const normalized = token.trim();
  if (normalized.length === 0) {
    throw new TypeError("Token must be a non-empty string");
  }
  return sha256Hex(normalized);
}

export function generateSessionId(): string {
  return randomBytes(AUTH_SESSION_ID_BYTE_LENGTH).toString("base64url");
}

export function hashSessionId(sessionId: string): string {
  if (typeof sessionId !== "string") {
    throw new TypeError("Session ID must be a string");
  }
  const normalized = sessionId.trim();
  if (normalized.length === 0) {
    throw new TypeError("Session ID must be a non-empty string");
  }
  return sha256Hex(normalized);
}

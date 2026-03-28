import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { DEFAULT_AI_CANDIDATE } from "../ai/aiCandidates.js";
import type { AiProvider } from "../ai/types.js";

export type PipelineEnv = {
  DATABASE_URL: string;
  REDIS_URL: string;
  AI_PROVIDER: AiProvider;
  AI_MODEL: string;
  AI_TIMEOUT_MS: number;
  PROMPT_VERSION: string;
  OPENAI_API_KEY?: string;
  ANTHROPIC_API_KEY?: string;
  GEMINI_API_KEY?: string;
};

let didAttemptDotEnvLoad = false;

function tryLoadDotEnvFile(path: string): boolean {
  if (!existsSync(path)) {
    return false;
  }
  try {
    process.loadEnvFile(path);
    return true;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to load environment file at ${path}: ${reason}`);
  }
}

/**
 * Loads .env once from common project paths when present.
 * Existing exported vars still take precedence over file values.
 */
function ensureDotEnvLoaded(): void {
  if (didAttemptDotEnvLoad) {
    return;
  }
  didAttemptDotEnvLoad = true;

  const moduleDir = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    resolve(process.cwd(), ".env"),
    resolve(process.cwd(), "backend", ".env"),
    resolve(moduleDir, "../../.env"),
  ];

  const seen = new Set<string>();
  for (const candidate of candidates) {
    if (seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    if (tryLoadDotEnvFile(candidate)) {
      return;
    }
  }
}

/**
 * Loads project .env once from common locations.
 * Useful for scripts that only need non-AI infra variables.
 */
export function loadProjectEnv(): void {
  ensureDotEnvLoaded();
}

/**
 * Reads a required environment variable, with optional fallback.
 */
function readEnv(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function readOptionalEnv(name: string): string | undefined {
  const value = process.env[name];
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function readAiProvider(): AiProvider {
  const raw = readEnv("AI_PROVIDER", DEFAULT_AI_CANDIDATE.provider);
  if (raw === "openai" || raw === "claude" || raw === "gemini") {
    return raw;
  }

  throw new Error(`Invalid AI_PROVIDER: ${raw}. Expected one of: openai, claude, gemini`);
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = readEnv(name, String(fallback));
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  return parsed;
}

/**
 * Returns normalized runtime configuration for the pipeline.
 */
export function getPipelineEnv(): PipelineEnv {
  ensureDotEnvLoaded();
  return {
    DATABASE_URL: readEnv("DATABASE_URL", "postgresql://localhost:5432/voteapp"),
    REDIS_URL: readEnv("REDIS_URL", "redis://localhost:6379"),
    AI_PROVIDER: readAiProvider(),
    AI_MODEL: readEnv("AI_MODEL", DEFAULT_AI_CANDIDATE.model),
    AI_TIMEOUT_MS: readPositiveIntegerEnv("AI_TIMEOUT_MS", 30000),
    PROMPT_VERSION: readEnv("PROMPT_VERSION", "state_resources_v2"),
    OPENAI_API_KEY: readOptionalEnv("OPENAI_API_KEY"),
    ANTHROPIC_API_KEY: readOptionalEnv("ANTHROPIC_API_KEY"),
    GEMINI_API_KEY: readOptionalEnv("GEMINI_API_KEY"),
  };
}

export type PipelineEnv = {
  DATABASE_URL: string;
  REDIS_URL: string;
  AI_MODEL: string;
  PROMPT_VERSION: string;
};

/**
 * Reads a required environment variable, with optional fallback.
 */
function readEnv(name: keyof PipelineEnv, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

/**
 * Returns normalized runtime configuration for the pipeline.
 */
export function getPipelineEnv(): PipelineEnv {
  return {
    DATABASE_URL: readEnv("DATABASE_URL", "postgresql://localhost:5432/voteapp"),
    REDIS_URL: readEnv("REDIS_URL", "redis://localhost:6379"),
    AI_MODEL: readEnv("AI_MODEL", "openai:gpt-5-mini"),
    PROMPT_VERSION: readEnv("PROMPT_VERSION", "state_resources_v1"),
  };
}

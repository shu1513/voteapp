import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const verifyHttpUrlReachabilityMock = vi.fn();

describe("runSourceUrlHealthProducer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it("preserves hard-failure streak and last status/error on transient checks in producer flow", async () => {
    const priorFirstHardFailedAt = new Date("2026-05-01T00:00:00.000Z");
    const priorLastHardFailedAt = new Date("2026-05-10T00:00:00.000Z");
    const priorLastError = "citation fetch returned status 404";

    let upsertArgs: unknown[] | null = null;

    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql.includes("SELECT COUNT(*)::text AS count")) {
        return { rows: [{ count: "1" }], rowCount: 1 };
      }
      if (sql.includes("WITH distinct_urls AS")) {
        return {
          rows: [
            {
              url: "https://example.com/source",
              last_checked_at: null,
              last_http_status: 404,
              last_error: priorLastError,
              consecutive_hard_failures: 2,
              first_hard_failed_at: priorFirstHardFailedAt,
              last_hard_failed_at: priorLastHardFailedAt,
            },
          ],
          rowCount: 1,
        };
      }
      if (sql.includes("INSERT INTO public.source_url_health")) {
        upsertArgs = params ?? null;
        return { rows: [], rowCount: 1 };
      }
      if (sql.includes("SELECT h.url\n      FROM public.source_url_health AS h")) {
        return { rows: [], rowCount: 0 };
      }
      throw new Error(`unexpected query in test: ${sql.slice(0, 120)}`);
    });

    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: false,
      reason: "citation URL fetch timed out",
    });

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: poolQueryMock,
        end: poolEndMock,
      })),
    }));

    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90_000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    vi.doMock("../../src/ai/urlReachability.js", () => ({
      verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
    }));

    vi.doMock("../../src/pipeline/elections/sourceUrlHealthPolicy.js", () => ({
      readSourceUrlHealthPolicyFromEnv: () => ({
        enabled: true,
        cleanupEnabled: false,
        asOfTimestamp: "2026-05-25T00:00:00.000Z",
        staleAfterDays: 30,
        maxUrlsPerRun: 100,
        maxCleanupUrlsPerRun: 100,
        hardFailureThreshold: 3,
        hardFailureWindowDays: 14,
        timeoutMs: 1000,
        concurrency: 1,
      }),
    }));

    const { runSourceUrlHealthProducer } = await import(
      "../../src/pipeline/elections/sourceUrlHealthProducer.js"
    );

    const result = await runSourceUrlHealthProducer();

    expect(result.checked_count).toBe(1);
    expect(result.transient_fail_count).toBe(1);

    expect(upsertArgs).not.toBeNull();
    const queryParams = upsertArgs as unknown[];
    const lastHttpStatus = queryParams[2] as Array<number | null>;
    const lastError = queryParams[3] as Array<string | null>;
    const consecutiveHardFailures = queryParams[4] as number[];
    const firstHardFailedAt = queryParams[5] as Array<string | null>;
    const lastHardFailedAt = queryParams[6] as Array<string | null>;

    expect(lastHttpStatus).toEqual([404]);
    expect(lastError).toEqual([priorLastError]);
    expect(consecutiveHardFailures).toEqual([2]);
    expect(firstHardFailedAt).toEqual([priorFirstHardFailedAt.toISOString()]);
    expect(lastHardFailedAt).toEqual([priorLastHardFailedAt.toISOString()]);
  });
});

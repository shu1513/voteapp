import { createClient } from "redis";
import { loadProjectEnv } from "../config/env.js";

type LegacyStreamPlan = {
  legacyKey: string;
  keyType: string;
  length: number | null;
  groups: number | null;
  consumers: number | null;
  firstEntryId: string | null;
  lastEntryId: string | null;
  archiveKey: string | null;
  action: "skip_missing" | "skip_non_stream" | "would_archive" | "archived";
  error?: string;
};

const LEGACY_SHARED_STREAM_KEYS = [
  "staging:draft",
  "staging:pending",
  "staging:validated",
  "staging:rejected",
  "staging:written",
] as const;

function compactUtcTimestamp(date = new Date()): string {
  // YYYYMMDDTHHMMSSZ
  return date.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

function buildArchiveKey(legacyKey: string, batchId: string): string {
  const safeKey = legacyKey.replace(/:/g, "_");
  return `archive:legacy_stream:${batchId}:${safeKey}`;
}

function parseFlag(name: string): boolean {
  return process.argv.includes(name);
}

async function describeLegacyStream(
  redis: ReturnType<typeof createClient>,
  legacyKey: string,
  batchId: string
): Promise<LegacyStreamPlan> {
  const keyType = await redis.type(legacyKey);
  if (keyType === "none") {
    return {
      legacyKey,
      keyType,
      length: null,
      groups: null,
      consumers: null,
      firstEntryId: null,
      lastEntryId: null,
      archiveKey: null,
      action: "skip_missing",
    };
  }

  if (keyType !== "stream") {
    return {
      legacyKey,
      keyType,
      length: null,
      groups: null,
      consumers: null,
      firstEntryId: null,
      lastEntryId: null,
      archiveKey: null,
      action: "skip_non_stream",
    };
  }

  const info = await redis.xInfoStream(legacyKey);
  const groups = await redis.xInfoGroups(legacyKey);
  const consumers = groups.reduce((sum, group) => sum + group.consumers, 0);

  return {
    legacyKey,
    keyType,
    length: info.length,
    groups: groups.length,
    consumers,
    firstEntryId: info.firstEntry?.id ?? null,
    lastEntryId: info.lastEntry?.id ?? null,
    archiveKey: buildArchiveKey(legacyKey, batchId),
    action: "would_archive",
  };
}

async function archiveLegacyStreams(apply: boolean): Promise<void> {
  loadProjectEnv();
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl || redisUrl.trim().length === 0) {
    throw new Error("REDIS_URL is required");
  }

  const redis = createClient({ url: redisUrl });
  const batchId = compactUtcTimestamp();
  await redis.connect();

  try {
    const plans: LegacyStreamPlan[] = [];

    for (const legacyKey of LEGACY_SHARED_STREAM_KEYS) {
      try {
        const plan = await describeLegacyStream(redis, legacyKey, batchId);
        plans.push(plan);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        plans.push({
          legacyKey,
          keyType: "unknown",
          length: null,
          groups: null,
          consumers: null,
          firstEntryId: null,
          lastEntryId: null,
          archiveKey: null,
          action: "skip_non_stream",
          error: message,
        });
      }
    }

    if (!apply) {
      console.log(
        JSON.stringify(
          {
            mode: "dry_run",
            batchId,
            redisUrl,
            plans,
            note: "Run with --apply to archive (rename) legacy shared stream keys.",
          },
          null,
          2
        )
      );
      return;
    }

    const results: LegacyStreamPlan[] = [];
    for (const plan of plans) {
      if (plan.action !== "would_archive" || !plan.archiveKey) {
        results.push(plan);
        continue;
      }

      const archiveExists = await redis.exists(plan.archiveKey);
      if (archiveExists > 0) {
        results.push({
          ...plan,
          action: "would_archive",
          error: `archive key already exists: ${plan.archiveKey}`,
        });
        continue;
      }

      // Atomic key move. Preserves full stream data + consumer-group metadata.
      await redis.rename(plan.legacyKey, plan.archiveKey);
      results.push({
        ...plan,
        action: "archived",
      });
    }

    console.log(
      JSON.stringify(
        {
          mode: "apply",
          batchId,
          redisUrl,
          results,
        },
        null,
        2
      )
    );
  } finally {
    await redis.quit();
  }
}

archiveLegacyStreams(parseFlag("--apply")).catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`archiveLegacySharedStreams failed: ${message}`);
  process.exitCode = 1;
});

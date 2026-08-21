import { verifyHttpUrlReachability } from "../../ai/urlReachability.js";
import type { CurrentRaceRatingPayload } from "../../contracts/currentRaceRatingPayloadContract.js";

// insideelections.com serves Cloudflare 403 to every non-browser client, so a
// 403 from an already-contract-validated host proves liveness, not access.
export type CurrentRaceRatingSourceVerification = {
  url: string;
  finalUrl: string;
  status: number;
  note: "ok" | "reachable_403";
};

export type CurrentRaceRatingSourceValidationResult =
  | { ok: true; verifications: CurrentRaceRatingSourceVerification[] }
  | { ok: false; reason: string; failedUrls: Array<{ url: string; reason: string }> };

export type CurrentRaceRatingSourceValidationOptions = {
  timeoutMs: number;
};

const VERIFY_CONCURRENCY = 4;

// Every URL a payload can carry: the per-row source_url plus each
// observation's outlet url inside evidence. The contract has already
// normalized these and enforced blocklist + outlet-domain rules, so this
// step checks liveness only — and never rewrites to a redirect target,
// which could silently move an observation off its outlet's domain.
export function collectCurrentRaceRatingSourceUrls(payload: CurrentRaceRatingPayload): string[] {
  const urls = new Set<string>();
  for (const row of payload.ratings) {
    urls.add(row.source_url);
    const observations = row.evidence.observations;
    if (!Array.isArray(observations)) {
      continue;
    }
    for (const observation of observations) {
      const url = (observation as { url?: unknown } | null)?.url;
      if (typeof url === "string" && url.trim().length > 0) {
        urls.add(url);
      }
    }
  }
  return [...urls];
}

export async function validateCurrentRaceRatingSourceUrls(
  payload: CurrentRaceRatingPayload,
  options: CurrentRaceRatingSourceValidationOptions
): Promise<CurrentRaceRatingSourceValidationResult> {
  const uniqueUrls = collectCurrentRaceRatingSourceUrls(payload);
  const checks: Array<{
    url: string;
    verification: Awaited<ReturnType<typeof verifyHttpUrlReachability>>;
  }> = new Array(uniqueUrls.length);

  const concurrency = Math.max(1, Math.min(VERIFY_CONCURRENCY, uniqueUrls.length));
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= uniqueUrls.length) {
          return;
        }
        const url = uniqueUrls[index];
        checks[index] = {
          url,
          verification: await verifyHttpUrlReachability(url, {
            timeoutMs: Math.min(options.timeoutMs, 10_000),
            allowStatusCodes: [403],
          }),
        };
      }
    })
  );

  const failed = checks.flatMap((check) =>
    check.verification.ok ? [] : [{ url: check.url, reason: check.verification.reason }]
  );
  if (failed.length > 0) {
    const first = failed[0];
    return {
      ok: false,
      reason: `rating source URL is not reachable: ${first.url} (${first.reason})`,
      failedUrls: failed,
    };
  }

  return {
    ok: true,
    verifications: checks.map((check) => {
      if (!check.verification.ok) {
        throw new Error(`unreachable url survived the failure filter: ${check.url}`);
      }
      return {
        url: check.url,
        finalUrl: check.verification.finalUrl,
        status: check.verification.status,
        note: check.verification.status === 403 ? "reachable_403" : "ok",
      };
    }),
  };
}

import { verifyHttpUrlReachability } from "../../ai/urlReachability.js";
import type {
  ElectionResultPayload,
  ParsedElectionResultPayloadRow,
} from "../../contracts/electionResultPayloadContract.js";

export type ElectionResultSourceAuthority = "verified" | "weak";

export type ElectionResultSourceVerification = {
  sourceUrl: string;
  finalUrl: string;
  status: number;
  authority: ElectionResultSourceAuthority;
};

export type ElectionResultSourceValidationResult =
  | {
      ok: true;
      payload: ElectionResultPayload;
      sourceVerifications: ElectionResultSourceVerification[];
    }
  | {
      ok: false;
      reason: string;
      blockedUrls: string[];
      reviewFeedbackLines: string[];
      failureDebug?: Record<string, unknown>;
    };

export type ElectionResultSourceValidationOptions = {
  timeoutMs: number;
};

const DEFAULT_VERIFY_CONCURRENCY = 4;

function cloneRowsWithFinalUrls(
  rows: readonly ParsedElectionResultPayloadRow[],
  finalUrlBySourceUrl: ReadonlyMap<string, string>
): ParsedElectionResultPayloadRow[] {
  return rows.map((row) => ({
    ...row,
    source_url: finalUrlBySourceUrl.get(row.source_url) ?? row.source_url,
  }));
}

export async function validateElectionResultSourceUrls(
  payload: ElectionResultPayload,
  options: ElectionResultSourceValidationOptions
): Promise<ElectionResultSourceValidationResult> {
  const uniqueSourceUrls = [...new Set(payload.results.map((row) => row.source_url))];
  const sourceChecks: Array<{
    url: string;
    verification: Awaited<ReturnType<typeof verifyHttpUrlReachability>>;
  }> = new Array(uniqueSourceUrls.length);

  const concurrency = Math.max(1, Math.min(DEFAULT_VERIFY_CONCURRENCY, uniqueSourceUrls.length));
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= uniqueSourceUrls.length) {
          return;
        }
        const url = uniqueSourceUrls[index];
        sourceChecks[index] = {
          url,
          verification: await verifyHttpUrlReachability(url, {
            timeoutMs: Math.min(options.timeoutMs, 10_000),
            allowStatusCodes: [403],
          }),
        };
      }
    })
  );

  const failedChecks = sourceChecks.flatMap((check) =>
    check.verification.ok
      ? []
      : [
          {
            url: check.url,
            reason: check.verification.reason,
          },
        ]
  );
  if (failedChecks.length > 0) {
    const first = failedChecks[0];
    return {
      ok: false,
      reason: `result source URL is not reachable: ${first.url} (${first.reason})`,
      blockedUrls: failedChecks.map((check) => check.url),
      reviewFeedbackLines: [
        "One or more result source URLs failed automated verification.",
        ...failedChecks.map((check) => `Do not reuse this unreachable/dead result source URL: ${check.url} (${check.reason})`),
        "Replace failed URLs with reachable official result pages when the result is unofficial/certified/not_final_yet.",
        "For projected results, AP/news URLs are acceptable only if result_status is projected.",
      ],
      failureDebug: {
        failed_result_source_urls: failedChecks,
      },
    };
  }

  const sourceVerifications: ElectionResultSourceVerification[] = [];
  const finalUrlBySourceUrl = new Map<string, string>();
  for (const check of sourceChecks) {
    if (!check.verification.ok) {
      continue;
    }
    const authority: ElectionResultSourceAuthority = check.verification.status === 403 ? "weak" : "verified";
    finalUrlBySourceUrl.set(check.url, check.verification.finalUrl);
    sourceVerifications.push({
      sourceUrl: check.url,
      finalUrl: check.verification.finalUrl,
      status: check.verification.status,
      authority,
    });
  }

  return {
    ok: true,
    payload: {
      results: cloneRowsWithFinalUrls(payload.results, finalUrlBySourceUrl),
    },
    sourceVerifications,
  };
}

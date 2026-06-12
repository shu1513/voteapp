import { verifyHttpUrlReachability } from "../../ai/urlReachability.js";
import type {
  PresidentialPrimaryDatePayload,
  PresidentialPrimaryDatePayloadRow,
} from "../../contracts/presidentialPrimaryDatePayloadContract.js";

export type PresidentialPrimaryDateSourceAuthority = "verified" | "weak";
export type PresidentialPrimaryDateSourceKind = "official_like" | "secondary";

export type PresidentialPrimaryDateSourceVerification = {
  sourceUrl: string;
  finalUrl: string;
  status: number;
  authority: PresidentialPrimaryDateSourceAuthority;
  sourceKind: PresidentialPrimaryDateSourceKind;
};

export type PresidentialPrimaryDateSourceValidationResult =
  | {
      ok: true;
      payload: PresidentialPrimaryDatePayload;
      sourceVerifications: PresidentialPrimaryDateSourceVerification[];
    }
  | {
      ok: false;
      reason: string;
      blockedUrls: string[];
      reviewFeedbackLines: string[];
      failureDebug?: Record<string, unknown>;
    };

export type PresidentialPrimaryDateSourceValidationRowFailure = {
  state_fips: string;
  reason: string;
  blockedUrls: string[];
  reviewFeedbackLines: string[];
  failureDebug?: Record<string, unknown>;
};

export type PresidentialPrimaryDatePartialSourceValidationResult = {
  payload: PresidentialPrimaryDatePayload;
  failedRows: PresidentialPrimaryDateSourceValidationRowFailure[];
  sourceVerifications: PresidentialPrimaryDateSourceVerification[];
  reviewFeedbackLines: string[];
  failureDebug?: Record<string, unknown>;
};

export type PresidentialPrimaryDateSourceValidationOptions = {
  timeoutMs: number;
};

type UrlVerificationResult = Awaited<ReturnType<typeof verifyHttpUrlReachability>>;

const DEFAULT_VERIFY_CONCURRENCY = 4;

const KNOWN_SECONDARY_HOST_PATTERNS = [
  /(^|\.)wikipedia\.org$/i,
  /(^|\.)ballotpedia\.org$/i,
  /(^|\.)apnews\.com$/i,
  /(^|\.)reuters\.com$/i,
  /(^|\.)nytimes\.com$/i,
  /(^|\.)washingtonpost\.com$/i,
  /(^|\.)cnn\.com$/i,
  /(^|\.)foxnews\.com$/i,
  /(^|\.)politico\.com$/i,
  /(^|\.)nbcnews\.com$/i,
  /(^|\.)abcnews\.go\.com$/i,
  /(^|\.)cbsnews\.com$/i,
  /(^|\.)npr\.org$/i,
];

const KNOWN_NATIONAL_PARTY_HOSTS = new Set([
  "democrats.org",
  "www.democrats.org",
  "gop.com",
  "www.gop.com",
  "rnc.org",
  "www.rnc.org",
  "lp.org",
  "www.lp.org",
  "gp.org",
  "www.gp.org",
]);

function hostnameForUrl(url: string): string | null {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}

function isKnownSecondaryHost(hostname: string): boolean {
  return KNOWN_SECONDARY_HOST_PATTERNS.some((pattern) => pattern.test(hostname));
}

function isGovernmentStateUsHost(hostname: string): boolean {
  return /(^|\.)state\.[a-z]{2}\.us$/i.test(hostname);
}

export function classifyPresidentialPrimaryDateSource(url: string): PresidentialPrimaryDateSourceKind {
  const hostname = hostnameForUrl(url);
  if (!hostname) {
    return "secondary";
  }
  if (isKnownSecondaryHost(hostname)) {
    return "secondary";
  }
  if (hostname.endsWith(".gov")) {
    return "official_like";
  }
  if (isGovernmentStateUsHost(hostname)) {
    return "official_like";
  }
  if (KNOWN_NATIONAL_PARTY_HOSTS.has(hostname)) {
    return "official_like";
  }
  return "secondary";
}

function uniqueSourceUrls(payload: PresidentialPrimaryDatePayload): string[] {
  const urls: string[] = [];
  const seen = new Set<string>();
  for (const row of payload.results) {
    for (const source of row.sources) {
      if (seen.has(source)) {
        continue;
      }
      seen.add(source);
      urls.push(source);
    }
  }
  return urls;
}

async function verifySourceUrls(
  urls: readonly string[],
  timeoutMs: number
): Promise<Array<{ url: string; verification: UrlVerificationResult }>> {
  const sourceChecks: Array<{ url: string; verification: UrlVerificationResult }> = new Array(urls.length);
  if (urls.length === 0) {
    return sourceChecks;
  }

  const concurrency = Math.max(1, Math.min(DEFAULT_VERIFY_CONCURRENCY, urls.length));
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= urls.length) {
          return;
        }
        const url = urls[index];
        sourceChecks[index] = {
          url,
          verification: await verifyHttpUrlReachability(url, {
            timeoutMs: Math.min(timeoutMs, 10_000),
            allowStatusCodes: [403],
          }),
        };
      }
    })
  );

  return sourceChecks;
}

function cloneRowsWithFinalUrls(
  rows: readonly PresidentialPrimaryDatePayloadRow[],
  finalUrlBySourceUrl: ReadonlyMap<string, string>
): PresidentialPrimaryDatePayloadRow[] {
  return rows.map((row) => ({
    ...row,
    sources: row.sources.map((source) => finalUrlBySourceUrl.get(source) ?? source),
  }));
}

function officialFoundRowsWithoutOfficialLikeSource(
  payload: PresidentialPrimaryDatePayload,
  sourceKindByFinalUrl: ReadonlyMap<string, PresidentialPrimaryDateSourceKind>,
  finalUrlBySourceUrl: ReadonlyMap<string, string>
): PresidentialPrimaryDatePayloadRow[] {
  return payload.results.filter((row) => {
    if (row.status !== "official_found") {
      return false;
    }
    return !row.sources.some((source) => {
      const finalUrl = finalUrlBySourceUrl.get(source) ?? source;
      return sourceKindByFinalUrl.get(finalUrl) === "official_like";
    });
  });
}

function rowHasOfficialLikeSource(
  row: PresidentialPrimaryDatePayloadRow,
  sourceKindByFinalUrl: ReadonlyMap<string, PresidentialPrimaryDateSourceKind>,
  finalUrlBySourceUrl: ReadonlyMap<string, string>
): boolean {
  return row.sources.some((source) => {
    const finalUrl = finalUrlBySourceUrl.get(source) ?? source;
    return sourceKindByFinalUrl.get(finalUrl) === "official_like";
  });
}

export async function validatePresidentialPrimaryDateSourceUrls(
  payload: PresidentialPrimaryDatePayload,
  options: PresidentialPrimaryDateSourceValidationOptions
): Promise<PresidentialPrimaryDateSourceValidationResult> {
  const urls = uniqueSourceUrls(payload);
  const checks = await verifySourceUrls(urls, options.timeoutMs);

  const failedChecks = checks.flatMap((check) =>
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
      reason: `presidential primary date source URL is not reachable: ${first.url} (${first.reason})`,
      blockedUrls: failedChecks.map((check) => check.url),
      reviewFeedbackLines: [
        "One or more presidential primary date source URLs failed automated verification.",
        ...failedChecks.map((check) => `Do not reuse this unreachable/dead source URL: ${check.url} (${check.reason})`),
        "Replace failed URLs with reachable official state election, secretary of state, state party, national party, or official calendar pages.",
        "If no official date is set yet, cite the official page or calendar checked and return status=\"not_official_yet\".",
      ],
      failureDebug: {
        failed_presidential_primary_date_source_urls: failedChecks,
      },
    };
  }

  const finalUrlBySourceUrl = new Map<string, string>();
  const sourceKindByFinalUrl = new Map<string, PresidentialPrimaryDateSourceKind>();
  const sourceVerifications: PresidentialPrimaryDateSourceVerification[] = [];
  for (const check of checks) {
    if (!check.verification.ok) {
      continue;
    }
    const authority: PresidentialPrimaryDateSourceAuthority =
      check.verification.status === 403 ? "weak" : "verified";
    const sourceKind = classifyPresidentialPrimaryDateSource(check.verification.finalUrl);
    finalUrlBySourceUrl.set(check.url, check.verification.finalUrl);
    sourceKindByFinalUrl.set(check.verification.finalUrl, sourceKind);
    sourceVerifications.push({
      sourceUrl: check.url,
      finalUrl: check.verification.finalUrl,
      status: check.verification.status,
      authority,
      sourceKind,
    });
  }

  const unofficialRows = officialFoundRowsWithoutOfficialLikeSource(
    payload,
    sourceKindByFinalUrl,
    finalUrlBySourceUrl
  );
  if (unofficialRows.length > 0) {
    return {
      ok: false,
      reason: `official_found presidential primary date requires an official-looking source for state_fips ${unofficialRows[0].state_fips}`,
      blockedUrls: [],
      reviewFeedbackLines: [
        "One or more official_found presidential primary date rows were backed only by secondary/non-official-looking sources.",
        ...unofficialRows.map((row) => `Find an official source for state_fips=${row.state_fips}, or return status=\"not_official_yet\" if no official date is set.`),
        "Official sources include state election office, secretary of state, official state election calendar, official state party page, national party page, or official statute/calendar page.",
        "Do not use news articles, blogs, Wikipedia, or unofficial calendars as the only source for official_found.",
      ],
      failureDebug: {
        official_found_without_official_like_source: unofficialRows.map((row) => ({
          state_fips: row.state_fips,
          sources: row.sources,
        })),
      },
    };
  }

  return {
    ok: true,
    payload: {
      results: cloneRowsWithFinalUrls(payload.results, finalUrlBySourceUrl),
    },
    sourceVerifications,
  };
}

export async function validatePresidentialPrimaryDateSourceUrlsPartial(
  payload: PresidentialPrimaryDatePayload,
  options: PresidentialPrimaryDateSourceValidationOptions
): Promise<PresidentialPrimaryDatePartialSourceValidationResult> {
  const urls = uniqueSourceUrls(payload);
  const checks = await verifySourceUrls(urls, options.timeoutMs);

  const failedCheckByUrl = new Map<string, string>();
  const finalUrlBySourceUrl = new Map<string, string>();
  const sourceKindByFinalUrl = new Map<string, PresidentialPrimaryDateSourceKind>();
  const sourceVerifications: PresidentialPrimaryDateSourceVerification[] = [];

  for (const check of checks) {
    if (!check.verification.ok) {
      failedCheckByUrl.set(check.url, check.verification.reason);
      continue;
    }

    const authority: PresidentialPrimaryDateSourceAuthority =
      check.verification.status === 403 ? "weak" : "verified";
    const sourceKind = classifyPresidentialPrimaryDateSource(check.verification.finalUrl);
    finalUrlBySourceUrl.set(check.url, check.verification.finalUrl);
    sourceKindByFinalUrl.set(check.verification.finalUrl, sourceKind);
    sourceVerifications.push({
      sourceUrl: check.url,
      finalUrl: check.verification.finalUrl,
      status: check.verification.status,
      authority,
      sourceKind,
    });
  }

  const validRows: PresidentialPrimaryDatePayloadRow[] = [];
  const failedRows: PresidentialPrimaryDateSourceValidationRowFailure[] = [];

  for (const row of payload.results) {
    const failedUrls = row.sources.filter((source) => failedCheckByUrl.has(source));
    if (failedUrls.length > 0) {
      const reason = failedCheckByUrl.get(failedUrls[0]!) ?? "source URL is not reachable";
      failedRows.push({
        state_fips: row.state_fips,
        reason: `presidential primary date source URL is not reachable: ${failedUrls[0]} (${reason})`,
        blockedUrls: failedUrls,
        reviewFeedbackLines: [
          `For state_fips=${row.state_fips}, one or more source URLs failed automated verification.`,
          ...failedUrls.map((url) => `Do not reuse this unreachable/dead source URL: ${url} (${failedCheckByUrl.get(url)})`),
          `Replace failed URLs for state_fips=${row.state_fips} with reachable official state election, secretary of state, state party, national party, or official calendar pages.`,
        ],
        failureDebug: {
          failed_presidential_primary_date_source_urls: failedUrls.map((url) => ({
            state_fips: row.state_fips,
            url,
            reason: failedCheckByUrl.get(url),
          })),
        },
      });
      continue;
    }

    if (
      row.status === "official_found" &&
      !rowHasOfficialLikeSource(row, sourceKindByFinalUrl, finalUrlBySourceUrl)
    ) {
      failedRows.push({
        state_fips: row.state_fips,
        reason: `official_found presidential primary date requires an official-looking source for state_fips ${row.state_fips}`,
        blockedUrls: [],
        reviewFeedbackLines: [
          `Find an official source for state_fips=${row.state_fips}, or return status=\"not_official_yet\" if no official date is set.`,
          "Official sources include state election office, secretary of state, official state election calendar, official state party page, national party page, or official statute/calendar page.",
          "Do not use news articles, blogs, Wikipedia, or unofficial calendars as the only source for official_found.",
        ],
        failureDebug: {
          official_found_without_official_like_source: {
            state_fips: row.state_fips,
            sources: row.sources,
          },
        },
      });
      continue;
    }

    validRows.push({
      ...row,
      sources: row.sources.map((source) => finalUrlBySourceUrl.get(source) ?? source),
    });
  }

  const reviewFeedbackLines = failedRows.flatMap((failure) => failure.reviewFeedbackLines);
  return {
    payload: {
      results: validRows,
    },
    failedRows,
    sourceVerifications,
    reviewFeedbackLines,
    failureDebug:
      failedRows.length > 0
        ? {
            failed_presidential_primary_date_source_rows: failedRows.map((failure) => ({
              state_fips: failure.state_fips,
              reason: failure.reason,
              blocked_urls: failure.blockedUrls,
            })),
          }
        : undefined,
  };
}

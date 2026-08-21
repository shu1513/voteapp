import { verifyHttpUrlReachability } from "../../ai/urlReachability.js";
import {
  validateCurrentRaceRatingUrl,
  type CurrentRaceRatingPayload,
} from "../../contracts/currentRaceRatingPayloadContract.js";
import type { CurrentRaceRatingOutlet } from "./currentRaceRatingConsensus.js";

// insideelections.com serves Cloudflare 403 to every non-browser client, so a
// 403 from that host proves liveness. Every other host (Sabato, Wikipedia,
// generic row sources) answers normally today — a 403 there means blocked or
// nonexistent, and must fail rather than pass as verified.
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

export type CurrentRaceRatingSourceUrlEntry = {
  url: string;
  // Set when the URL is an outlet observation's, so redirect targets can be
  // held to the same outlet-domain rule as the stored URL; null for row-level
  // source_url slots.
  outlet: CurrentRaceRatingOutlet | null;
};

function allow403For(url: string): boolean {
  const hostname = new URL(url).hostname.toLowerCase();
  return hostname === "insideelections.com" || hostname.endsWith(".insideelections.com");
}

// Every URL a payload can carry: the per-row source_url plus each
// observation's outlet url inside evidence. The contract has already
// normalized these and enforced blocklist + outlet-domain rules, so this
// step checks liveness (and re-checks policy on redirect targets) — it
// never rewrites a stored URL to a redirect target, which could silently
// move an observation off its outlet's domain.
export function collectCurrentRaceRatingSourceUrls(
  payload: CurrentRaceRatingPayload
): CurrentRaceRatingSourceUrlEntry[] {
  const outletByUrl = new Map<string, CurrentRaceRatingOutlet | null>();
  for (const row of payload.ratings) {
    if (!outletByUrl.has(row.source_url)) {
      outletByUrl.set(row.source_url, null);
    }
    const observations = row.evidence.observations;
    if (!Array.isArray(observations)) {
      continue;
    }
    for (const observation of observations) {
      const entry = observation as { url?: unknown; outlet?: unknown } | null;
      const url = entry?.url;
      if (typeof url === "string" && url.trim().length > 0) {
        // An observation URL is outlet-bound; that stricter binding wins if
        // the same URL also appears in a source_url slot.
        outletByUrl.set(url, typeof entry?.outlet === "string" ? (entry.outlet as CurrentRaceRatingOutlet) : null);
      }
    }
  }
  return [...outletByUrl].map(([url, outlet]) => ({ url, outlet }));
}

export async function validateCurrentRaceRatingSourceUrls(
  payload: CurrentRaceRatingPayload,
  options: CurrentRaceRatingSourceValidationOptions
): Promise<CurrentRaceRatingSourceValidationResult> {
  const entries = collectCurrentRaceRatingSourceUrls(payload);
  const checks: Array<{
    entry: CurrentRaceRatingSourceUrlEntry;
    verification: Awaited<ReturnType<typeof verifyHttpUrlReachability>>;
  }> = new Array(entries.length);

  const concurrency = Math.max(1, Math.min(VERIFY_CONCURRENCY, entries.length));
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= entries.length) {
          return;
        }
        const entry = entries[index];
        checks[index] = {
          entry,
          verification: await verifyHttpUrlReachability(entry.url, {
            timeoutMs: Math.min(options.timeoutMs, 10_000),
            // The shared verifier defaults to allowing 403, so the empty
            // list must be passed explicitly for non-IE hosts.
            allowStatusCodes: allow403For(entry.url) ? [403] : [],
          }),
        };
      }
    })
  );

  const failed: Array<{ url: string; reason: string }> = [];
  const verifications: CurrentRaceRatingSourceVerification[] = [];
  for (const check of checks) {
    if (!check.verification.ok) {
      failed.push({ url: check.entry.url, reason: check.verification.reason });
      continue;
    }
    // Liveness was proven by whatever the redirect chain landed on, so the
    // final URL must satisfy the same policy as the stored one — a banned,
    // blocklisted, or off-outlet redirect target fails the check.
    const finalUrlPolicy = validateCurrentRaceRatingUrl(
      check.verification.finalUrl,
      check.entry.outlet ?? undefined
    );
    if (typeof finalUrlPolicy !== "string") {
      failed.push({
        url: check.entry.url,
        reason: `redirects to a disallowed target ${check.verification.finalUrl}: ${finalUrlPolicy.reason}`,
      });
      continue;
    }
    // The 403 allowance was granted off the ORIGINAL host, but the verifier
    // applies it to the final response — so an IE URL redirecting to some
    // other host's 403 would otherwise pass. The exception is only valid
    // when the host that actually answered 403 is Inside Elections.
    if (check.verification.status === 403 && !allow403For(check.verification.finalUrl)) {
      failed.push({
        url: check.entry.url,
        reason: `redirect target ${check.verification.finalUrl} returned 403, and only insideelections.com may answer 403`,
      });
      continue;
    }
    verifications.push({
      url: check.entry.url,
      finalUrl: check.verification.finalUrl,
      status: check.verification.status,
      note: check.verification.status === 403 ? "reachable_403" : "ok",
    });
  }

  if (failed.length > 0) {
    const first = failed[0];
    return {
      ok: false,
      reason: `rating source URL failed verification: ${first.url} (${first.reason})`,
      failedUrls: failed,
    };
  }

  return { ok: true, verifications };
}

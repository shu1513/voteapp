import type { Pool, PoolClient } from "pg";

import type { AddressResolvedDistrict } from "./addressDistrictLookup.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type ManualResearchTriggerSource = "address_resolve" | "me_address_update" | "manual_seed";
export type ManualResearchAgentKind = "claude" | "codex" | "human" | "other";

export const MANUAL_RESEARCH_AGENT_KINDS: readonly ManualResearchAgentKind[] = [
  "claude",
  "codex",
  "human",
  "other",
];

// A request that keeps failing parks as 'failed' instead of cycling through
// the queue forever; an operator can re-seed the district deliberately.
export const MANUAL_RESEARCH_MAX_ATTEMPTS = 3;

export type EnqueueManualDistrictResearchResult = {
  checked: number;
  enqueued: string[];
  bumped: string[];
  skipped_fresh: number;
  failed: number;
};

const EMPTY_ENQUEUE_RESULT: EnqueueManualDistrictResearchResult = {
  checked: 0,
  enqueued: [],
  bumped: [],
  skipped_fresh: 0,
  failed: 0,
};

export type ClaimedManualDistrictResearchRequest = {
  request_id: string;
  district_id: string;
  district_name: string;
  district_type: string;
  state: string;
  request_count: number;
  trigger_source: string;
  last_elections_searched_at_at_request: string | null;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

/**
 * Demand-driven manual research enqueue. When an address lookup resolves
 * districts that were never researched (districts.last_elections_searched_at
 * IS NULL) or researched longer than cooldownDays ago, upsert a queued request
 * per stale district. Repeat lookups for a district that still has an open
 * request bump request_count + last_requested_at instead of inserting a
 * duplicate (partial unique index on district_id WHERE status is open); the
 * count is the demand signal the claim ordering uses.
 *
 * Mirror of createAutoDistrictResearchTrigger's contract: never throws, and is
 * meant to be called fire-and-forget from the address API so it can never
 * affect the address response.
 */
export async function enqueueManualDistrictResearchRequestsForStaleDistricts(
  db: Queryable,
  input: {
    districts: readonly AddressResolvedDistrict[];
    triggerSource: ManualResearchTriggerSource;
    cooldownDays: number;
  }
): Promise<EnqueueManualDistrictResearchResult> {
  const { districts, triggerSource, cooldownDays } = input;
  if (districts.length === 0) {
    return { ...EMPTY_ENQUEUE_RESULT };
  }

  try {
    const districtIds = districts.map((district) => district.id);
    const staleResult = await db.query(
      `
        SELECT id, last_elections_searched_at
        FROM public.districts
        WHERE id = ANY($1::uuid[])
          AND (
            last_elections_searched_at IS NULL
            OR last_elections_searched_at < now() - make_interval(days => $2::int)
          )
      `,
      [districtIds, cooldownDays]
    );

    const staleFreshnessById = new Map<string, string | null>(
      staleResult.rows.map((row: { id: string; last_elections_searched_at: string | null }) => [
        row.id,
        row.last_elections_searched_at,
      ])
    );
    const staleDistricts = districts.filter((district) => staleFreshnessById.has(district.id));

    const result: EnqueueManualDistrictResearchResult = {
      checked: districts.length,
      enqueued: [],
      bumped: [],
      skipped_fresh: districts.length - staleDistricts.length,
      failed: 0,
    };

    for (const district of staleDistricts) {
      try {
        const upsert = await db.query(
          `
            INSERT INTO public.manual_district_research_requests
              (district_id, district_name_snapshot, district_type_snapshot, state_snapshot,
               trigger_source, status, last_elections_searched_at_at_request)
            VALUES ($1, $2, $3, $4, $5, 'queued', $6)
            ON CONFLICT (district_id) WHERE status IN ('queued', 'claimed', 'running')
            DO UPDATE SET
              request_count = public.manual_district_research_requests.request_count + 1,
              last_requested_at = now(),
              updated_at = now()
            RETURNING request_count
          `,
          [
            district.id,
            district.name,
            district.district_type,
            district.state,
            triggerSource,
            staleFreshnessById.get(district.id) ?? null,
          ]
        );

        // A brand-new row carries the default request_count of 1; any value
        // above 1 means the ON CONFLICT branch bumped an existing open request.
        const requestCount = Number(upsert.rows[0]?.request_count ?? 1);
        if (requestCount <= 1) {
          result.enqueued.push(district.id);
        } else {
          result.bumped.push(district.id);
        }
      } catch (error) {
        result.failed += 1;
        console.warn("manual district research enqueue failed:", {
          districtId: district.id,
          reason: toReason(error),
        });
      }
    }

    if (result.enqueued.length > 0 || result.bumped.length > 0) {
      console.log("manual district research enqueued districts:", {
        triggerSource,
        enqueued: result.enqueued,
        bumped: result.bumped,
        skipped_fresh: result.skipped_fresh,
        failed: result.failed,
      });
    }

    return result;
  } catch (error) {
    console.warn("manual district research enqueue trigger failed:", toReason(error));
    return { ...EMPTY_ENQUEUE_RESULT, checked: districts.length, failed: districts.length };
  }
}

/**
 * Claim the highest-priority queued request whose district is still stale.
 *
 * Freshness is re-checked at claim time so an agent never picks up a district
 * the AI pipeline or another agent already researched. Both steps use the same
 * SQL staleness predicate the enqueue uses: first every queued request whose
 * district is now fresh is retired as 'skipped' in one bulk UPDATE, then a
 * single atomic UPDATE (FOR UPDATE SKIP LOCKED, so concurrent agents never
 * grab the same row) claims the hottest remaining request. Returns null when
 * nothing claimable remains.
 */
export async function claimNextManualDistrictResearchRequest(
  db: Queryable,
  input: {
    claimedBy: string;
    agentKind: ManualResearchAgentKind;
    cooldownDays: number;
  }
): Promise<ClaimedManualDistrictResearchRequest | null> {
  const { claimedBy, agentKind, cooldownDays } = input;

  await db.query(
    `
      UPDATE public.manual_district_research_requests AS r
      SET status = 'skipped',
          finished_at = now(),
          summary = 'district already fresh at claim time',
          updated_at = now()
      FROM public.districts AS d
      WHERE d.id = r.district_id
        AND r.status = 'queued'
        AND d.last_elections_searched_at IS NOT NULL
        AND d.last_elections_searched_at >= now() - make_interval(days => $1::int)
    `,
    [cooldownDays]
  );

  const claimed = await db.query(
    `
      UPDATE public.manual_district_research_requests AS r
      SET status = 'claimed',
          claimed_at = now(),
          claimed_by = $1,
          agent_kind = $2,
          updated_at = now()
      WHERE r.id = (
        SELECT r2.id
        FROM public.manual_district_research_requests AS r2
        JOIN public.districts AS d ON d.id = r2.district_id
        WHERE r2.status = 'queued'
          AND (
            d.last_elections_searched_at IS NULL
            OR d.last_elections_searched_at < now() - make_interval(days => $3::int)
          )
        ORDER BY r2.request_count DESC, r2.requested_at ASC
        FOR UPDATE OF r2 SKIP LOCKED
        LIMIT 1
      )
      RETURNING
        r.id,
        r.district_id,
        r.district_name_snapshot,
        r.district_type_snapshot,
        r.state_snapshot,
        r.request_count,
        r.trigger_source,
        r.last_elections_searched_at_at_request
    `,
    [claimedBy, agentKind, cooldownDays]
  );

  const row = claimed.rows[0] as
    | {
        id: string;
        district_id: string;
        district_name_snapshot: string;
        district_type_snapshot: string;
        state_snapshot: string;
        request_count: number;
        trigger_source: string;
        last_elections_searched_at_at_request: string | null;
      }
    | undefined;

  if (!row) {
    return null;
  }

  return {
    request_id: row.id,
    district_id: row.district_id,
    district_name: row.district_name_snapshot,
    district_type: row.district_type_snapshot,
    state: row.state_snapshot,
    request_count: Number(row.request_count),
    trigger_source: row.trigger_source,
    last_elections_searched_at_at_request: row.last_elections_searched_at_at_request,
  };
}

export async function markManualDistrictResearchRequestRunning(
  db: Queryable,
  requestId: string
): Promise<boolean> {
  const updated = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'running',
          started_at = COALESCE(started_at, now()),
          attempt_count = attempt_count + 1,
          last_error = NULL,
          updated_at = now()
      WHERE id = $1
        AND status = 'claimed'
    `,
    [requestId]
  );
  return (updated.rowCount ?? 0) > 0;
}

export async function markManualDistrictResearchRequestSucceeded(
  db: Queryable,
  input: { requestId: string; manifestPath: string; summary?: string | null }
): Promise<boolean> {
  const updated = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'succeeded',
          finished_at = now(),
          manifest_path = $2,
          summary = $3,
          last_error = NULL,
          updated_at = now()
      WHERE id = $1
        AND status IN ('claimed', 'running')
    `,
    [input.requestId, input.manifestPath, input.summary ?? null]
  );
  return (updated.rowCount ?? 0) > 0;
}

export async function markManualDistrictResearchRequestFailed(
  db: Queryable,
  input: { requestId: string; error: string }
): Promise<boolean> {
  const updated = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'failed',
          finished_at = now(),
          last_error = $2,
          updated_at = now()
      WHERE id = $1
        AND status IN ('claimed', 'running')
    `,
    [input.requestId, input.error.slice(0, 4000)]
  );
  return (updated.rowCount ?? 0) > 0;
}

/**
 * Return a claimed/running request to the queue (deliberate operator release).
 * Reopening as 'queued' keeps the one-open-request-per-district invariant.
 */
export async function releaseManualDistrictResearchRequest(
  db: Queryable,
  input: { requestId: string; note?: string | null }
): Promise<boolean> {
  const updated = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'queued',
          claimed_at = NULL,
          claimed_by = NULL,
          agent_kind = NULL,
          started_at = NULL,
          last_error = $2,
          updated_at = now()
      WHERE id = $1
        AND status IN ('claimed', 'running')
    `,
    [input.requestId, input.note ?? null]
  );
  return (updated.rowCount ?? 0) > 0;
}

export type StaleClaimSweepResult = {
  requeued: number;
  parked_failed: number;
};

/**
 * Recover requests whose claiming agent went away: a claimed/running row whose
 * claim is older than maxClaimHours goes back to the queue — unless it has
 * already burned MANUAL_RESEARCH_MAX_ATTEMPTS runs, in which case it parks as
 * 'failed' so a broken district cannot cycle forever. Manual research runs for
 * hours and sessions die; without the sweep a crashed agent would block its
 * district until someone touched the row by hand.
 *
 * The clock is claimed_at, never updated_at: the enqueue bump refreshes
 * updated_at on open rows, so a hot district receiving user lookups would keep
 * a dead session's claim alive forever if updated_at were the clock.
 */
export async function releaseStaleManualDistrictResearchClaims(
  db: Queryable,
  input: { maxClaimHours: number }
): Promise<StaleClaimSweepResult> {
  const parked = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'failed',
          finished_at = now(),
          last_error = 'auto-parked: claim exceeded max hold time after max attempts',
          updated_at = now()
      WHERE status IN ('claimed', 'running')
        AND claimed_at < now() - make_interval(hours => $1::int)
        AND attempt_count >= $2
    `,
    [input.maxClaimHours, MANUAL_RESEARCH_MAX_ATTEMPTS]
  );

  const requeued = await db.query(
    `
      UPDATE public.manual_district_research_requests
      SET status = 'queued',
          claimed_at = NULL,
          claimed_by = NULL,
          agent_kind = NULL,
          started_at = NULL,
          last_error = 'auto-released: claim exceeded max hold time',
          updated_at = now()
      WHERE status IN ('claimed', 'running')
        AND claimed_at < now() - make_interval(hours => $1::int)
    `,
    [input.maxClaimHours]
  );

  return {
    requeued: requeued.rowCount ?? 0,
    parked_failed: parked.rowCount ?? 0,
  };
}

export type ManualDistrictResearchQueueStats = {
  status: string;
  count: number;
};

export async function getManualDistrictResearchQueueStats(
  db: Queryable
): Promise<ManualDistrictResearchQueueStats[]> {
  const result = await db.query(
    `
      SELECT status, count(*)::int AS count
      FROM public.manual_district_research_requests
      GROUP BY status
      ORDER BY status
    `
  );
  return result.rows as ManualDistrictResearchQueueStats[];
}

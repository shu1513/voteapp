// Link writes and the all-or-nothing snapshot writer (plan-san-diego-finance.md
// Phase 1). Copy-adapted from sanJoseFinance/sanJoseFinanceWriter.ts — same
// vendor portal, same CAL 2.20 export, same accounting model; only the table
// prefix (sdcity_) and error wording differ. Upsert semantics: one active
// link per (candidate, election); a manual active link is protected — a
// matching automatic link reuses it, a conflicting one errors; an active
// upsert deactivates other automatic links first.
//
// Snapshot semantics: the caller stages every component and calls
// replaceSanDiegoCityCandidateFinanceSnapshot only when all of them passed
// source-health checks — the write is one transaction, so a summary can
// never coexist with breakdowns or outside groups from a different as-of.
// Source unavailable → the caller does not call this at all and the prior
// snapshot survives untouched; source affirmatively reports no qualifying
// data → the caller passes zero totals and empty arrays and the
// delete-and-insert clears the stale detail rows.

import type { Pool, PoolClient } from "pg";
import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import { upsertFinanceLabelClassification } from "../finance/financeIndustryClassificationService.js";

// The efile.systems export writes the literal "Pending" for committees
// without an assigned FPPC id (San Diego workbooks also carry blank
// Filer_ID cells the shared parser normalizes to "Pending"). Never a
// durable committee identity. The Phase 2 resolver imports this constant.
export const SAN_DIEGO_PENDING_FILER_ID = "Pending";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & { connect: () => Promise<PoolClient> };

export type SanDiegoCityFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  /** FPPC id as text; never the literal "Pending" (resolver never links those). */
  fppcId: string;
  committeeName: string;
  linkStatus?: "active" | "needs_review" | "inactive";
  linkSource?: "manual" | "efile_export";
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

// All snapshot amounts arrive as integer cents (the aggregators' unit) and
// are converted to exact dollar strings at the database boundary.
export type SanDiegoCityFinanceSummaryInput = {
  /** Σ(F460 line 1 + line 4 Amount_A): donor money only, never line 5. */
  totalRaisedCents: number | null;
  totalSpentCents: number | null;
  /** Signed balance — an indebted committee legitimately reports negative. */
  cashOnHandCents: number | null;
  debtsOwedCents: number | null;
  loansReceivedCents: number | null;
  outsideSupportCents: number | null;
  outsideOpposeCents: number | null;
  /** One sentence naming what the direct totals do NOT cover; null = complete. */
  directCoverageNote: string | null;
  /** Same disclosure for the outside totals (e-filed S496 ∪ Schedule D only,
   * plus any per-candidate veto/quarantine); null = complete. */
  outsideCoverageNote: string | null;
  methodologyVersion: string;
  sourceUrl: string | null;
  /** ISO date the balances are reported through (latest covered Thru_Date). */
  reportedThrough: string | null;
};

export type SanDiegoCityDirectBreakdownInput = {
  categoryType: "occupation" | "employer" | "industry" | "contribution_size";
  categoryName: string;
  amountCents: number;
  contributorCount: number | null;
  sourceUrl?: string | null;
};

export type SanDiegoCityOutsideGroupInput = {
  /** Raw Filer_ID — may be the literal "Pending" (kept; name disambiguates). */
  spenderFilerId: string;
  spenderName: string;
  supportOppose: "support" | "oppose";
  amountCents: number;
  expenditureCount: number | null;
  sourceUrl?: string | null;
};

const text = (value: string, label: string): string => {
  const result = value.trim();
  if (!result) throw new Error(`${label} is required`);
  return result;
};
const optional = (value: string | null | undefined): string | null =>
  value?.trim() || null;
// Exact cents → "dollars.cc" string; string arithmetic, never binary floats.
// Flows must be nonnegative (refunds are already netted inside the
// aggregates) and throwing aborts the snapshot transaction so the prior
// snapshot survives; cash on hand is a signed balance and passes negatives
// through (Math.trunc keeps the sign, e.g. -4200 → "-42.00").
const centsToDollars = (
  cents: number | null,
  label: string,
  options?: { allowNegative?: boolean },
): string | null => {
  if (cents === null) return null;
  if (!Number.isSafeInteger(cents))
    throw new Error(`${label} must be integer cents`);
  if (cents < 0 && !options?.allowNegative)
    throw new Error(`${label} must be nonnegative`);
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}${Math.trunc(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
};

export async function upsertSanDiegoCityFinanceLink(input: {
  db: Queryable;
  link: SanDiegoCityFinanceLinkInput;
}): Promise<{ linkId: string }> {
  const link = input.link;
  const linkStatus = link.linkStatus ?? "active";
  const linkSource = link.linkSource ?? "manual";
  // Every text field is validated and normalized BEFORE any mutation below:
  // this function also runs standalone on a bare Pool (auto-link), where a
  // mid-flight validation throw after the deactivate UPDATE would leave the
  // candidate with no active link. The trimmed fppcId is the single value
  // used for the manual-link comparison, the deactivation predicate, and
  // the INSERT — a padded input must match a stored id, not false-conflict.
  const candidateId = text(link.candidateId, "candidate id");
  const electionId = text(link.electionId, "election id");
  const candidateNameNormalized = text(
    link.candidateNameNormalized,
    "candidate name",
  );
  const fppcId = text(link.fppcId, "FPPC id");
  const committeeName = text(link.committeeName, "committee name");
  // Case-insensitive: live data says "Pending", but this is the last line
  // before a placeholder would become a durable committee identity, so an
  // upstream re-casing must fail loudly here (matching the DB constraint).
  if (fppcId.toLowerCase() === SAN_DIEGO_PENDING_FILER_ID.toLowerCase())
    throw new Error(
      "San Diego finance links require an assigned FPPC id, not Pending",
    );
  // Manual protection applies to EVERY automatic write, not only active ones:
  // a needs_review upsert with the manual link's fppc_id would otherwise hit
  // ON CONFLICT and rewrite the operator's row to efile_export/needs_review.
  if (linkSource === "efile_export") {
    const manual = await input.db.query<{ id: string; fppc_id: string }>(
      `SELECT id::text,fppc_id FROM public.sdcity_candidate_finance_links WHERE candidate_id=$1::uuid AND election_id=$2::uuid AND link_status='active' AND link_source='manual' LIMIT 1`,
      [candidateId, electionId],
    );
    if (manual.rows.length) {
      if (manual.rows[0]!.fppc_id === fppcId) {
        // An exact committee match IS an export verification of the manual
        // link, so advance last_verified_at (and nothing else — the row
        // stays the operator's). Without this a stale-election selector
        // driving off active links' last_verified_at would treat the
        // election as stale on every run forever.
        if (link.lastVerifiedAt)
          await input.db.query(
            `UPDATE public.sdcity_candidate_finance_links SET last_verified_at=$2::timestamptz WHERE id=$1::uuid`,
            [manual.rows[0]!.id, link.lastVerifiedAt.toISOString()],
          );
        return { linkId: manual.rows[0]!.id };
      }
      throw new Error(
        "San Diego automatic finance link conflicts with protected manual link",
      );
    }
  }
  if (linkStatus === "active")
    await input.db.query(
      `UPDATE public.sdcity_candidate_finance_links SET link_status='inactive' WHERE candidate_id=$1::uuid AND election_id=$2::uuid AND fppc_id<>$3 AND link_status='active' AND link_source<>'manual'`,
      [candidateId, electionId, fppcId],
    );
  const result = await input.db.query<{ id: string }>(
    `INSERT INTO public.sdcity_candidate_finance_links (candidate_id,election_id,election_year,candidate_name_normalized,fppc_id,committee_name,link_status,link_source,source_url,last_verified_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz) ON CONFLICT (candidate_id,election_id,fppc_id) DO UPDATE SET election_year=EXCLUDED.election_year,candidate_name_normalized=EXCLUDED.candidate_name_normalized,committee_name=EXCLUDED.committee_name,link_status=EXCLUDED.link_status,link_source=EXCLUDED.link_source,source_url=EXCLUDED.source_url,last_verified_at=EXCLUDED.last_verified_at RETURNING id::text`,
    [
      candidateId,
      electionId,
      link.electionYear,
      candidateNameNormalized,
      fppcId,
      committeeName,
      linkStatus,
      linkSource,
      optional(link.sourceUrl),
      link.lastVerifiedAt?.toISOString() ?? null,
    ],
  );
  if (!result.rows[0]?.id)
    throw new Error("San Diego finance link upsert returned no id");
  return { linkId: result.rows[0].id };
}

/**
 * Writes one candidate's complete finance snapshot — link, summary, direct
 * breakdowns, outside group amounts, and any industry-label classifications —
 * in a single transaction. See the header comment for the staging contract;
 * any validation failure (negative flow, blank name) rolls the whole
 * snapshot back.
 */
export async function replaceSanDiegoCityCandidateFinanceSnapshot(input: {
  db: PoolLike;
  link: SanDiegoCityFinanceLinkInput;
  summary: SanDiegoCityFinanceSummaryInput;
  directBreakdowns: readonly SanDiegoCityDirectBreakdownInput[];
  outsideGroups: readonly SanDiegoCityOutsideGroupInput[];
  classifications?: readonly FinanceLabelClassification[];
  syncedAt?: Date;
}): Promise<{ linkId: string }> {
  if (typeof input.db.connect !== "function")
    throw new Error("San Diego finance snapshot writes require a Pool");
  const syncedAt = (input.syncedAt ?? new Date()).toISOString();
  const client = await input.db.connect();
  try {
    await client.query("BEGIN");
    const { linkId } = await upsertSanDiegoCityFinanceLink({
      db: client,
      link: input.link,
    });
    const year = input.link.electionYear;
    await client.query(
      `INSERT INTO public.sdcity_candidate_finance_summaries (link_id,election_year,total_raised,total_spent,cash_on_hand,debts_owed,loans_received,outside_support_total,outside_oppose_total,direct_coverage_note,outside_coverage_note,methodology_version,source_url,reported_through,last_synced_at) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::date,$15::timestamptz) ON CONFLICT (link_id,election_year) DO UPDATE SET total_raised=EXCLUDED.total_raised,total_spent=EXCLUDED.total_spent,cash_on_hand=EXCLUDED.cash_on_hand,debts_owed=EXCLUDED.debts_owed,loans_received=EXCLUDED.loans_received,outside_support_total=EXCLUDED.outside_support_total,outside_oppose_total=EXCLUDED.outside_oppose_total,direct_coverage_note=EXCLUDED.direct_coverage_note,outside_coverage_note=EXCLUDED.outside_coverage_note,methodology_version=EXCLUDED.methodology_version,source_url=EXCLUDED.source_url,reported_through=EXCLUDED.reported_through,last_synced_at=EXCLUDED.last_synced_at`,
      [
        linkId,
        year,
        centsToDollars(input.summary.totalRaisedCents, "total raised"),
        centsToDollars(input.summary.totalSpentCents, "total spent"),
        centsToDollars(input.summary.cashOnHandCents, "cash on hand", {
          allowNegative: true,
        }),
        centsToDollars(input.summary.debtsOwedCents, "debts owed"),
        centsToDollars(input.summary.loansReceivedCents, "loans received"),
        centsToDollars(input.summary.outsideSupportCents, "outside support"),
        centsToDollars(input.summary.outsideOpposeCents, "outside oppose"),
        optional(input.summary.directCoverageNote),
        optional(input.summary.outsideCoverageNote),
        text(input.summary.methodologyVersion, "methodology version"),
        optional(input.summary.sourceUrl),
        input.summary.reportedThrough,
        syncedAt,
      ],
    );
    await client.query(
      `DELETE FROM public.sdcity_candidate_finance_direct_breakdowns WHERE link_id=$1::uuid AND election_year=$2`,
      [linkId, year],
    );
    for (const row of input.directBreakdowns)
      await client.query(
        `INSERT INTO public.sdcity_candidate_finance_direct_breakdowns (link_id,election_year,category_type,category_name,amount,contributor_count,source_url,last_synced_at) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8::timestamptz)`,
        [
          linkId,
          year,
          row.categoryType,
          text(row.categoryName, "category name"),
          centsToDollars(row.amountCents, "breakdown amount"),
          row.contributorCount,
          optional(row.sourceUrl),
          syncedAt,
        ],
      );
    await client.query(
      `DELETE FROM public.sdcity_candidate_finance_outside_groups WHERE link_id=$1::uuid AND election_year=$2`,
      [linkId, year],
    );
    // Plain inserts: the aggregator already groups per (spender identity,
    // direction) — a unique-key collision here is a caller bug and should
    // abort the snapshot, not be papered over.
    for (const group of input.outsideGroups)
      await client.query(
        `INSERT INTO public.sdcity_candidate_finance_outside_groups (link_id,election_year,spender_filer_id,spender_name,support_oppose,amount,expenditure_count,source_url,last_synced_at) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9::timestamptz)`,
        [
          linkId,
          year,
          text(group.spenderFilerId, "spender filer id"),
          text(group.spenderName, "spender name"),
          group.supportOppose,
          centsToDollars(group.amountCents, "outside amount"),
          group.expenditureCount,
          optional(group.sourceUrl),
          syncedAt,
        ],
      );
    for (const classification of input.classifications ?? [])
      await upsertFinanceLabelClassification({ db: client, classification });
    await client.query("COMMIT");
    return { linkId };
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw error;
  } finally {
    client.release();
  }
}

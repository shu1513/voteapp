// Phase 3 writer: candidate-committee links and outside-spending relation
// rows only. The Phase 5 snapshot writer (summaries, breakdowns, outside
// group amounts) extends this file later. Upsert semantics mirror the Los
// Angeles writer: one active link per (candidate, election); a manual active
// link is protected — a matching automatic link reuses it, a conflicting one
// errors; an active upsert deactivates other automatic links first.

import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

export type SanFranciscoFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  contestCode: string;
  fppcId: string;
  filerNid: string;
  committeeName: string;
  linkStatus?: "active" | "needs_review" | "inactive";
  linkSource?: "manual" | "sfec_dashboard";
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type SanFranciscoOutsideCommitteeLinkInput = {
  /** Real FPPC id, or the resolver's synthetic "name:…" identity. */
  spenderFppcId: string;
  spenderName: string;
  supportOppose: "support" | "oppose";
  sourceUrl?: string | null;
};

const text = (value: string, label: string): string => {
  const result = value.trim();
  if (!result) throw new Error(`${label} is required`);
  return result;
};
const optional = (value: string | null | undefined): string | null =>
  value?.trim() || null;

export async function upsertSanFranciscoFinanceLink(input: {
  db: Queryable;
  link: SanFranciscoFinanceLinkInput;
}): Promise<{ linkId: string }> {
  const link = input.link;
  const linkStatus = link.linkStatus ?? "active";
  const linkSource = link.linkSource ?? "manual";
  // Manual protection applies to EVERY automatic write, not only active ones:
  // a needs_review upsert with the manual link's fppc_id would otherwise hit
  // ON CONFLICT and rewrite the operator's row to sfec_dashboard/needs_review.
  if (linkSource === "sfec_dashboard") {
    const manual = await input.db.query<{ id: string; fppc_id: string }>(
      `SELECT id::text,fppc_id FROM public.sfc_candidate_finance_links WHERE candidate_id=$1::uuid AND election_id=$2::uuid AND link_status='active' AND link_source='manual' LIMIT 1`,
      [link.candidateId, link.electionId],
    );
    if (manual.rows.length) {
      if (manual.rows[0]!.fppc_id === link.fppcId) return { linkId: manual.rows[0]!.id };
      throw new Error(
        "San Francisco automatic finance link conflicts with protected manual link",
      );
    }
  }
  if (linkStatus === "active")
    await input.db.query(
      `UPDATE public.sfc_candidate_finance_links SET link_status='inactive' WHERE candidate_id=$1::uuid AND election_id=$2::uuid AND fppc_id<>$3 AND link_status='active' AND link_source<>'manual'`,
      [link.candidateId, link.electionId, link.fppcId],
    );
  const result = await input.db.query<{ id: string }>(
    `INSERT INTO public.sfc_candidate_finance_links (candidate_id,election_id,election_year,candidate_name_normalized,contest_code,fppc_id,filer_nid,committee_name,link_status,link_source,source_url,last_verified_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::timestamptz) ON CONFLICT (candidate_id,election_id,fppc_id) DO UPDATE SET election_year=EXCLUDED.election_year,candidate_name_normalized=EXCLUDED.candidate_name_normalized,contest_code=EXCLUDED.contest_code,filer_nid=EXCLUDED.filer_nid,committee_name=EXCLUDED.committee_name,link_status=EXCLUDED.link_status,link_source=EXCLUDED.link_source,source_url=EXCLUDED.source_url,last_verified_at=EXCLUDED.last_verified_at RETURNING id::text`,
    [
      text(link.candidateId, "candidate id"),
      text(link.electionId, "election id"),
      link.electionYear,
      text(link.candidateNameNormalized, "candidate name"),
      text(link.contestCode, "contest code"),
      text(link.fppcId, "FPPC id"),
      text(link.filerNid, "filer nid"),
      text(link.committeeName, "committee name"),
      linkStatus,
      linkSource,
      optional(link.sourceUrl),
      link.lastVerifiedAt?.toISOString() ?? null,
    ],
  );
  if (!result.rows[0]?.id)
    throw new Error("San Francisco finance link upsert returned no id");
  return { linkId: result.rows[0].id };
}

/**
 * Flags active automatic links of one election whose committee is no longer
 * in the manifest contest: needs_review, never deletion — the money history
 * is real, the disappearance needs a human eye (repo rollover, committee
 * re-registration, upstream edit). Manual links are never touched. Returns
 * the flagged link ids.
 */
export async function flagSanFranciscoFinanceLinksMissingFromManifest(input: {
  db: Queryable;
  electionId: string;
  presentFppcIds: readonly string[];
}): Promise<string[]> {
  const result = await input.db.query<{ id: string }>(
    `UPDATE public.sfc_candidate_finance_links SET link_status='needs_review' WHERE election_id=$1::uuid AND link_status='active' AND link_source='sfec_dashboard' AND NOT (fppc_id=ANY($2::text[])) RETURNING id::text`,
    [input.electionId, [...input.presentFppcIds]],
  );
  return result.rows.map((row) => row.id);
}

/**
 * Replaces one candidate's outside-spending relation rows for one election
 * with the manifest's current set (delete-and-insert; relations are identity
 * rows fully owned by the manifest snapshot). Callers wrap the batch in a
 * transaction when atomicity across candidates matters.
 */
export async function replaceSanFranciscoOutsideCommitteeLinks(input: {
  db: Queryable;
  candidateId: string;
  electionId: string;
  electionYear: number;
  relations: readonly SanFranciscoOutsideCommitteeLinkInput[];
  lastVerifiedAt: Date;
}): Promise<void> {
  await input.db.query(
    `DELETE FROM public.sfc_candidate_finance_outside_committee_links WHERE candidate_id=$1::uuid AND election_id=$2::uuid`,
    [input.candidateId, input.electionId],
  );
  for (const relation of input.relations)
    await input.db.query(
      // DO NOTHING: two id-less manifest entries can normalize to one
      // synthetic spender identity; the relation is identical, keep the first.
      `INSERT INTO public.sfc_candidate_finance_outside_committee_links (candidate_id,election_id,election_year,spender_fppc_id,spender_name,support_oppose,relation_source,source_url,last_verified_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,'sfec_dashboard',$7,$8::timestamptz) ON CONFLICT (candidate_id,election_id,spender_fppc_id,support_oppose) DO NOTHING`,
      [
        input.candidateId,
        input.electionId,
        input.electionYear,
        text(relation.spenderFppcId, "spender FPPC id"),
        text(relation.spenderName, "spender name"),
        relation.supportOppose,
        optional(relation.sourceUrl),
        input.lastVerifiedAt.toISOString(),
      ],
    );
}

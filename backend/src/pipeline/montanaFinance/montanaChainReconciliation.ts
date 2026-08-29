// Montana cash-begin chain reconciliation (docs/plans/montana-finance.md).
//
// CERS exposes NO official cover totals publicly — the control is the
// cash-begin chain: for consecutive canonical C5 reports,
// begin(N) + cash_receipts(N) − cash_expenditures(N) must equal begin(N+1),
// checked per side (primCashBeg / genCashBeg separately). Detail sums
// define receipts and expenditures.
//
// Term definitions (load-bearing — wider than directContributionTotal):
// cash receipts = ALL itemized cash inflows — contributions (individual +
// committee + candidate-personal) + cash loan proceeds + the misc-receipts
// family (refunds / fundraisers / interest); cash expenditures = the cash
// portions of the disbursement lists. In-kind amounts and debt tracking
// never touch the bank and feed neither side.
//
// The chain residual IS the derived unitemized lump (the public
// `...LessThan35` fields are dead-zero): hidden small-donor receipts make
// the actual next-begin HIGHER than the itemized-derived ending, so the
// lump is POSITIVE. Gate: small positive (absolute floor OR share of
// itemized receipts) passes; a small NEGATIVE residual also passes — the
// Phase 3 live run surfaced real unitemized cash outflows (bank service
// fees / petty-cash class: observed −$9.13, −$15.00 on filers with zero
// itemized flows in the period) — but only down to a tight floor, and a
// negative lump never counts toward the unitemized total (it is money
// leaving, not raised). Anything beyond either bound → fail closed. The
// latest period's lump is unknowable until the next report files; the
// derived ending balance understates by exactly that lump (accepted).
//
// NULL begin anchors (Phase 3 live run: 40 of 151 C5 rows across the first
// 24 harvested candidates — a routine CERS list shape, not bad data): a
// null is MISSING information, not zero. Each null effective-begin is
// carried forward from the previous report's derived ending (first report:
// campaign accounts open empty, so 0). A link INTO a carried anchor closes
// by construction (marked `carriedAnchor` — verified live: mirrored ±X
// failure pairs under a null≡0 reading collapse to exact closure), and the
// whole span's residual lands on the first link into a real anchor, where
// the normal lump gate judges it — conservation is still checked between
// every pair of true anchors, and a wrongly-zeroed first anchor surfaces
// as an excessive positive lump at the next real anchor.
//
// Verified live 2026-08-27: Bedey report 76535 closes to the cent
// (17,840.09 + 17,030.33 − 15,642.08 = 19,228.34; residual 0.00).
//
// Side rollover (verified live 2026-08-28, Eddy / Supreme Court): CERS can
// reclassify the whole balance from the primary side to the general side
// between reports (primCashBeg -> 0, genCashBeg inherits everything). The
// per-side equations cannot close across that boundary, so such links fall
// back to the COMBINED conservation equation under the same lump gate —
// see MontanaChainLinkResult.side.

import type { MontanaCersReportDetailArtifact } from "./montanaCersParsers.js";
import { MONTANA_CERS_CANDIDATE_DETAIL_LISTS } from "./montanaCersParsers.js";
import type { MontanaCersReportInventoryRow } from "./montanaCersParsers.js";

/** A lump at or under this is accepted regardless of receipts size. */
export const MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS = 50_000;
/** Above the floor, a lump must stay under this share of itemized cash receipts. */
export const MONTANA_CHAIN_LUMP_MAX_RATIO = 0.25;
/**
 * Tolerated unitemized cash OUTFLOW per link (bank fees / petty cash — the
 * Phase 3 live class sits under $25). A more negative residual fails closed.
 */
export const MONTANA_CHAIN_LUMP_NEGATIVE_FLOOR_CENTS = 10_000;

export type MontanaElectionSide = "primary" | "general";

export type MontanaReportCashFlows = {
  inflowCashCents: Record<MontanaElectionSide, number>;
  outflowCashCents: Record<MontanaElectionSide, number>;
};

/**
 * Sums a report-detail artifact into per-side cash inflows/outflows using
 * the plan's list classification. Only `cashAmt` is summed — in-kind and
 * debt amounts are structurally excluded.
 */
export function computeMontanaReportCashFlows(artifact: MontanaCersReportDetailArtifact): MontanaReportCashFlows {
  const flows: MontanaReportCashFlows = {
    inflowCashCents: { primary: 0, general: 0 },
    outflowCashCents: { primary: 0, general: 0 },
  };
  for (const listName of MONTANA_CERS_CANDIDATE_DETAIL_LISTS.inflow) {
    for (const row of artifact.lists[listName]) {
      if (row.amountTypeDescr === null) {
        continue; // zero-amount placeholder rows carry no side (parser-enforced)
      }
      flows.inflowCashCents[row.amountTypeDescr === "Primary" ? "primary" : "general"] += row.cashAmtCents;
    }
  }
  for (const listName of MONTANA_CERS_CANDIDATE_DETAIL_LISTS.outflow) {
    for (const row of artifact.lists[listName]) {
      if (row.amountTypeDescr === null) {
        continue;
      }
      flows.outflowCashCents[row.amountTypeDescr === "Primary" ? "primary" : "general"] += row.cashAmtCents;
    }
  }
  return flows;
}

export type MontanaChainReport = {
  inventory: MontanaCersReportInventoryRow;
  flows: MontanaReportCashFlows;
};

export type MontanaChainLinkResult = {
  reportId: number;
  nextReportId: number;
  /**
   * "combined" marks a side-rollover link: CERS reclassified the balance
   * between sides (observed live 2026-08-28 — Eddy, Supreme Court: after the
   * primary the whole balance moves from primCashBeg to genCashBeg), so the
   * per-side equations cannot close and the conservation check runs on the
   * summed sides instead. Money is still conserved — a combined link passes
   * only when the summed equation closes within the lump gate.
   */
  side: MontanaElectionSide | "combined";
  /** Derived unitemized lump for this report period on this side. */
  lumpCents: number;
  /**
   * True when the link lands on a carried (null-in-source) begin anchor and
   * therefore closes by construction — conservation for its period is
   * checked at the next real anchor instead.
   */
  carriedAnchor: boolean;
  ok: boolean;
  failure: "negative_residual" | "excessive_residual" | null;
};

export type MontanaChainReconciliation = {
  /** True only when every checkable link on both sides passed. */
  ok: boolean;
  /**
   * True when at least one report carries REAL begin balances on both sides
   * — the chain's conservation checks then verify the JSON flows against
   * official figures. False for an all-carried (all-null) chain, whose
   * links close tautologically.
   */
  hasRealAnchor: boolean;
  links: MontanaChainLinkResult[];
  /**
   * Derived ending balance of the LAST report (begin + inflow − outflow,
   * both sides combined, carried anchors filled in) — the plan's cashOnHand
   * source. Understates by the last period's unknowable lump. Null when the
   * chain is broken (fail closed).
   */
  derivedEndingBalanceCents: number | null;
  /**
   * Sum of POSITIVE derived lumps across passing links, both sides — the
   * unitemized contribution total. Tolerated negative lumps are unitemized
   * outflows (bank fees), not negative raising, and contribute zero.
   */
  derivedUnitemizedTotalCents: number | null;
};

function beginCents(row: MontanaCersReportInventoryRow, side: MontanaElectionSide): number | null {
  return side === "primary" ? row.primCashBegCents : row.genCashBegCents;
}

function lumpGateFailure(
  lumpCents: number,
  inflowCents: number
): MontanaChainLinkResult["failure"] {
  if (lumpCents < -MONTANA_CHAIN_LUMP_NEGATIVE_FLOOR_CENTS) {
    return "negative_residual";
  }
  if (lumpCents <= MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS) {
    return null;
  }
  return lumpCents <= inflowCents * MONTANA_CHAIN_LUMP_MAX_RATIO ? null : "excessive_residual";
}

type MontanaEffectiveBegin = {
  cents: Record<MontanaElectionSide, number>;
  carried: Record<MontanaElectionSide, boolean>;
};

/**
 * Fills null begin anchors: the first report's null begin is 0 (campaign
 * accounts open empty — a wrong 0 surfaces as an excessive positive lump at
 * the next real anchor), and every later null carries the previous report's
 * derived ending forward, per side.
 */
function computeEffectiveBegins(reports: readonly MontanaChainReport[]): MontanaEffectiveBegin[] {
  const effective: MontanaEffectiveBegin[] = [];
  for (let index = 0; index < reports.length; index += 1) {
    const entry: MontanaEffectiveBegin = {
      cents: { primary: 0, general: 0 },
      carried: { primary: false, general: false },
    };
    for (const side of ["primary", "general"] as const) {
      const actual = beginCents(reports[index]!.inventory, side);
      if (actual !== null) {
        entry.cents[side] = actual;
      } else if (index === 0) {
        entry.carried[side] = true;
      } else {
        const previous = effective[index - 1]!;
        entry.cents[side] =
          previous.cents[side] +
          reports[index - 1]!.flows.inflowCashCents[side] -
          reports[index - 1]!.flows.outflowCashCents[side];
        entry.carried[side] = true;
      }
    }
    effective.push(entry);
  }
  return effective;
}

/**
 * Reconciles consecutive canonical C5 reports (caller passes the output of
 * selectMontanaCanonicalReports, in order, paired with each report's detail
 * flows). Missing begin anchors break the chain — fail closed.
 *
 * Degenerate case: a single report has zero checkable links and returns
 * ok with a zero unitemized total — nothing is chain-verified, exactly as
 * every candidate's final period is never chain-verified (its lump is
 * unknowable until the next report files). Callers publish such totals on
 * the strength of the cross-surface CSV checks, and the user-facing
 * coverage note scopes the chain claim to consecutive reports.
 */
export function reconcileMontanaCashBeginChain(reports: readonly MontanaChainReport[]): MontanaChainReconciliation {
  const links: MontanaChainLinkResult[] = [];
  const effective = computeEffectiveBegins(reports);

  for (let index = 0; index + 1 < reports.length; index += 1) {
    const current = reports[index]!;
    const next = reports[index + 1]!;
    const sideLinks: MontanaChainLinkResult[] = [];
    for (const side of ["primary", "general"] as const) {
      const begin = effective[index]!.cents[side];
      const nextBegin = effective[index + 1]!.cents[side];
      const derivedEnding = begin + current.flows.inflowCashCents[side] - current.flows.outflowCashCents[side];
      const lumpCents = nextBegin - derivedEnding;
      const failure = lumpGateFailure(lumpCents, current.flows.inflowCashCents[side]);
      sideLinks.push({
        reportId: current.inventory.reportId,
        nextReportId: next.inventory.reportId,
        side,
        lumpCents,
        carriedAnchor: effective[index + 1]!.carried[side],
        ok: failure === null,
        failure,
      });
    }
    if (sideLinks.every((link) => link.ok)) {
      links.push(...sideLinks);
      continue;
    }
    // Side-rollover fallback: reclassification between sides breaks the
    // per-side equations while conserving money. Guarded narrowly: it
    // applies ONLY at the observed rollover signature — the primary side
    // collapsing to zero after the primary election (Eddy: primCashBeg
    // 241,307.00 -> 0 with the balance re-homed under genCashBeg). An
    // arbitrary pair of offsetting per-side failures must NOT be absorbed
    // here, and combined math must not soften the per-side ratio gate
    // outside that boundary — everything else keeps its per-side failures
    // as the diagnostics and fails closed.
    const primaryCollapsed = effective[index]!.cents.primary > 0 && effective[index + 1]!.cents.primary === 0;
    if (!primaryCollapsed) {
      links.push(...sideLinks);
      continue;
    }
    const beginCombined = effective[index]!.cents.primary + effective[index]!.cents.general;
    const nextBeginCombined = effective[index + 1]!.cents.primary + effective[index + 1]!.cents.general;
    const inflowCombined = current.flows.inflowCashCents.primary + current.flows.inflowCashCents.general;
    const outflowCombined = current.flows.outflowCashCents.primary + current.flows.outflowCashCents.general;
    const lumpCombined = nextBeginCombined - (beginCombined + inflowCombined - outflowCombined);
    const combinedFailure = lumpGateFailure(lumpCombined, inflowCombined);
    if (combinedFailure === null) {
      links.push({
        reportId: current.inventory.reportId,
        nextReportId: next.inventory.reportId,
        side: "combined",
        lumpCents: lumpCombined,
        carriedAnchor: effective[index + 1]!.carried.primary || effective[index + 1]!.carried.general,
        ok: true,
        failure: null,
      });
    } else {
      links.push(...sideLinks);
    }
  }

  const ok = links.every((link) => link.ok);

  // An ending balance is only ever derivable FROM a real balance anchor: a
  // chain whose every begin is carried (a live all-null filer) reconciles
  // its links tautologically, and its "ending" would be pure derivation
  // from an assumed empty start that no source figure ever checks. The
  // itemized totals still publish on the CSV/JSON cross-checks; the
  // balance stays null ("not reported").
  const hasRealAnchor = effective.some((entry) => !entry.carried.primary && !entry.carried.general);

  let derivedEndingBalanceCents: number | null = null;
  const last = reports.at(-1);
  const lastEffective = effective.at(-1);
  if (ok && hasRealAnchor && last !== undefined && lastEffective !== undefined) {
    derivedEndingBalanceCents =
      lastEffective.cents.primary +
      lastEffective.cents.general +
      last.flows.inflowCashCents.primary +
      last.flows.inflowCashCents.general -
      last.flows.outflowCashCents.primary -
      last.flows.outflowCashCents.general;
  }

  return {
    ok,
    hasRealAnchor,
    links,
    derivedEndingBalanceCents,
    // Positive lumps only: a tolerated negative lump is unitemized spending
    // (bank fees), and must not shrink the unitemized CONTRIBUTION total.
    derivedUnitemizedTotalCents: ok ? links.reduce((sum, link) => sum + Math.max(0, link.lumpCents), 0) : null,
  };
}

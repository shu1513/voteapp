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
// lump is POSITIVE. Gate: lump ≥ 0 and small (absolute floor OR share of
// itemized receipts). Negative (itemized flows overshoot actual cash) or
// large → fail closed. The latest period's lump is unknowable until the
// next report files; the derived ending balance understates by exactly
// that lump (accepted).
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
  ok: boolean;
  failure: "negative_residual" | "excessive_residual" | null;
};

export type MontanaChainReconciliation = {
  /** True only when every checkable link on both sides passed. */
  ok: boolean;
  links: MontanaChainLinkResult[];
  /**
   * Derived ending balance of the LAST report (begin + inflow − outflow,
   * both sides combined) — the plan's cashOnHand source. Understates by the
   * last period's unknowable lump. Null when the chain is broken or a
   * begin anchor is missing (fail closed).
   */
  derivedEndingBalanceCents: number | null;
  /** Sum of derived lumps across passing links, both sides — the unitemized total. */
  derivedUnitemizedTotalCents: number | null;
};

function beginCents(row: MontanaCersReportInventoryRow, side: MontanaElectionSide): number | null {
  return side === "primary" ? row.primCashBegCents : row.genCashBegCents;
}

function lumpGateFailure(
  lumpCents: number,
  inflowCents: number
): MontanaChainLinkResult["failure"] {
  if (lumpCents < 0) {
    return "negative_residual";
  }
  if (lumpCents <= MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS) {
    return null;
  }
  return lumpCents <= inflowCents * MONTANA_CHAIN_LUMP_MAX_RATIO ? null : "excessive_residual";
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
  let anchorsMissing = false;

  for (let index = 0; index + 1 < reports.length; index += 1) {
    const current = reports[index]!;
    const next = reports[index + 1]!;
    const sideLinks: MontanaChainLinkResult[] = [];
    let sideAnchorsMissing = false;
    for (const side of ["primary", "general"] as const) {
      const begin = beginCents(current.inventory, side);
      const nextBegin = beginCents(next.inventory, side);
      if (begin === null || nextBegin === null) {
        sideAnchorsMissing = true;
        continue;
      }
      const derivedEnding = begin + current.flows.inflowCashCents[side] - current.flows.outflowCashCents[side];
      const lumpCents = nextBegin - derivedEnding;
      const failure = lumpGateFailure(lumpCents, current.flows.inflowCashCents[side]);
      sideLinks.push({
        reportId: current.inventory.reportId,
        nextReportId: next.inventory.reportId,
        side,
        lumpCents,
        ok: failure === null,
        failure,
      });
    }
    if (sideAnchorsMissing) {
      anchorsMissing = true;
      links.push(...sideLinks);
      continue;
    }
    if (sideLinks.every((link) => link.ok)) {
      links.push(...sideLinks);
      continue;
    }
    // Side-rollover fallback: reclassification between sides breaks the
    // per-side equations while conserving money. Accept the link only when
    // the COMBINED equation closes within the same lump gate; otherwise keep
    // the per-side failures as the diagnostics.
    const beginCombined =
      beginCents(current.inventory, "primary")! + beginCents(current.inventory, "general")!;
    const nextBeginCombined = beginCents(next.inventory, "primary")! + beginCents(next.inventory, "general")!;
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
        ok: true,
        failure: null,
      });
    } else {
      links.push(...sideLinks);
    }
  }

  const ok = !anchorsMissing && links.every((link) => link.ok);

  let derivedEndingBalanceCents: number | null = null;
  const last = reports.at(-1);
  if (ok && last !== undefined) {
    const primaryBegin = beginCents(last.inventory, "primary");
    const generalBegin = beginCents(last.inventory, "general");
    if (primaryBegin !== null && generalBegin !== null) {
      derivedEndingBalanceCents =
        primaryBegin +
        generalBegin +
        last.flows.inflowCashCents.primary +
        last.flows.inflowCashCents.general -
        last.flows.outflowCashCents.primary -
        last.flows.outflowCashCents.general;
    }
  }

  return {
    ok,
    links,
    derivedEndingBalanceCents,
    derivedUnitemizedTotalCents: ok ? links.reduce((sum, link) => sum + link.lumpCents, 0) : null,
  };
}

/**
 * Report the divided floor roll calls the campaign cannot import yet because
 * LegiScan still shows their bill as enrolled (status 3) rather than enacted.
 *
 * Run from backend/, against the extracted LegiScan datasets:
 *
 *     npx tsx evidence/rollcall/audits/parkedPoolAudit.ts
 *
 * LEGISCAN_DATA overrides the dataset root (default /Users/shu/legiscan-data);
 * each configured session is read from `<root>/<st>-<sessionId>`.
 *
 * The filters are the production ones, not a reconstruction: every session in
 * LEGISCAN_STATE_CONFIGS supplies its own kept/excluded questions, and
 * classifyLegiscanRollCall decides floor-ness exactly as the fetcher does. A
 * roll counts as "divided" when the losing side holds at least a quarter of
 * the winning side — the campaign's worklist rule.
 *
 * The report splits the pool by whether the answer can still change:
 *
 *   * STILL SITTING (`sine_die` 0): the governor can still sign. Check the
 *     state's own action log — a dataset re-download is not the mechanism.
 *     LegiScan's status trails the state by more than a cut interval (two
 *     Delaware bills signed four days BEFORE the cut still read status 3;
 *     legiscan-de-2163 CODE-FINDINGS §7), and the rolls are already pending in
 *     legislative_votes, so only the enactment fact is missing.
 *   * ADJOURNED (`sine_die` 1): the disposition is settled, but status 3 does
 *     NOT mean "never signed" — it can simply be stale. Alabama 2021 SB46
 *     reads status 3 in the dataset and was signed 2021-05-17. Every bill here
 *     still needs the state's record read before it is written off.
 *
 * Status 3 proves nothing on its own in either direction: a vetoed bill keeps
 * it too (Alaska 2026 HB10 and HB93 read status 3 long after their vetoes).
 * Resolutions and constitutional amendments are flagged separately because a
 * governor's signature is usually not their enactment step.
 */
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  classifyLegiscanDatasetFile,
  classifyLegiscanRollCall,
  isLegiscanCommitteeChamberRollCall,
  parseLegiscanRollCall,
} from "../../../src/pipeline/rollcall/legiscanRollCall.js";
import { LEGISCAN_STATE_CONFIGS, type LegiscanStateConfig } from "../../../src/pipeline/rollcall/legiscanStateConfigs.js";

const DATA = process.env.LEGISCAN_DATA ?? "/Users/shu/legiscan-data";
const LEGISCAN_STATUS_ENROLLED = 3;

type SessionRow = {
  session: string;
  sineDie: number | null;
  rollsByBill: Map<string, number>;
  bills: string[];
  nonBills: string[];
};

function isDivided(yea: number, nay: number): boolean {
  const low = Math.min(yea, nay);
  const high = Math.max(yea, nay);
  return high > 0 && low >= high / 4;
}

function auditSession(config: LegiscanStateConfig, dir: string): SessionRow | null {
  const bills = new Map<number, { billNumber: string; billType: string; status: unknown }>();
  const votes: Record<string, unknown>[] = [];
  let sineDie: number | null = null;
  for (const entry of readdirSync(dir, { recursive: true }) as string[]) {
    if (!entry.endsWith(".json")) continue;
    let payload;
    try {
      payload = classifyLegiscanDatasetFile(JSON.parse(readFileSync(join(dir, entry), "utf8")) as unknown);
    } catch {
      continue;
    }
    if (payload.kind === "bill") {
      // Raw fields, not parseLegiscanBill: the audit only needs the status,
      // and the strict parser rejects numbers the fetcher never imports
      // (California's `AB160A`), which would abort the whole session.
      const { bill_id: billId, bill_number: billNumber, bill_type: billType, status, session } = payload.bill;
      if (typeof billId !== "number" || typeof billNumber !== "string" || typeof billType !== "string") continue;
      bills.set(billId, { billNumber, billType, status });
      const sine = (session as Record<string, unknown> | undefined)?.sine_die;
      if (sineDie === null && typeof sine === "number") sineDie = sine;
    } else if (payload.kind === "vote") {
      votes.push(payload.rollCall);
    }
  }
  if (bills.size === 0) return null;

  const rollsByBill = new Map<string, number>();
  const nonBills = new Set<string>();
  for (const raw of votes) {
    if (isLegiscanCommitteeChamberRollCall(raw)) continue;
    let rollCall;
    try {
      rollCall = parseLegiscanRollCall(raw);
    } catch {
      continue;
    }
    const bill = bills.get(rollCall.billId);
    if (!bill || bill.status !== LEGISCAN_STATUS_ENROLLED) continue;
    if (!isDivided(rollCall.yea, rollCall.nay)) continue;
    const verdict = classifyLegiscanRollCall({
      desc: rollCall.desc,
      total: rollCall.total,
      chamber: rollCall.chamber,
      billType: bill.billType,
      config,
      rollCallId: rollCall.rollCallId,
    });
    if (verdict.isFloorVote !== true) continue;
    rollsByBill.set(bill.billNumber, (rollsByBill.get(bill.billNumber) ?? 0) + 1);
    if (bill.billType !== "B") nonBills.add(bill.billNumber);
  }
  if (rollsByBill.size === 0) return null;
  const numbers = [...rollsByBill.keys()].sort();
  return {
    session: `${config.jurisdiction}-${config.sessionId}`,
    sineDie,
    rollsByBill,
    bills: numbers.filter((n) => !nonBills.has(n)),
    nonBills: numbers.filter((n) => nonBills.has(n)),
  };
}

function report(title: string, rows: SessionRow[], stillSitting: boolean): void {
  console.log(`\n${title}`);
  if (rows.length === 0) {
    console.log("  (none)");
    return;
  }
  const total = (row: SessionRow) => [...row.rollsByBill.values()].reduce((a, b) => a + b, 0);
  for (const row of rows.sort((a, b) => total(b) - total(a) || a.session.localeCompare(b.session))) {
    console.log(`  ${row.session.padEnd(10)} ${String(total(row)).padStart(3)} parked rolls / ${String(row.rollsByBill.size).padStart(2)} bills`);
    if (row.bills.length > 0) {
      const lead = stillSitting ? "can still be signed — read the state's action log for" : "dataset says enrolled — verify against the state's record";
      console.log(`       ${lead}: ${row.bills.join(", ")}`);
    }
    if (row.nonBills.length > 0) {
      console.log(`       not a bill (signature is usually not the enactment step): ${row.nonBills.join(", ")}`);
    }
  }
  console.log(`  subtotal ${rows.reduce((sum, row) => sum + total(row), 0)} rolls`);
  console.log(
    stillSitting
      ? "  Do NOT assume a re-download will show the signature; the rolls are already\n  pending in legislative_votes, only the enactment fact is missing."
      : "  Status 3 after adjournment can be stale (Alabama 2021 SB46 was signed\n  2021-05-17 and still reads 3). Verify before treating any of these as dead.",
  );
}

function main(): void {
  const seen = new Set<string>();
  const sitting: SessionRow[] = [];
  const adjourned: SessionRow[] = [];
  for (const config of Object.values(LEGISCAN_STATE_CONFIGS)) {
    const key = `${config.jurisdiction}-${config.sessionId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const dir = join(DATA, `${config.jurisdiction.toLowerCase()}-${config.sessionId}`);
    if (!existsSync(dir)) continue;
    const row = auditSession(config, dir);
    if (row) (row.sineDie === 0 ? sitting : adjourned).push(row);
  }
  report("STILL SITTING — these can still be signed", sitting, true);
  report("ADJOURNED — disposition settled, dataset status unverified", adjourned, false);
}

main();

// Phase 0 probe for the Austin city finance module (plan-austin-finance.md).
// NO schema, NO writes, NO persisted artifacts: exercises the City of Austin
// Socrata datasets live through austinSocrataClient and checks the plan's
// hand-verified gates. Every pinned number was derived by hand from live
// responses on 2026-08-18 (Kirk Watson, 2024 Mayor = a closed cycle, so the
// fixtures are stable). A FAIL means the dataset's shape or the city's filing
// conventions changed — re-verify by hand before build.
//
// Gates:
//   1. Report Detail carries exact duplicate rows (distinct report ids < rows).
//   2. Effective-report selection: five reports carry the 2024 election tag,
//      two are corrections that supersede their originals, the ATX.7 special
//      report is dropped as re-reported; corrected raised/spent to the cent.
//   3. Itemized effective contribution rows reproduce every effective cover
//      total to the cent (superseded originals excluded).
//   4. ATX.7 rows are re-reported inside the next regular report.
//   5. Occupation coverage on effective individual rows; no entity donors;
//      no row above the per-person cap.
//   6. Outside spending: economic-payment dedupe across reports, the Georgia
//      D6 multi-target quarantine, self-DCE exclusion, and SUPPORT/OPPOSE
//      direction joined from city Committee Purpose rows.
//   7. PII: typed rows carry exactly the declared keys (no address/zip/geom).
// Informational (printed, not gated): direction coverage across the whole
// DCE dataset, 2026 filers by seat (roster cross-check), and — only when the
// TEC bulk artifact is present locally — whether TEC purpose rows exist for
// the spenders the city Committee Purpose data leaves undirected.

import { stat } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import {
  AUSTIN_CANDIDATE_CORRECTION_FORM_CODES,
  AUSTIN_CANDIDATE_REGULAR_FORM_CODES,
  AUSTIN_CANDIDATE_SPECIAL_FORM_CODES,
  getAustinCommitteePurposeRows,
  getAustinContributionRowsByRecipient,
  getAustinDirectCampaignExpenditureRows,
  getAustinReportDetailRowCounts,
  getAustinReportDetailRowsByElection,
  getAustinReportDetailRowsByFiler,
  selectAustinEffectiveReports,
  type AustinCommitteePurposeRow,
  type AustinContributionRow,
  type AustinDirectCampaignExpenditureRow,
  type AustinReportDetailRow,
} from "../pipeline/austinFinance/austinSocrataClient.js";
import { loadProjectEnv } from "../config/env.js";
import {
  DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR,
  getTexasTecCsvDatabaseArtifactCachePaths,
} from "../pipeline/texasFinance/texasTecCsvDatabaseArtifactCache.js";
import { readTexasTecPurposeRows } from "../pipeline/texasFinance/texasTecCsvDatabaseReader.js";

// --- Pinned fixture: Kirk Watson, 2024 Mayor (closed cycle). ---
const WATSON = { filerName: "Watson, Kirk P.", givenNames: "Kirk", surname: "Watson", electionDate: "2024-11-05" } as const;

const FIX = {
  cycleReportCount: 5, // effective reports tagged election 2024-11-05
  cycleCorrectionCount: 2, // R20240101100718990 + R20240701100718989 (CORCOH filed 2024-12-02)
  droppedSpecialCount: 1, // COHATX7 R20241027100718930 (2024-10-27..30) inside the Jan-15 semiannual
  raisedCents: 104_772_990, // $1,047,729.90 = 710,580.84 + 217,672.94 (corrected) + 102,414.56 + 17,061.56
  spentCents: 107_598_085, // $1,075,980.85
  itemizedRowCount: 3_053,
  reportsWithCoverTotals: 4, // the 2023-H2 report has no contrib_total and no rows
  atx7RowCount: 36,
  atx7Cents: 1_203_656, // $12,036.56
  atx7RowsFoundInNextRegular: 35, // by (normalized donor, date, amount); the remaining row differs in spelling/date
  individualRowCount: 3_053,
  entityRowCount: 0,
  blankOccupationRows: 585,
  topOccupation: { name: "RETIRED", cents: 13_479_731 },
  secondOccupation: { name: "ATTORNEY", cents: 11_553_058 },
  maxContributionCents: 45_000, // 2024 indexed per-person cap
  // Outside spending inside the cycle window (2023-07-01..2024-12-31 = span of the effective cycle reports).
  allocatedSpender: { name: "AUSTIN LEADERSHIP PAC", payments: 5, cents: 21_419_920, direction: "support" as const },
  allocatedSpenderCount: 1,
  quarantinedPayments: 6,
  quarantinedCents: 12_549_644, // RECA $71k (5 targets), Realtors x3 (with Fuentes), Firefighters PSF x2 (with Kelly)
  reca: { rows: 10, reports: 2, targets: 5, cents: 7_100_000 },
} as const;

const NOVEMBER_2026_ELECTION_DATE = "2026-11-03";

const REPORT_DETAIL_KEYS = [
  "reportId",
  "filerName",
  "formTypeCode",
  "formType",
  "reportType",
  "dateFiled",
  "periodFrom",
  "periodTo",
  "electionDate",
  "electionType",
  "officeSought",
  "officeHeld",
  "contribTotalCents",
  "expendTotalCents",
  "contribBalanceCents",
  "outstandingLoanCents",
  "reportUrl",
].sort();
const CONTRIBUTION_KEYS = [
  "transactionId",
  "reportId",
  "recipient",
  "donor",
  "donorType",
  "contributionType",
  "amountCents",
  "contributionDate",
  "occupation",
  "employer",
  "reportFiled",
  "correction",
  "reportUrl",
].sort();
const DCE_KEYS = [
  "dceId",
  "parentTransaction",
  "reportId",
  "paidBy",
  "payee",
  "paymentDate",
  "amountCents",
  "candidateOrMeasure",
  "officeSoughtInfo",
  "officeHeldInfo",
  "correction",
  "reportUrl",
].sort();
const PURPOSE_KEYS = [
  "committeePurposeId",
  "reportId",
  "filerName",
  "committeeActivity",
  "purposeType",
  "recipient",
  "officeSought",
  "officeHeld",
  "electionDate",
  "measureDescription",
  "correction",
  "reportUrl",
].sort();

type Gate = { name: string; pass: boolean; detail: string };
type Direction = "support" | "oppose";

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

/** Committee/spender identity: the same clerk registry string appears in every dataset. */
function committeeKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\bTHE\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const PERSON_SUFFIX_TOKENS = new Set(["JR", "SR", "II", "III", "IV"]);

function normalizePersonText(value: string): string[] {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .split(" ")
    .filter((token) => token && !PERSON_SUFFIX_TOKENS.has(token));
}

/**
 * Deterministic person identity for the probe: first given-name token + last
 * surname token, so `Watson, Kirk P.`, `Kirk,Watson` and the fixture's
 * (Kirk, Watson) all key to `KIRK WATSON`. Nicknames (`Chito,Vela`) and typos
 * (`Diegal, Mike`) deliberately do NOT match — Phase 2 brings the shared name
 * gates; here a miss is reported, never bridged.
 */
function personKey(givenNames: string, surname: string): string | null {
  const first = normalizePersonText(givenNames)[0];
  const lastTokens = normalizePersonText(surname);
  const last = lastTokens[lastTokens.length - 1];
  return first && last ? `${first} ${last}` : null;
}

function commaParts(value: string): string[] {
  return value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part && normalizePersonText(part).length > 0);
}

/** DCE targets, Report Detail filers and self-filing spenders are `Last, First M.`; no comma → not a person. */
function lastFirstPersonKey(value: string): string | null {
  const parts = commaParts(value);
  return parts.length >= 2 ? personKey(parts.slice(1).join(" "), parts[0]!) : null;
}

/** Committee Purpose recipients are `First,Last` (stray spaces, hyphens, typos included). */
function firstLastPersonKey(value: string): string | null {
  const parts = commaParts(value);
  return parts.length >= 2 ? personKey(parts[0]!, parts.slice(1).join(" ")) : null;
}

/** `COUNCIL_MBR_DISTRICT_04 District 4` / `MAYOR District Austin` → the leading office code. */
function officeCode(value: string | null): string | null {
  const match = /^(MAYOR|COUNCIL_MBR_DISTRICT_\d{2})\b/.exec((value ?? "").trim().toUpperCase());
  return match ? match[1]! : null;
}

function sumCents<T>(rows: readonly T[], select: (row: T) => number): number {
  return rows.reduce((total, row) => total + select(row), 0);
}

// --- Outside-spending rules (plan gotchas 3 and 4) ---------------------------

type EconomicPayment = {
  spenderKey: string;
  spenderName: string;
  amountCents: number;
  paymentDate: string;
  rows: AustinDirectCampaignExpenditureRow[];
  reportIds: Set<string>;
  /** Distinct target person keys (first+last), across every row of the payment. */
  targetKeys: Map<string, string>; // key -> as-reported name
};

/**
 * One economic payment = (spender, payee, date, amount). The same payment
 * appears once per target AND again when the same report is re-filed, so the
 * per-row view double counts in both directions.
 */
function groupEconomicPayments(rows: readonly AustinDirectCampaignExpenditureRow[]): { payments: EconomicPayment[]; rowsWithoutSpender: number } {
  const byKey = new Map<string, EconomicPayment>();
  let rowsWithoutSpender = 0;
  for (const row of rows) {
    if (row.paidBy === null) {
      rowsWithoutSpender += 1; // spender identity missing → quarantined, never guessed from the payee
      continue;
    }
    const spenderKey = committeeKey(row.paidBy);
    const key = `${spenderKey}\u0000${committeeKey(row.payee ?? "")}\u0000${row.paymentDate ?? ""}\u0000${row.amountCents}`;
    const payment =
      byKey.get(key) ??
      {
        spenderKey,
        spenderName: row.paidBy,
        amountCents: row.amountCents,
        paymentDate: row.paymentDate ?? "",
        rows: [],
        reportIds: new Set<string>(),
        targetKeys: new Map<string, string>(),
      };
    payment.rows.push(row);
    payment.reportIds.add(row.reportId);
    // Non-person targets (measures) key by their normalized text so they still count as a target.
    const targetKey = lastFirstPersonKey(row.candidateOrMeasure) ?? committeeKey(row.candidateOrMeasure);
    if (!payment.targetKeys.has(targetKey)) payment.targetKeys.set(targetKey, row.candidateOrMeasure);
    byKey.set(key, payment);
  }
  return { payments: [...byKey.values()], rowsWithoutSpender };
}

/** SUPPORT/OPPOSE per (spender, candidate) for one election; both directions → null (ambiguous). */
function committeeDirections(
  purposeRows: readonly AustinCommitteePurposeRow[],
  candidateKey: string,
  electionDate: string | null
): Map<string, Direction | null> {
  const directions = new Map<string, Direction | null>();
  for (const row of purposeRows) {
    if (row.purposeType !== "CANDIDATE" || !row.recipient || row.filerName === null) continue;
    if (electionDate !== null && row.electionDate !== electionDate) continue;
    const direction: Direction | null =
      row.committeeActivity === "SUPPORT" ? "support" : row.committeeActivity === "OPPOSE" ? "oppose" : null;
    if (!direction) continue; // ASSIST is officeholder help, not electioneering
    if (firstLastPersonKey(row.recipient) !== candidateKey) continue;
    const key = committeeKey(row.filerName);
    const existing = directions.get(key);
    directions.set(key, existing === undefined || existing === direction ? direction : null);
  }
  return directions;
}

async function main(): Promise<void> {
  if (process.argv.length > 2) {
    throw new Error(`Austin finance probe takes no flags, got: ${process.argv.slice(2).join(" ")}`);
  }
  // backend/.env may carry AUSTIN_SOCRATA_APP_TOKEN and TEXAS_TEC_CSV_DATABASE_CACHE_DIR.
  loadProjectEnv();
  const gates: Gate[] = [];

  // --- Gate 1: Report Detail duplicate rows. ---
  const counts = await getAustinReportDetailRowCounts();
  console.log(`report detail: ${counts.totalRows} rows, ${counts.distinctReportIds} distinct report ids`);
  gates.push({
    name: "Report Detail repeats rows (dedupe by report_id is load-bearing)",
    pass: counts.distinctReportIds < counts.totalRows && counts.distinctReportIds > 0,
    detail: `${counts.totalRows} rows / ${counts.distinctReportIds} distinct`,
  });

  // --- Gate 2: effective-report selection on Watson. ---
  const filerReports = await getAustinReportDetailRowsByFiler(WATSON.filerName);
  const selection = selectAustinEffectiveReports(filerReports);
  const cycleReports = selection.effective.filter((row) => row.electionDate === WATSON.electionDate);
  const cycleCorrections = cycleReports.filter((row) => AUSTIN_CANDIDATE_CORRECTION_FORM_CODES.has(row.formTypeCode));
  const cycleDroppedSpecial = selection.droppedSpecial.filter((row) => row.electionDate === WATSON.electionDate);
  const cycleKeptSpecial = selection.keptSpecial.filter((row) => row.electionDate === WATSON.electionDate);
  const raisedCents = sumCents(cycleReports, (row) => row.contribTotalCents ?? 0);
  const spentCents = sumCents(cycleReports, (row) => row.expendTotalCents ?? 0);
  console.log(
    `watson reports: ${filerReports.length} rows, ${selection.effective.length} effective, ${selection.superseded.length} superseded, ${selection.droppedSpecial.length} special dropped, ${selection.keptSpecial.length} special kept, ${selection.ignored.length} ignored`
  );
  for (const row of cycleReports) {
    console.log(
      `  ${row.reportId} ${row.formTypeCode.padEnd(6)} ${row.periodFrom}..${row.periodTo} raised=${row.contribTotalCents === null ? "n/a" : usd(row.contribTotalCents)} spent=${row.expendTotalCents === null ? "n/a" : usd(row.expendTotalCents)}`
    );
  }
  gates.push({
    name: "effective reports: corrections supersede, ATX.7 dropped, corrected raised/spent to the cent",
    pass:
      cycleReports.length === FIX.cycleReportCount &&
      cycleCorrections.length === FIX.cycleCorrectionCount &&
      cycleDroppedSpecial.length === FIX.droppedSpecialCount &&
      cycleKeptSpecial.length === 0 &&
      raisedCents === FIX.raisedCents &&
      spentCents === FIX.spentCents,
    detail: `${cycleReports.length} cycle reports (${cycleCorrections.length} corrections), raised=${usd(raisedCents)} spent=${usd(spentCents)}`,
  });

  // --- Gate 3: itemized rows reproduce every effective cover. ---
  const contributions = await getAustinContributionRowsByRecipient(WATSON.filerName);
  const cycleReportIds = new Set(cycleReports.map((row) => row.reportId));
  const itemized = contributions.filter((row) => cycleReportIds.has(row.reportId));
  const itemizedByReport = new Map<string, number>();
  for (const row of itemized) itemizedByReport.set(row.reportId, (itemizedByReport.get(row.reportId) ?? 0) + row.amountCents);
  const coverMismatches: string[] = [];
  let reportsWithCoverTotals = 0;
  for (const row of cycleReports) {
    if (row.contribTotalCents === null) continue;
    reportsWithCoverTotals += 1;
    const itemizedCents = itemizedByReport.get(row.reportId) ?? 0;
    if (itemizedCents !== row.contribTotalCents) {
      coverMismatches.push(`${row.reportId}: itemized ${usd(itemizedCents)} vs cover ${usd(row.contribTotalCents)}`);
    }
  }
  console.log(`watson contributions: ${contributions.length} rows total, ${itemized.length} on effective cycle reports`);
  gates.push({
    name: "itemized effective rows = corrected cover totals, per report and in sum",
    pass:
      itemized.length === FIX.itemizedRowCount &&
      reportsWithCoverTotals === FIX.reportsWithCoverTotals &&
      coverMismatches.length === 0 &&
      sumCents(itemized, (row) => row.amountCents) === FIX.raisedCents,
    detail:
      coverMismatches.length === 0
        ? `${itemized.length} rows = ${usd(sumCents(itemized, (row) => row.amountCents))} across ${reportsWithCoverTotals} covers`
        : coverMismatches.join("; "),
  });

  // --- Gate 4: ATX.7 rows re-reported inside the next regular report. ---
  const atx7ReportIds = new Set(cycleDroppedSpecial.map((row) => row.reportId));
  const atx7Rows = contributions.filter((row) => atx7ReportIds.has(row.reportId));
  const nextRegular = cycleReports.filter((row) =>
    cycleDroppedSpecial.some((special) => row.periodFrom! <= special.periodFrom! && special.periodTo! <= row.periodTo!)
  );
  const nextRegularIds = new Set(nextRegular.map((row) => row.reportId));
  const nextRegularKeys = new Set(
    contributions
      .filter((row) => nextRegularIds.has(row.reportId))
      .map((row) => `${committeeKey(row.donor)}|${row.contributionDate}|${row.amountCents}`)
  );
  const atx7Found = atx7Rows.filter((row) => nextRegularKeys.has(`${committeeKey(row.donor)}|${row.contributionDate}|${row.amountCents}`));
  gates.push({
    name: "ATX.7 pre-election rows reappear on the next regular report (never add both)",
    pass:
      atx7Rows.length === FIX.atx7RowCount &&
      sumCents(atx7Rows, (row) => row.amountCents) === FIX.atx7Cents &&
      atx7Found.length === FIX.atx7RowsFoundInNextRegular &&
      nextRegular.length === 1,
    detail: `${atx7Rows.length} ATX.7 rows = ${usd(sumCents(atx7Rows, (row) => row.amountCents))}, ${atx7Found.length} matched by (donor, date, amount) in ${nextRegular.map((row) => row.reportId).join(",")}`,
  });

  // --- Gate 5: occupation coverage + cap. ---
  const individualRows = itemized.filter((row) => row.donorType === "INDIVIDUAL");
  const entityRows = itemized.filter((row) => row.donorType !== "INDIVIDUAL");
  const blankOccupationRows = individualRows.filter((row) => row.occupation === null);
  const occupationCents = new Map<string, number>();
  for (const row of individualRows) {
    if (row.occupation === null) continue;
    const name = row.occupation.replace(/\s+/g, " ").toUpperCase();
    occupationCents.set(name, (occupationCents.get(name) ?? 0) + row.amountCents);
  }
  const topOccupations = [...occupationCents.entries()].sort((left, right) => right[1] - left[1]);
  const maxContributionCents = Math.max(...itemized.map((row) => row.amountCents));
  console.log(`watson occupations: top ${topOccupations.slice(0, 5).map(([name, cents]) => `${name}=${usd(cents)}`).join(", ")}`);
  gates.push({
    name: "occupations: individual-only rows, blank rate pinned, top buckets pinned, cap respected",
    pass:
      individualRows.length === FIX.individualRowCount &&
      entityRows.length === FIX.entityRowCount &&
      blankOccupationRows.length === FIX.blankOccupationRows &&
      topOccupations[0]?.[0] === FIX.topOccupation.name &&
      topOccupations[0]?.[1] === FIX.topOccupation.cents &&
      topOccupations[1]?.[0] === FIX.secondOccupation.name &&
      topOccupations[1]?.[1] === FIX.secondOccupation.cents &&
      maxContributionCents === FIX.maxContributionCents,
    detail: `${individualRows.length} individual rows, ${blankOccupationRows.length} blank occupation, max ${usd(maxContributionCents)}`,
  });

  // --- Gate 6: outside spending. ---
  const dceRows = await getAustinDirectCampaignExpenditureRows();
  const purposeRows = await getAustinCommitteePurposeRows();
  console.log(
    `dce: ${dceRows.length} rows; committee purpose: ${purposeRows.length} rows (${purposeRows.filter((row) => row.filerName === null).length} without filer name, unattributable)`
  );
  const cycleWindowFrom = cycleReports[0]?.periodFrom ?? WATSON.electionDate;
  const cycleWindowTo = cycleReports[cycleReports.length - 1]?.periodTo ?? WATSON.electionDate;
  const cycleDce = dceRows.filter((row) => row.paymentDate !== null && cycleWindowFrom <= row.paymentDate && row.paymentDate <= cycleWindowTo);
  const { payments, rowsWithoutSpender: cycleRowsWithoutSpender } = groupEconomicPayments(cycleDce);
  const watsonKey = personKey(WATSON.givenNames, WATSON.surname)!;
  const directions = committeeDirections(purposeRows, watsonKey, WATSON.electionDate);
  const allocated = new Map<string, { payments: number; cents: number; direction: Direction | null }>();
  const quarantined: EconomicPayment[] = [];
  let selfPayments = 0;
  for (const payment of payments) {
    if (!payment.targetKeys.has(watsonKey)) continue;
    if (lastFirstPersonKey(payment.spenderName) === watsonKey) {
      selfPayments += 1; // the candidate's own committee reporting a DCE on itself is direct spending
      continue;
    }
    if (payment.targetKeys.size !== 1) {
      quarantined.push(payment);
      continue;
    }
    const entry = allocated.get(payment.spenderKey) ?? { payments: 0, cents: 0, direction: directions.get(payment.spenderKey) ?? null };
    entry.payments += 1;
    entry.cents += payment.amountCents;
    allocated.set(payment.spenderKey, entry);
  }
  console.log(`watson outside (${cycleWindowFrom}..${cycleWindowTo}): ${cycleDce.length} rows → ${payments.length} payments in window (${cycleRowsWithoutSpender} row(s) without spender quarantined)`);
  for (const [spender, entry] of allocated) console.log(`  allocated ${spender}: ${entry.payments} payments ${usd(entry.cents)} ${entry.direction ?? "direction not reported"}`);
  for (const payment of quarantined) {
    console.log(`  quarantined ${payment.spenderKey} ${payment.paymentDate} ${usd(payment.amountCents)} → ${payment.targetKeys.size} targets, ${payment.rows.length} rows, ${payment.reportIds.size} reports`);
  }
  const reca = payments.find((payment) => payment.spenderKey.startsWith("REAL ESTATE COUNCIL") && payment.amountCents === FIX.reca.cents);
  const allocatedEntry = allocated.get(FIX.allocatedSpender.name);
  gates.push({
    name: "outside: dedupe across reports, D6 multi-target quarantine, self-DCE excluded, direction from Committee Purpose",
    pass:
      allocated.size === FIX.allocatedSpenderCount &&
      allocatedEntry?.payments === FIX.allocatedSpender.payments &&
      allocatedEntry?.cents === FIX.allocatedSpender.cents &&
      allocatedEntry?.direction === FIX.allocatedSpender.direction &&
      quarantined.length === FIX.quarantinedPayments &&
      sumCents(quarantined, (payment) => payment.amountCents) === FIX.quarantinedCents &&
      selfPayments === 0 &&
      reca !== undefined &&
      reca.rows.length === FIX.reca.rows &&
      reca.reportIds.size === FIX.reca.reports &&
      reca.targetKeys.size === FIX.reca.targets &&
      quarantined.includes(reca),
    detail: `allocated ${allocated.size} spender(s), quarantined ${quarantined.length} payments ${usd(sumCents(quarantined, (payment) => payment.amountCents))}, RECA ${reca ? `${reca.rows.length} rows/${reca.reportIds.size} reports/${reca.targetKeys.size} targets` : "missing"}`,
  });

  // --- Gate 7: PII allowlist on typed rows. ---
  const piiPattern = /address|zip|geom|city|treasurer|phone/i;
  const keySets: [string, string[], string[]][] = [
    ["report detail", Object.keys(filerReports[0] as AustinReportDetailRow).sort(), REPORT_DETAIL_KEYS],
    ["contribution", Object.keys(contributions[0] as AustinContributionRow).sort(), CONTRIBUTION_KEYS],
    ["dce", Object.keys(dceRows[0] as AustinDirectCampaignExpenditureRow).sort(), DCE_KEYS],
    ["purpose", Object.keys(purposeRows[0] as AustinCommitteePurposeRow).sort(), PURPOSE_KEYS],
  ];
  gates.push({
    name: "typed rows carry exactly the declared keys (no address/zip/geom)",
    pass: keySets.every(([, actual, expected]) => actual.join(",") === expected.join(",") && !actual.some((key) => piiPattern.test(key))),
    detail: keySets.map(([label, actual]) => `${label} ${actual.length}`).join(", "),
  });

  // --- Informational: direction coverage over the whole DCE dataset. ---
  const { payments: allPayments, rowsWithoutSpender } = groupEconomicPayments(dceRows);
  const anyDirection = new Map<string, Set<string>>(); // spenderKey -> target keys with SUPPORT/OPPOSE rows (any election)
  for (const row of purposeRows) {
    if (row.purposeType !== "CANDIDATE" || !row.recipient || row.filerName === null) continue;
    if (row.committeeActivity !== "SUPPORT" && row.committeeActivity !== "OPPOSE") continue;
    const key = committeeKey(row.filerName);
    const set = anyDirection.get(key) ?? new Set<string>();
    const recipientKey = firstLastPersonKey(row.recipient);
    if (recipientKey) set.add(recipientKey);
    anyDirection.set(key, set);
  }
  let coveredCents = 0;
  let uncoveredCents = 0;
  let selfCents = 0;
  const uncoveredSpenders = new Map<string, number>();
  for (const payment of allPayments) {
    const spenderPersonKey = lastFirstPersonKey(payment.spenderName);
    const isSelf = spenderPersonKey !== null && payment.targetKeys.has(spenderPersonKey);
    if (isSelf) {
      selfCents += payment.amountCents;
      continue;
    }
    const known = anyDirection.get(payment.spenderKey);
    const covered = known !== undefined && [...payment.targetKeys.keys()].every((target) => known.has(target));
    if (covered) coveredCents += payment.amountCents;
    else {
      uncoveredCents += payment.amountCents;
      uncoveredSpenders.set(payment.spenderName, (uncoveredSpenders.get(payment.spenderName) ?? 0) + payment.amountCents);
    }
  }
  console.log(`\ndce rows without a spender (quarantined): ${rowsWithoutSpender}`);
  console.log(
    `direction coverage (all DCE payments, any election, any target office incl. state races): covered ${usd(coveredCents)}, uncovered ${usd(uncoveredCents)}, self-DCE ${usd(selfCents)} → ${((coveredCents / Math.max(1, coveredCents + uncoveredCents)) * 100).toFixed(1)}% of non-self dollars`
  );
  for (const [spender, cents] of [...uncoveredSpenders.entries()].sort((left, right) => right[1] - left[1]).slice(0, 8)) {
    console.log(`  uncovered ${spender}: ${usd(cents)}`);
  }

  // --- Informational: 2026 filers by seat (roster cross-check). ---
  const candidateCodes = new Set([...AUSTIN_CANDIDATE_REGULAR_FORM_CODES, ...AUSTIN_CANDIDATE_CORRECTION_FORM_CODES, ...AUSTIN_CANDIDATE_SPECIAL_FORM_CODES]);
  const reports2026 = await getAustinReportDetailRowsByElection({ electionDate: NOVEMBER_2026_ELECTION_DATE, formTypeCodes: [...candidateCodes] });
  const filersBySeat = new Map<string, Map<string, { latestPeriodTo: string; latestFiled: string }>>();
  let reports2026WithoutFiler = 0;
  for (const row of reports2026) {
    if (row.filerName === null) {
      reports2026WithoutFiler += 1;
      continue;
    }
    const code = officeCode(row.officeSought) ?? `UNPARSED(${row.officeSought ?? "none"})`;
    const seat = filersBySeat.get(code) ?? new Map();
    const existing = seat.get(row.filerName);
    if (!existing || (row.periodTo ?? "") > existing.latestPeriodTo) {
      seat.set(row.filerName, { latestPeriodTo: row.periodTo ?? "", latestFiled: row.dateFiled });
    }
    filersBySeat.set(code, seat);
  }
  console.log(`\n2026-11-03 filers in Report Detail (${reports2026.length} candidate-form rows, ${reports2026WithoutFiler} without filer name):`);
  for (const [code, seat] of [...filersBySeat.entries()].sort()) {
    console.log(`  ${code}: ${[...seat.entries()].map(([name, info]) => `${name} (through ${info.latestPeriodTo || "?"})`).join("; ")}`);
  }

  // --- Informational: TEC purpose rows for undirected spenders (only when the artifact is cached locally). ---
  const tecCacheDir = process.env.TEXAS_TEC_CSV_DATABASE_CACHE_DIR?.trim() || DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR;
  const tecZipPath = getTexasTecCsvDatabaseArtifactCachePaths(tecCacheDir).zipPath;
  const tecAvailable = await stat(tecZipPath).then((info) => info.isFile()).catch(() => false);
  if (!tecAvailable) {
    console.log(`\nTEC fallback dry-run: SKIPPED (no artifact at ${tecZipPath}; set TEXAS_TEC_CSV_DATABASE_CACHE_DIR to run)`);
  } else {
    const spenderKeys = new Set([...uncoveredSpenders.keys()].map(committeeKey));
    const tecRows = await readTexasTecPurposeRows({ zipPath: tecZipPath, predicate: (row) => spenderKeys.has(committeeKey(row.filerName)) });
    console.log(`\nTEC fallback dry-run: ${tecRows.length} TEC purpose rows for ${spenderKeys.size} undirected spender(s)`);
    const byFiler = new Map<string, number>();
    for (const row of tecRows) byFiler.set(row.filerName, (byFiler.get(row.filerName) ?? 0) + 1);
    for (const [filer, count] of byFiler) console.log(`  ${filer}: ${count} purpose rows`);
  }

  // --- Summary. ---
  console.log("\n=== Phase 0 gates ===");
  let failures = 0;
  for (const gate of gates) {
    const status = gate.pass ? "PASS" : "FAIL";
    if (!gate.pass) failures += 1;
    console.log(`${status}  ${gate.name} — ${gate.detail}`);
  }
  if (failures > 0) {
    process.exitCode = 1;
    console.log(`\n${failures} gate(s) failed`);
  } else {
    console.log("\nall gates passed");
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Austin candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}

// Phase 0 probe for the Denver city finance module (plan-denver-finance.md).
// NO schema, NO writes, NO persisted artifacts: exercises the SearchLight
// JSON API live through denverSearchlightClient and checks the plan's
// hand-verified gates. Every pinned number was derived by hand from live
// responses on 2026-08-12 (Mike Johnston, filerId 658, cycle 26 = the closed
// 2023 Municipal General election, so the fixtures are stable). A FAIL means
// the API's composition rules changed — re-verify by hand before build.
//
// Gates:
//   1. Receipts composition: contributions total = private + FEF (overview
//      split), and the transaction feed reproduces both parts by subtype.
//   2. FEF identification is by subtype ("Fair Elections Payments"), never
//      by the fefTransaction boolean (city-payment rows carry false).
//   3. Disbursement matrix: expenditures total = subtype "Expenditure" with
//      independentExpnFlag=false; the FEF endpoints report subsets (never
//      additive); "Unpaid Obligation" rows are excluded.
//   4. Header rejection: the candidate-name expenditure search header equals
//      direct + unpaid obligations + IE rows — publishing it double-counts.
//   5. Outside spenders: support/oppose lists sum exactly to the overview's
//      IE fields.
//   6. IE spender-id resolution: every spender name resolves to exactly one
//      type-3 search entry (the stable outside-group identity).
//   7. Signed negatives: refund/overlimit rows are real and net into totals.
//   8. Identity cardinality: filerId is canonical; every transaction row's
//      entity id belongs to the filer's committeeIds set.
//   9. Pagination stability: two full sweeps are identical, no duplicate
//      transaction ids, first page unchanged after the final page.
//  10. Filed-report chains: amendment versions flatten and select cleanly;
//      per-filing summaries fetch; balance chain and composition printed for
//      reconciliation (pinned after first observation).
//  11. Registration anomalies (cycle 36): every anomaly is in the documented
//      allowlist — nothing new appeared silently.
//  12. PII allowlist: typed transaction rows carry exactly the declared keys
//      (no address/zip fields can reach logs or fixtures).

import { pathToFileURL } from "node:url";

import {
  DENVER_SEARCHLIGHT_SEARCH_TYPE_INDEPENDENT_EXPENDITURE,
  getDenverCandidatesByElectionCycle,
  getDenverContributionsTotalCents,
  getDenverElectionCyclesByFiler,
  getDenverExpendituresTotalCents,
  getDenverFefContributionsTotalCents,
  getDenverFefExpendituresTotalCents,
  getDenverFiler,
  getDenverFilingsByCommittee,
  getDenverFilingSummary,
  getDenverFinancialOverview,
  getDenverOutsideSpenders,
  searchDenverCommitteesAndCandidates,
  searchDenverContributionTransactions,
  selectLatestDenverFilings,
  sweepDenverContributionTransactions,
  sweepDenverExpenditureTransactions,
  type DenverContributionTransaction,
  type DenverExpenditureTransaction,
} from "../pipeline/denverFinance/denverSearchlightClient.js";

// --- Pinned fixture: Mike Johnston, 2023 Municipal General (closed cycle). ---
const JOHNSTON = { filerId: 658, candidateName: "Mike Johnston", electionCycleId: 26 } as const;
const JOHNSTON_COMMITTEE_ENTITY_IDS = [641, 807] as const;

const FIX = {
  contributionsTotalCents: 201_626_363, // $2,016,263.63
  privateContributionsCents: 124_933_988, // $1,249,339.88 (overview campaignContributionsToCandidate)
  fefCents: 76_692_375, // $766,923.75 (both FEF endpoints)
  monetaryNonQualifyingCents: 102_590_843, // Monetary, fefTransaction=false
  monetaryQualifyingCents: 22_107_400, // Monetary, fefTransaction=true
  inKindCents: 235_745,
  fefPaymentRowCount: 4,
  negativeContributionRows: 208,
  contributionRowCount: 7_978,
  expendituresTotalCents: 201_464_423, // $2,014,644.23
  directNonFefSpendCents: 124_772_048,
  unpaidObligationCents: 500_000,
  unpaidObligationRows: 2,
  ieRowsCents: 516_697_053, // IE rows inside the candidate-name expenditure search
  expenditureHeaderCents: 718_661_476, // direct + unpaid + IE — never publish
  ieSupportCents: 500_995_460, // $5,009,954.60
  ieOpposeCents: 15_701_593, // $157,015.93
} as const;

// --- November 2026 vacancy cycle (live registration data — allowlists, not counts). ---
const VACANCY_CYCLE_ID = 36;
/** Verified live 2026-08-12: two non-terminated "Monica Martinez" filer records. */
const DOCUMENTED_DUPLICATE_NAMES = new Set(["MONICA MARTINEZ"]);
/** Verified live 2026-08-12: listed in cycle 36 but getElectionCyclesByFiler returns []. */
const DOCUMENTED_EMPTY_CYCLE_FILER_IDS = new Set([1329, 1330]);

const CONTRIBUTION_ROW_KEYS = [
  "transactionId",
  "transactionSubType",
  "recipientName",
  "recipientCommitteeName",
  "recipientCommitteeId",
  "officeSought",
  "district",
  "contributorName",
  "contributorId",
  "amountCents",
  "date",
  "contributorEmployer",
  "contributorOccupation",
  "contributorCity",
  "contributorStateCode",
  "contactTypeId",
  "txnPurpose",
  "fefTransaction",
].sort();

const EXPENDITURE_ROW_KEYS = [
  "transactionId",
  "transactionSubType",
  "committeeName",
  "committeeId",
  "candidateName",
  "candidateOffice",
  "candidateDistrict",
  "amountCents",
  "date",
  "purpose",
  "payee",
  "contactTypeId",
  "fefTransaction",
  "electioneeringCommFlag",
  "independentExpnFlag",
].sort();

const FAIR_ELECTIONS_PAYMENTS_SUBTYPE = "Fair Elections Payments";

type Gate = { name: string; pass: boolean; detail: string };

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

function normalizeName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-zA-Z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function sumCents<T>(rows: readonly T[], select: (row: T) => number): number {
  return rows.reduce((total, row) => total + select(row), 0);
}

async function main(): Promise<void> {
  if (process.argv.length > 2) {
    throw new Error(`Denver finance probe takes no flags, got: ${process.argv.slice(2).join(" ")}`);
  }
  const gates: Gate[] = [];

  // --- Endpoint totals + overview (the split everything reconciles against). ---
  const overview = await getDenverFinancialOverview(JOHNSTON);
  const contributionsTotal = await getDenverContributionsTotalCents(JOHNSTON);
  const expendituresTotal = await getDenverExpendituresTotalCents(JOHNSTON);
  const fefContributions = await getDenverFefContributionsTotalCents(JOHNSTON);
  const fefExpenditures = await getDenverFefExpendituresTotalCents(JOHNSTON);
  console.log(
    `endpoint totals: contributions=${usd(contributionsTotal)} expenditures=${usd(expendituresTotal)} fefIn=${usd(fefContributions)} fefOut=${usd(fefExpenditures)}`
  );
  console.log(
    `overview: private=${usd(overview.campaignContributionsToCandidateCents)} fef=${usd(overview.fairElectionsFundToCandidateCents)} ieFor=${usd(overview.independentExpendituresSupportingCandidateCents)} ieAgainst=${usd(overview.independentExpendituresOpposingCandidateCents)}`
  );

  // --- Contribution sweep (also exercises pagination: 7,978 rows at 1,000/page). ---
  const sweep1 = await sweepDenverContributionTransactions({
    candidateName: JOHNSTON.candidateName,
    electionCycleIds: [JOHNSTON.electionCycleId],
  });
  const sweep2 = await sweepDenverContributionTransactions({
    candidateName: JOHNSTON.candidateName,
    electionCycleIds: [JOHNSTON.electionCycleId],
  });
  const contributionRows = sweep1.rows;
  console.log(`contribution sweep: ${contributionRows.length} rows, header ${usd(sweep1.totalContributionAmountCents)}`);

  const bySubtype = new Map<string, { cents: number; count: number }>();
  for (const row of contributionRows) {
    const key = `${row.transactionSubType}|fef=${row.fefTransaction}`;
    const entry = bySubtype.get(key) ?? { cents: 0, count: 0 };
    entry.cents += row.amountCents;
    entry.count += 1;
    bySubtype.set(key, entry);
  }
  for (const [key, entry] of [...bySubtype.entries()].sort()) {
    console.log(`  ${key}: ${usd(entry.cents)} (${entry.count})`);
  }

  // Gate 1: receipts composition.
  const feedTotal = sumCents(contributionRows, (row) => row.amountCents);
  gates.push({
    name: "receipts: total = private + FEF, feed reproduces all three",
    pass:
      contributionsTotal === FIX.contributionsTotalCents &&
      overview.campaignContributionsToCandidateCents === FIX.privateContributionsCents &&
      overview.fairElectionsFundToCandidateCents === FIX.fefCents &&
      fefContributions === FIX.fefCents &&
      contributionsTotal ===
        overview.campaignContributionsToCandidateCents + overview.fairElectionsFundToCandidateCents &&
      sweep1.totalContributionAmountCents === contributionsTotal &&
      feedTotal === contributionsTotal &&
      contributionRows.length === FIX.contributionRowCount,
    detail: `total=${usd(contributionsTotal)} feed=${usd(feedTotal)} rows=${contributionRows.length}`,
  });

  // Gate 2: FEF identification by subtype, never the boolean.
  const fefPaymentRows = contributionRows.filter(
    (row) => row.transactionSubType === FAIR_ELECTIONS_PAYMENTS_SUBTYPE
  );
  const privateRows = contributionRows.filter(
    (row) => row.transactionSubType !== FAIR_ELECTIONS_PAYMENTS_SUBTYPE
  );
  gates.push({
    name: 'FEF city money = subtype "Fair Elections Payments", rows carry fefTransaction=false',
    pass:
      fefPaymentRows.length === FIX.fefPaymentRowCount &&
      sumCents(fefPaymentRows, (row) => row.amountCents) === FIX.fefCents &&
      fefPaymentRows.every((row) => !row.fefTransaction) &&
      sumCents(privateRows, (row) => row.amountCents) === FIX.privateContributionsCents,
    detail: `${fefPaymentRows.length} FEF rows = ${usd(sumCents(fefPaymentRows, (row) => row.amountCents))}, private = ${usd(sumCents(privateRows, (row) => row.amountCents))}`,
  });

  // Gate 7: signed negatives.
  const negativeRows = contributionRows.filter((row) => row.amountCents < 0);
  gates.push({
    name: "signed refund/overlimit rows present and netted",
    pass: negativeRows.length === FIX.negativeContributionRows,
    detail: `${negativeRows.length} negative rows`,
  });

  // --- Expenditure sweep + matrix. ---
  const expenditureSweep = await sweepDenverExpenditureTransactions({
    candidateName: JOHNSTON.candidateName,
    electionCycleIds: [JOHNSTON.electionCycleId],
  });
  const expenditureRows = expenditureSweep.rows;
  const directRows = expenditureRows.filter(
    (row) => row.transactionSubType === "Expenditure" && !row.independentExpnFlag
  );
  const directCents = sumCents(directRows, (row) => row.amountCents);
  const directFefCents = sumCents(
    directRows.filter((row) => row.fefTransaction),
    (row) => row.amountCents
  );
  const unpaidRows = expenditureRows.filter((row) => row.transactionSubType === "Unpaid Obligation");
  const ieRows = expenditureRows.filter((row) => row.independentExpnFlag);
  const ieCents = sumCents(ieRows, (row) => row.amountCents);
  console.log(
    `expenditure sweep: ${expenditureRows.length} rows, header ${usd(expenditureSweep.totalExpendituresAmountCents)}, direct ${usd(directCents)}, IE ${usd(ieCents)}`
  );

  // Gate 3: disbursement matrix.
  gates.push({
    name: "disbursements: subtype Expenditure + !IE flag = endpoint total; FEF is a subset",
    pass:
      expendituresTotal === FIX.expendituresTotalCents &&
      directCents === expendituresTotal &&
      directFefCents === FIX.fefCents &&
      fefExpenditures === FIX.fefCents &&
      directCents - directFefCents === FIX.directNonFefSpendCents &&
      sumCents(unpaidRows, (row) => row.amountCents) === FIX.unpaidObligationCents &&
      unpaidRows.length === FIX.unpaidObligationRows,
    detail: `direct=${usd(directCents)} (fef subset ${usd(directFefCents)}), unpaid=${usd(sumCents(unpaidRows, (row) => row.amountCents))}`,
  });

  // Gate 4: header rejection.
  gates.push({
    name: "expenditure header = direct + unpaid + IE (never publishable)",
    pass:
      expenditureSweep.totalExpendituresAmountCents === FIX.expenditureHeaderCents &&
      expenditureSweep.totalExpendituresAmountCents ===
        directCents + sumCents(unpaidRows, (row) => row.amountCents) + ieCents &&
      ieCents === FIX.ieRowsCents,
    detail: `header=${usd(expenditureSweep.totalExpendituresAmountCents)} = ${usd(directCents)} + ${usd(sumCents(unpaidRows, (row) => row.amountCents))} + ${usd(ieCents)}`,
  });

  // --- Outside spenders. ---
  const supportSpenders = await getDenverOutsideSpenders({ ...JOHNSTON, direction: "support" });
  const opposeSpenders = await getDenverOutsideSpenders({ ...JOHNSTON, direction: "oppose" });
  for (const spender of supportSpenders) console.log(`  support ${usd(spender.totalCents)} ${spender.name}`);
  for (const spender of opposeSpenders) console.log(`  oppose  ${usd(spender.totalCents)} ${spender.name}`);

  // Gate 5: lists sum to the overview.
  const supportCents = sumCents(supportSpenders, (row) => row.totalCents);
  const opposeCents = sumCents(opposeSpenders, (row) => row.totalCents);
  gates.push({
    name: "outside spender lists sum exactly to overview IE fields",
    pass:
      supportCents === FIX.ieSupportCents &&
      opposeCents === FIX.ieOpposeCents &&
      supportCents === overview.independentExpendituresSupportingCandidateCents &&
      opposeCents === overview.independentExpendituresOpposingCandidateCents &&
      supportCents + opposeCents === ieCents,
    detail: `support=${usd(supportCents)} oppose=${usd(opposeCents)} (IE rows ${usd(ieCents)})`,
  });

  // Gate 6: spender-id resolution (exactly one type-3 match per name).
  const spenderNames = [...new Set([...supportSpenders, ...opposeSpenders].map((row) => row.name))];
  const unresolved: string[] = [];
  for (const name of spenderNames) {
    const typeThree = (await searchDenverCommitteesAndCandidates(name)).filter(
      (entry) => entry.type === DENVER_SEARCHLIGHT_SEARCH_TYPE_INDEPENDENT_EXPENDITURE
    );
    // Exact raw name first: IE entities exist that differ only by punctuation
    // ("A Better Denver" Ind808 vs "A Better Denver!" Ind678, verified live),
    // so a punctuation-insensitive match is ambiguous by construction.
    const exact = typeThree.filter((entry) => entry.name === name);
    const matches =
      exact.length === 1
        ? exact
        : typeThree.filter((entry) => normalizeName(entry.name) === normalizeName(name));
    if (matches.length !== 1) {
      unresolved.push(`${name} (${matches.length} matches)`);
      continue;
    }
    console.log(`  spender id ${matches[0]!.uniqueId} <- ${name}`);
  }
  gates.push({
    name: "every outside spender resolves to exactly one type-3 id",
    pass: unresolved.length === 0 && spenderNames.length > 0,
    detail: unresolved.length === 0 ? `${spenderNames.length} spenders resolved` : unresolved.join("; "),
  });

  // Gate 8: identity cardinality.
  const filer = await getDenverFiler(JOHNSTON.filerId);
  const entityIds = new Set(filer.committeeIds);
  const foreignContribution = contributionRows.find((row) => !entityIds.has(row.recipientCommitteeId));
  const foreignExpenditure = expenditureRows.find((row) => !entityIds.has(row.committeeId));
  gates.push({
    name: "filer entity ids pinned; every transaction row's entity id is in the set",
    pass:
      [...entityIds].sort((a, b) => a - b).join(",") === JOHNSTON_COMMITTEE_ENTITY_IDS.join(",") &&
      foreignContribution === undefined &&
      foreignExpenditure === undefined,
    detail: `committeeIds=[${[...entityIds].sort((a, b) => a - b).join(", ")}]${foreignContribution ? ` foreign contribution txn ${foreignContribution.transactionId}` : ""}${foreignExpenditure ? ` foreign expenditure txn ${foreignExpenditure.transactionId}` : ""}`,
  });

  // Gate 9: pagination stability (sweep twice + first-page recheck).
  const ids1 = sweep1.rows.map((row) => row.transactionId);
  const ids2 = sweep2.rows.map((row) => row.transactionId);
  const firstPageAgain = await searchDenverContributionTransactions({
    candidateName: JOHNSTON.candidateName,
    electionCycleIds: [JOHNSTON.electionCycleId],
    pageNum: 1,
    pageSize: 1_000,
  });
  const firstPageIds = firstPageAgain.rows.map((row) => row.transactionId);
  gates.push({
    name: "pagination: two sweeps identical, no duplicate ids, first page stable",
    pass:
      ids1.length === ids2.length &&
      ids1.every((id, index) => id === ids2[index]) &&
      new Set(ids1).size === ids1.length &&
      firstPageIds.every((id, index) => id === ids1[index]),
    detail: `${ids1.length} rows x2, ${new Set(ids1).size} distinct ids, first page recheck ${firstPageIds.length} rows`,
  });

  // Gate 10a: the filings endpoint is FILER-scoped — querying either committee
  // entity id returns the same filing set (verified live; querying both and
  // summing would double-count).
  const filingsByEntity = await Promise.all(
    JOHNSTON_COMMITTEE_ENTITY_IDS.map((entityId) => getDenverFilingsByCommittee({ committeeEntityId: entityId }))
  );
  const filingIdSets = filingsByEntity.map((filings) =>
    filings
      .map((filing) => filing.filingId)
      .sort((a, b) => a - b)
      .join(",")
  );
  gates.push({
    name: "filings endpoint is filer-scoped: both entity ids return the same set",
    pass: filingIdSets.every((set) => set === filingIdSets[0]),
    detail: `${filingsByEntity[0]!.length} filing versions per entity query`,
  });

  // Gate 10b: filed-report reconciliation on the single (deduplicated) set.
  // Event-based filings ("Major Contributions Report", null period) are early
  // disclosures of money that also appears on period reports — excluded, like
  // San Diego's F497 rule.
  const filings = filingsByEntity[0]!;
  const cycleFilings = filings.filter(
    (filing) => filing.electionCycleId === JOHNSTON.electionCycleId && filing.filingPeriodId !== null
  );
  const latest = selectLatestDenverFilings(cycleFilings);
  console.log(
    `filer filings: cycle ${JOHNSTON.electionCycleId}: ${cycleFilings.length} period versions -> ${latest.length} in force`
  );
  let reportedInflowCents = 0;
  let reportedRefundCents = 0;
  let reportedExpenditureCents = 0;
  let balanceChainHolds = latest.length > 0;
  let balanceIdentityHolds = latest.length > 0;
  let previousClosing: number | null = null;
  for (const filing of latest) {
    const summary = await getDenverFilingSummary(filing.filingId);
    const inflow =
      summary.totalMonetaryContributionsCents +
      summary.totalFefQualifyingContributionsCents +
      summary.totalInKindContributionsCents +
      summary.totalFairElectionsFundingCents;
    reportedInflowCents += inflow;
    reportedRefundCents += summary.totalRefundsCents;
    reportedExpenditureCents += summary.totalExpendituresCents;
    // Per-filing CASH identity (derived live: the residues across filings
    // 10052/10226/10698 sum to exactly the cycle's in-kind total, $2,357.45):
    // in-kind contributions are non-cash — they count toward raised but not
    // toward the balance, and totalExpenditures carries no in-kind twin.
    const expectedClosing =
      summary.openingBalanceCents +
      inflow -
      summary.totalInKindContributionsCents +
      summary.totalNonDonorFundsCents +
      summary.totalNewLoansCents -
      summary.totalRefundsCents -
      summary.totalExpendituresCents;
    if (expectedClosing !== summary.closingBalanceCents) {
      balanceIdentityHolds = false;
      console.log(
        `  balance identity break at filing ${filing.filingId}: expected close ${usd(expectedClosing)}, reported ${usd(summary.closingBalanceCents)}`
      );
    }
    console.log(
      `  filing ${filing.filingId} v${filing.filingVersion} ${filing.filingPeriodName ?? filing.filingPeriodId}: open=${usd(summary.openingBalanceCents)} in=${usd(inflow)} refunds=${usd(summary.totalRefundsCents)} out=${usd(summary.totalExpendituresCents)} close=${usd(summary.closingBalanceCents)}`
    );
    if (previousClosing !== null && summary.openingBalanceCents !== previousClosing) {
      balanceChainHolds = false;
      console.log(
        `  balance chain break at filing ${filing.filingId}: opening ${usd(summary.openingBalanceCents)} != prior closing ${usd(previousClosing)}`
      );
    }
    previousClosing = summary.closingBalanceCents;
  }
  console.log(
    `filed-report reconciliation (cycle ${JOHNSTON.electionCycleId}): gross in=${usd(reportedInflowCents)} refunds=${usd(reportedRefundCents)} net in=${usd(reportedInflowCents - reportedRefundCents)} out=${usd(reportedExpenditureCents)} vs endpoints in=${usd(contributionsTotal)} out=${usd(expendituresTotal)}`
  );
  gates.push({
    name: "filed reports: cash identity + chain hold; raised (net) and spend reconcile to the endpoints",
    pass:
      balanceIdentityHolds &&
      balanceChainHolds &&
      reportedInflowCents - reportedRefundCents === contributionsTotal &&
      reportedExpenditureCents === expendituresTotal,
    detail: `identity ${balanceIdentityHolds ? "holds" : "BREAKS"}, chain ${balanceChainHolds ? "holds" : "BREAKS"}, net in ${usd(reportedInflowCents - reportedRefundCents)}, spend ${usd(reportedExpenditureCents)}`,
  });

  // Gate 11: registration anomalies (cycle 36, live data — allowlist check).
  const registrants = await getDenverCandidatesByElectionCycle(VACANCY_CYCLE_ID);
  console.log(`\ncycle ${VACANCY_CYCLE_ID} registrants: ${registrants.length}`);
  const namesSeen = new Map<string, number>();
  for (const registrant of registrants) {
    namesSeen.set(normalizeName(registrant.fullName), (namesSeen.get(normalizeName(registrant.fullName)) ?? 0) + 1);
  }
  const anomalies: string[] = [];
  for (const [name, count] of namesSeen) {
    if (count > 1 && !DOCUMENTED_DUPLICATE_NAMES.has(name)) {
      anomalies.push(`undocumented duplicate name: ${name} x${count}`);
    }
  }
  for (const registrant of registrants) {
    const registrantFiler = await getDenverFiler(registrant.filerId);
    const cycles = await getDenverElectionCyclesByFiler(registrant.filerId);
    const inCycle = cycles.some((cycle) => cycle.electionCycleId === VACANCY_CYCLE_ID);
    console.log(
      `  ${registrant.fullName} (filer ${registrant.filerId}, committee ${registrant.committeeId}, ${registrantFiler.filerStatusName ?? "?"}${registrantFiler.isTerminated ? ", TERMINATED" : ""}): cycles [${cycles.map((cycle) => cycle.electionCycleId).join(", ")}]`
    );
    if (!inCycle && !DOCUMENTED_EMPTY_CYCLE_FILER_IDS.has(registrant.filerId)) {
      anomalies.push(`undocumented cycle-list gap: ${registrant.fullName} filer ${registrant.filerId}`);
    }
  }
  gates.push({
    name: "cycle-36 registration anomalies all documented",
    pass: anomalies.length === 0,
    detail: anomalies.length === 0 ? `${registrants.length} registrants, anomalies within allowlist` : anomalies.join("; "),
  });

  // Gate 12: PII allowlist on typed rows.
  const contributionKeys = Object.keys(contributionRows[0] as DenverContributionTransaction).sort();
  const expenditureKeys = Object.keys(expenditureRows[0] as DenverExpenditureTransaction).sort();
  const piiPattern = /address|zip/i;
  gates.push({
    name: "typed rows carry exactly the declared keys (no address/zip)",
    pass:
      contributionKeys.join(",") === CONTRIBUTION_ROW_KEYS.join(",") &&
      expenditureKeys.join(",") === EXPENDITURE_ROW_KEYS.join(",") &&
      !contributionKeys.some((key) => piiPattern.test(key)) &&
      !expenditureKeys.some((key) => piiPattern.test(key)),
    detail: `contribution keys ${contributionKeys.length}, expenditure keys ${expenditureKeys.length}`,
  });

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
    console.error("Denver candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}

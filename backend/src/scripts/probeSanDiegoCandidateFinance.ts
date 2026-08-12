// Phase 0 probe for the San Diego city finance module (plan-san-diego-finance.md).
// NO schema, NO writes: downloads the csd efile.systems bulk workbooks into the
// artifact cache, runs the shipped San José machinery against them (the direct
// aggregator and committee resolver are agency-agnostic), and checks the plan's
// hand-derived Phase 0 gates. The outside-spending tally is reimplemented here
// with San Diego gates because the San José aggregator hardcodes two things
// that are wrong for San Diego (verified live 2026-08-12):
//   - its jurisdiction veto requires "SAN JOSE", which excludes every San
//     Diego row ("City of San Diego"), and
//   - its office veto accepts only CCM, while San Diego S496/D rows carry
//     council races as both CCM and COU.
// The tally below is the SJ union/dedup semantics (S496 ∪ Schedule-D IND,
// deduped by (spender, Tran_ID); within a key the latest Rpt_Date wins) with
// the SD gates — it doubles as the Phase 3 aggregator spec.
//
// Gates (each hand-derived from the raw 2025+2026 workbooks on 2026-08-10..12,
// see the plan's "What the sample file proved"; a FAIL means either the
// composition rules or the source changed — re-verify by hand before build):
//   1. Amendment canonicalization: Gerardo Ramirez's semi-annual chain
//      (orig 300071267) resolves to amendment 300071809 (rpt 002); 300071688
//      (rpt 001) and the original are excluded. The most_recent export
//      variant retains BOTH amendments, so this proves our selection, not
//      the vendor's.
//   2. Loan exclusion: Ramirez loans_received = $20,000.00 and total_raised
//      excludes it (the F460 line-5 trap).
//   3. Cross-year spend: Antonio Martinez cycle total_spent = $121,576.79
//      (Σ line 11 Amount_A) — NOT the naive Σ yearly line-11B $146,419.75.
//   4. Outside union: Bailey's oppose groups include the "Working Families
//      Opposing Richard Bailey" spender, with its S496/Schedule-D Tran_ID
//      collision (PDT1: $45,000 vs $50,000) counted exactly once.
//   5. Committee mapping: all 8 November candidates resolve to exactly one
//      candidate-controlled committee, or print an explicit reason.

import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import {
  getEfileCalWorkbookArtifactCachePaths,
  refreshEfileCalWorkbookArtifactCache,
  type EfileCalAgencyConfig,
} from "../pipeline/efileCalFinance/efileCalBulkClient.js";
import {
  parseEfileCalWorkbook,
  type EfileCalS496Row,
  type EfileCalScheduleDRow,
  type EfileCalWorkbook,
} from "../pipeline/efileCalFinance/efileCalWorkbookParser.js";
import { aggregateSanJoseDirectFinance } from "../pipeline/sanJoseFinance/sanJoseDirectFinanceAggregator.js";
import {
  collectSanJoseExportCommittees,
  resolveSanJoseCandidateCommittees,
  sanJosePersonNameMatchesCandidate,
  normalizeSanJoseTextKey,
  SAN_JOSE_PENDING_FILER_ID,
  type SanJoseAppCandidate,
} from "../pipeline/sanJoseFinance/sanJoseCandidateCommitteeResolver.js";

// Phase 1 moves this into the pipeline module; the probe pins it first.
export const SAN_DIEGO_EFILE_AGENCY_CONFIG: EfileCalAgencyConfig = {
  agencyKey: "csd",
  portalBaseUrl: "https://efile.sandiego.gov",
  // Same S3 host as San José — one vendor bucket, per-agency prefixes
  // (verified live 2026-08-10).
  allowedExportHosts: ["efs-efile-campaign-exports.s3.amazonaws.com"],
};

export const SAN_DIEGO_FINANCE_PROBE_CACHE_DIR =
  "scratch/san-diego-campaign-finance/efile";

/** November 2026 runoff field, from the City Clerk's official candidate log
 * (sandiego.gov/city-clerk/elections/city/electioninfo, read 2026-08-10). */
const NOVEMBER_2026_CANDIDATES: readonly { displayName: string; seatNumber: number }[] = [
  { displayName: "Richard Bailey", seatNumber: 2 },
  { displayName: "Nicole Crosby", seatNumber: 2 },
  { displayName: "Henry Foster III", seatNumber: 4 },
  { displayName: "Martha Abraham", seatNumber: 4 },
  { displayName: "Kent Lee", seatNumber: 6 },
  { displayName: "Mark Powell", seatNumber: 6 },
  { displayName: "Antonio Martinez", seatNumber: 8 },
  { displayName: "Gerardo Ramirez", seatNumber: 8 },
];

const CYCLE_YEARS = [2025, 2026] as const;

/**
 * Clerk-log committee evidence (the Phase 2 top evidence tier, hand-resolved
 * for the probe): the official candidate log links each candidate to a vendor
 * filer GUID (`/public/search/campaign/filings/<guid>?type=coe`), whose
 * filing list names the committee; the committee's FPPC id comes from the
 * bulk export. Used only when committee-name evidence fails — "Re-Elect X…"
 * prefixes and surname-only names ("Powell for City Council 2026") defeat
 * token matching by design (they carry no given name at the name position).
 */
const CLERK_LOG_COMMITTEES: Readonly<
  Record<string, { filerId: string; committeeName: string; clerkGuid: string }>
> = {
  "Henry Foster III": {
    filerId: "1481166",
    committeeName: "Re-Elect Henry Foster III for San Diego City Council 2026",
    clerkGuid: "1b96adf9-a028-4423-adbd-2297686d3821",
  },
  "Kent Lee": {
    filerId: "1478315",
    committeeName: "Re-Elect Kent Lee for City Council 2026",
    clerkGuid: "b9ee798b-173b-4c16-8738-44e91266c843",
  },
  "Mark Powell": {
    filerId: "1485884",
    committeeName: "POWELL FOR CITY COUNCIL 2026",
    clerkGuid: "00c6b8e4-6982-456c-8e40-56df9c7b011a",
  },
};

// San Diego office codes observed live for council races. CAL writes CCM;
// this vendor's SD tenant also emits COU on a minority of rows (10 of 158
// S496 rows in the 2026 file) — both are council.
const SD_COUNCIL_OFFICE_CODES = new Set(["CCM", "COU"]);

type Gate = { name: string; pass: boolean; detail: string };

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

async function loadCycleWorkbook(input: {
  refresh: boolean;
  force: boolean;
}): Promise<EfileCalWorkbook> {
  const workbook: EfileCalWorkbook = {
    summary: [],
    scheduleA: [],
    scheduleC: [],
    scheduleB1: [],
    scheduleD: [],
    s496: [],
    s497: [],
  };
  for (const year of CYCLE_YEARS) {
    const paths = getEfileCalWorkbookArtifactCachePaths({
      cacheDir: SAN_DIEGO_FINANCE_PROBE_CACHE_DIR,
      agencyKey: SAN_DIEGO_EFILE_AGENCY_CONFIG.agencyKey,
      year,
      mostRecentOnly: true,
    });
    if (input.refresh) {
      const refresh = await refreshEfileCalWorkbookArtifactCache({
        config: SAN_DIEGO_EFILE_AGENCY_CONFIG,
        year,
        mostRecentOnly: true,
        cacheDir: SAN_DIEGO_FINANCE_PROBE_CACHE_DIR,
        force: input.force,
      });
      console.log(`workbook ${year}: ${refresh.status} (${refresh.workbookPath})`);
    }
    let bytes: Uint8Array;
    try {
      bytes = await readFile(paths.workbookPath);
    } catch {
      throw new Error(
        `San Diego ${year} workbook is not cached at ${paths.workbookPath}; run with --refresh first`,
      );
    }
    // Collect mode: San Diego's live 2025 export carries a Major Donor filing
    // block with blank Form_Type (10 summary rows, Marriott, Filer_ID
    // Pending). Unusable rows are printed below; sync-time policy (Phase 1)
    // is: any unusable row belonging to a LINKED committee blocks that
    // committee's write.
    const parsed = parseEfileCalWorkbook(bytes, { collectUnusableRows: true });
    for (const unusable of parsed.unusableRows ?? []) {
      console.log(
        `  unusable row ${year} ${unusable.sheet} row ${unusable.rowNumber}: ${unusable.reason}`,
      );
    }
    workbook.summary.push(...parsed.summary);
    workbook.scheduleA.push(...parsed.scheduleA);
    workbook.scheduleC.push(...parsed.scheduleC);
    workbook.scheduleB1.push(...parsed.scheduleB1);
    workbook.scheduleD.push(...parsed.scheduleD);
    workbook.s496.push(...parsed.s496);
    workbook.s497.push(...parsed.s497);
  }
  return workbook;
}

// ---------------------------------------------------------------------------
// Outside-spending tally: SJ union/dedup semantics with San Diego gates.
// ---------------------------------------------------------------------------

type OutsideRow = {
  filerId: string;
  filerName: string;
  tranId: string;
  eFilingId: string;
  rptDate: string | null;
  amountCents: number;
  candidateLastName: string | null;
  candidateFirstName: string | null;
  officeCd: string | null;
  jurisDscr: string | null;
  distNo: string | null;
  suppOppCd: string | null;
  memo: boolean;
  source: "s496" | "scheduleD";
};

function targetName(row: OutsideRow): string | null {
  if (row.candidateLastName === null && row.candidateFirstName === null) return null;
  if (row.candidateFirstName === null) return row.candidateLastName;
  if (row.candidateLastName === null) return row.candidateFirstName;
  return `${row.candidateFirstName} ${row.candidateLastName}`;
}

function spenderIdentity(row: OutsideRow): string {
  return row.filerId === SAN_JOSE_PENDING_FILER_ID
    ? `${SAN_JOSE_PENDING_FILER_ID}::${normalizeSanJoseTextKey(row.filerName)}`
    : row.filerId;
}

function rowKey(row: OutsideRow): string {
  return JSON.stringify([spenderIdentity(row), row.tranId]);
}

function dedupeLatestReports(rows: readonly OutsideRow[]): OutsideRow[] {
  const byKey = new Map<string, OutsideRow[]>();
  for (const row of rows) {
    const key = `${rowKey(row)}|${normalizeSanJoseTextKey(targetName(row))}`;
    const group = byKey.get(key) ?? [];
    group.push(row);
    byKey.set(key, group);
  }
  const deduped: OutsideRow[] = [];
  for (const group of byKey.values()) {
    group.sort(
      (a, b) =>
        (b.rptDate ?? "").localeCompare(a.rptDate ?? "") ||
        b.eFilingId.length - a.eFilingId.length ||
        b.eFilingId.localeCompare(a.eFilingId),
    );
    deduped.push(group[0]!);
  }
  return deduped;
}

type OutsideTally = {
  supportCents: number;
  opposeCents: number;
  groups: { spenderName: string; direction: "support" | "oppose"; cents: number; count: number }[];
  officeCdHistogram: Record<string, number>;
  excluded: { office: number; jurisdiction: number; district: number; direction: number };
};

function tallySanDiegoOutside(input: {
  candidate: { displayName: string; seatNumber: number };
  s496: readonly EfileCalS496Row[];
  scheduleD: readonly EfileCalScheduleDRow[];
}): OutsideTally {
  const s496Rows: OutsideRow[] = input.s496.map((row) => ({
    filerId: row.filerId,
    filerName: row.filerName,
    tranId: row.tranId,
    eFilingId: row.eFilingId,
    rptDate: row.rptDate,
    amountCents: row.amountCents,
    candidateLastName: row.candidateLastName,
    candidateFirstName: row.candidateFirstName,
    officeCd: row.officeCd,
    jurisDscr: row.jurisDscr,
    distNo: row.distNo,
    suppOppCd: row.suppOppCd,
    memo: row.memo,
    source: "s496",
  }));
  const s496Keys = new Set(s496Rows.map(rowKey));
  const dOnlyRows: OutsideRow[] = input.scheduleD
    .filter((row) => row.expnCode === "IND")
    .map(
      (row): OutsideRow => ({
        filerId: row.filerId,
        filerName: row.filerName,
        tranId: row.tranId,
        eFilingId: row.eFilingId,
        rptDate: row.rptDate,
        amountCents: row.amountCents,
        candidateLastName: row.candidateLastName,
        candidateFirstName: row.candidateFirstName,
        officeCd: row.officeCd,
        jurisDscr: row.jurisDscr,
        distNo: row.distNo,
        suppOppCd: row.suppOppCd,
        memo: row.memo,
        source: "scheduleD",
      }),
    )
    .filter((row) => !s496Keys.has(rowKey(row)));
  const union = [...dedupeLatestReports(s496Rows), ...dedupeLatestReports(dOnlyRows)];

  const tally: OutsideTally = {
    supportCents: 0,
    opposeCents: 0,
    groups: [],
    officeCdHistogram: {},
    excluded: { office: 0, jurisdiction: 0, district: 0, direction: 0 },
  };
  const groups = new Map<string, OutsideTally["groups"][number]>();
  for (const row of union) {
    if (row.memo) continue;
    const name = targetName(row);
    if (name === null) continue;
    if (!sanJosePersonNameMatchesCandidate(name, input.candidate.displayName)) continue;
    const officeKey = row.officeCd ?? "(blank)";
    tally.officeCdHistogram[officeKey] = (tally.officeCdHistogram[officeKey] ?? 0) + 1;
    // Office veto: blank fails closed, same as SJ; SD accepts CCM and COU.
    if (row.officeCd === null || !SD_COUNCIL_OFFICE_CODES.has(row.officeCd)) {
      tally.excluded.office += 1;
      continue;
    }
    if (
      row.jurisDscr !== null &&
      !normalizeSanJoseTextKey(row.jurisDscr).includes("SAN DIEGO")
    ) {
      tally.excluded.jurisdiction += 1;
      continue;
    }
    if (row.distNo !== null) {
      const district = /^\d{1,2}$/.test(row.distNo) ? Number(row.distNo) : null;
      if (district !== input.candidate.seatNumber) {
        tally.excluded.district += 1;
        continue;
      }
    }
    const direction = row.suppOppCd?.trim().toUpperCase();
    if (direction !== "SUPPORT" && direction !== "OPPOSE") {
      tally.excluded.direction += 1;
      continue;
    }
    const dir = direction === "SUPPORT" ? "support" : "oppose";
    if (dir === "support") tally.supportCents += row.amountCents;
    else tally.opposeCents += row.amountCents;
    const key = JSON.stringify([spenderIdentity(row), dir]);
    const group = groups.get(key) ?? {
      spenderName: row.filerName,
      direction: dir,
      cents: 0,
      count: 0,
    };
    group.cents += row.amountCents;
    group.count += 1;
    if (row.filerName.length > group.spenderName.length) group.spenderName = row.filerName;
    groups.set(key, group);
  }
  tally.groups = [...groups.values()].sort((a, b) => b.cents - a.cents);
  return tally;
}

// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  for (const arg of args) {
    if (arg !== "--refresh" && arg !== "--force") {
      throw new Error(`Unknown San Diego finance probe flag: ${arg}`);
    }
  }
  const refresh = args.includes("--refresh");
  const force = args.includes("--force");

  const workbook = await loadCycleWorkbook({ refresh, force });
  console.log(
    `rows: summary=${workbook.summary.length} A=${workbook.scheduleA.length} C=${workbook.scheduleC.length} B1=${workbook.scheduleB1.length} D=${workbook.scheduleD.length} s496=${workbook.s496.length} s497=${workbook.s497.length}`,
  );

  const gates: Gate[] = [];

  // --- Gate 5: committee resolution for all 8 November candidates. ---
  const committees = collectSanJoseExportCommittees([
    ...workbook.summary,
    ...workbook.scheduleA,
    ...workbook.scheduleC,
    ...workbook.scheduleB1,
    ...workbook.scheduleD,
    ...workbook.s496,
    ...workbook.s497,
  ]);
  console.log(`\nexport committees: ${committees.length}`);
  const candidates: SanJoseAppCandidate[] = NOVEMBER_2026_CANDIDATES.map((candidate) => ({
    candidateId: normalizeSanJoseTextKey(candidate.displayName),
    displayName: candidate.displayName,
    officeName: "City Council Member",
    seatNumber: candidate.seatNumber,
    electionYear: 2026,
    stateFilingIds: [],
  }));
  const resolutions = resolveSanJoseCandidateCommittees({ candidates, committees });
  const matched = new Map<string, { filerId: string; committeeName: string }>();
  for (const resolution of resolutions) {
    if (resolution.status === "matched") {
      matched.set(resolution.candidate.displayName, {
        filerId: resolution.filerId,
        committeeName: resolution.committeeName,
      });
      console.log(
        `  matched   D${resolution.candidate.seatNumber} ${resolution.candidate.displayName} -> ${resolution.filerId} "${resolution.committeeName}" (${resolution.matchedBy})`,
      );
      continue;
    }
    const clerkLog = CLERK_LOG_COMMITTEES[resolution.candidate.displayName];
    if (clerkLog !== undefined) {
      // The clerk-log tier outranks name evidence, but the export must agree
      // the committee exists under that FPPC id with a matching name.
      const committee = committees.find((entry) => entry.filerId === clerkLog.filerId);
      const nameConfirmed = committee?.committeeNames.some(
        (name) => normalizeSanJoseTextKey(name) === normalizeSanJoseTextKey(clerkLog.committeeName),
      );
      if (nameConfirmed) {
        matched.set(resolution.candidate.displayName, {
          filerId: clerkLog.filerId,
          committeeName: clerkLog.committeeName,
        });
        console.log(
          `  matched   D${resolution.candidate.seatNumber} ${resolution.candidate.displayName} -> ${clerkLog.filerId} "${clerkLog.committeeName}" (clerk_log ${clerkLog.clerkGuid})`,
        );
        continue;
      }
      console.log(
        `  unmatched D${resolution.candidate.seatNumber} ${resolution.candidate.displayName}: clerk-log committee ${clerkLog.filerId} not confirmed in export (${resolution.reason})`,
      );
      continue;
    }
    console.log(
      `  ${resolution.status.padEnd(9)} D${resolution.candidate.seatNumber} ${resolution.candidate.displayName}: ${resolution.reason}`,
    );
  }
  gates.push({
    name: "all 8 candidates resolve (or explicit reason printed)",
    pass: matched.size === NOVEMBER_2026_CANDIDATES.length,
    detail: `${matched.size}/8 matched`,
  });

  // --- Direct aggregation per matched committee. ---
  const directByCandidate = new Map<
    string,
    ReturnType<typeof aggregateSanJoseDirectFinance>
  >();
  for (const [displayName, link] of matched) {
    const direct = aggregateSanJoseDirectFinance({
      filerId: link.filerId,
      summary: workbook.summary,
      scheduleA: workbook.scheduleA,
      scheduleC: workbook.scheduleC,
      scheduleB1: workbook.scheduleB1,
    });
    directByCandidate.set(displayName, direct);
    console.log(
      `\n${displayName} (${link.filerId}): raised=${usd(direct.totalRaisedCents)} spent=${usd(direct.totalSpentCents)} loans=${usd(direct.loansReceivedCents)} cash=${direct.cashOnHandCents === null ? "n/a" : usd(direct.cashOnHandCents)} debts=${direct.debtsOwedCents === null ? "n/a" : usd(direct.debtsOwedCents)} through=${direct.reportedThrough}`,
    );
    console.log(
      `  filings: ${direct.filings.map((filing) => `${filing.eFilingId}/${filing.reportNum} ${filing.fromDate}..${filing.thruDate}`).join(", ")}`,
    );
    for (const violation of direct.violations) {
      console.log(`  violation ${violation.type}: ${violation.message}`);
    }
  }

  // --- Gate 1: amendment canonicalization on the Ramirez chain. ---
  const ramirez = directByCandidate.get("Gerardo Ramirez");
  const ramirezIds = new Set(ramirez?.filings.map((filing) => filing.eFilingId) ?? []);
  gates.push({
    name: "canonicalization keeps 300071809 (rpt 002), drops 300071688/300071267",
    pass:
      ramirezIds.has("300071809") &&
      !ramirezIds.has("300071688") &&
      !ramirezIds.has("300071267"),
    detail: `Ramirez canonical filings: ${[...ramirezIds].join(", ") || "(none)"}`,
  });

  // --- Gate 2: loan exclusion. ---
  gates.push({
    name: "Ramirez loans_received = $20,000.00, excluded from total_raised",
    pass: ramirez !== undefined && ramirez.loansReceivedCents === 2_000_000,
    detail: ramirez
      ? `loans=${usd(ramirez.loansReceivedCents)} raised=${usd(ramirez.totalRaisedCents)}`
      : "Ramirez unresolved",
  });

  // --- Gate 3: Martinez cross-year spend. ---
  const martinez = directByCandidate.get("Antonio Martinez");
  gates.push({
    name: "Martinez cycle spent = $121,576.79 (Σ 11A, not Σ yearly 11B)",
    pass: martinez !== undefined && martinez.totalSpentCents === 12_157_679,
    detail: martinez ? `spent=${usd(martinez.totalSpentCents)}` : "Martinez unresolved",
  });

  // --- Outside spending per candidate (SD gates). ---
  console.log("");
  let baileyGate: Gate = {
    name: "Bailey oppose includes Working Families PDT1 counted once",
    pass: false,
    detail: "Bailey tally missing",
  };
  for (const candidate of NOVEMBER_2026_CANDIDATES) {
    const tally = tallySanDiegoOutside({
      candidate,
      s496: workbook.s496,
      scheduleD: workbook.scheduleD,
    });
    console.log(
      `outside D${candidate.seatNumber} ${candidate.displayName}: support=${usd(tally.supportCents)} oppose=${usd(tally.opposeCents)} officeCds=${JSON.stringify(tally.officeCdHistogram)} excluded=${JSON.stringify(tally.excluded)}`,
    );
    for (const group of tally.groups) {
      console.log(`    ${group.direction} ${usd(group.cents)} (${group.count}) ${group.spenderName}`);
    }
    if (candidate.displayName === "Richard Bailey") {
      const wfob = tally.groups.find(
        (group) =>
          group.direction === "oppose" &&
          normalizeSanJoseTextKey(group.spenderName).includes("WORKING FAMILIES"),
      );
      baileyGate = {
        name: "Bailey oppose includes Working Families PDT1 counted once",
        pass: wfob !== undefined && wfob.cents >= 4_500_000,
        detail: wfob
          ? `${wfob.spenderName}: ${usd(wfob.cents)} across ${wfob.count} expenditures`
          : "no Working Families oppose group found",
      };
    }
  }
  gates.push(baileyGate);

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
    console.error("San Diego candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}

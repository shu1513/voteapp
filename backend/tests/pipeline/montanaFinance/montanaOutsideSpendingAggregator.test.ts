import { describe, expect, it } from "vitest";

import type {
  MontanaCersCandidateSearchRow,
  MontanaCersIeSweepArtifact,
  MontanaCersIeTransactionRow,
} from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  MONTANA_IE_RECOVERY_TOLERANCE_CENTS,
  montanaIeAttachmentRecoveryFor,
} from "../../../src/pipeline/montanaFinance/montanaOutsideAttachmentRecovery.js";
import {
  aggregateMontanaOutsideSpendingForCandidate,
  classifyMontanaOutsideSpendingRows,
  montanaIeClassifiedAmountCents,
  montanaIeCycleWindow,
  parseMontanaCandidateIssue,
  resolveMontanaIeTarget,
  summarizeMontanaOutsideSpendingByCommittee,
} from "../../../src/pipeline/montanaFinance/montanaOutsideSpendingAggregator.js";

function registration(overrides: Partial<MontanaCersCandidateSearchRow>): MontanaCersCandidateSearchRow {
  return {
    candidateId: 21020,
    lastName: "Bedey",
    firstName: "David",
    middleInitial: "F.",
    electionYear: 2026,
    officeTitle: "Senate District No. 43",
    officeCode: "SD",
    partyDescr: "Republican",
    candidateStatusDescr: "Active",
    resCountyDescr: null,
    ...overrides,
  };
}

// The 2026 registration slice the live corpus exercised (2026-08-28),
// including a race-switcher whose dead registrations carry their own
// candidateIds and a nickname-formal pair.
const REGISTRATIONS: MontanaCersCandidateSearchRow[] = [
  registration({}),
  registration({ candidateId: 21077, lastName: "Wirth", firstName: "Zachary", middleInitial: "E", officeTitle: "Senate District No. 09", candidateStatusDescr: "Closed" }),
  registration({ candidateId: 21041, lastName: "Walsh", firstName: "Kenneth", middleInitial: "M", officeTitle: "House District No. 69", candidateStatusDescr: "Closed" }),
  registration({ candidateId: 21466, lastName: "Nikolakakos", firstName: "George", middleInitial: "P", officeTitle: "Senate District No. 12", candidateStatusDescr: "Reopened" }),
  registration({ candidateId: 20659, lastName: "Nikolakakos", firstName: "George", middleInitial: "P", officeTitle: "Senate District No. 11", candidateStatusDescr: "Closed" }),
  registration({ candidateId: 21166, lastName: "Nikolakakos", firstName: "George", middleInitial: null, officeTitle: "House District No. 22", candidateStatusDescr: "Closed" }),
  registration({ candidateId: 20476, lastName: "Love", firstName: "Kathy", middleInitial: "E", officeTitle: "Senate District No. 43", candidateStatusDescr: "Active" }),
  registration({ candidateId: 30001, lastName: "Sanders", firstName: "David", middleInitial: null, officeTitle: "Public Service Commission District No. 5" }),
  registration({ candidateId: 30002, lastName: "Wilson", firstName: "Dan", middleInitial: null, officeTitle: "Supreme Court Justice No. 04" }),
  // Same-year same-name pair on DIFFERENT live registrations: never resolvable.
  registration({ candidateId: 30003, lastName: "Smith", firstName: "Pat", officeTitle: "House District No. 01" }),
  registration({ candidateId: 30004, lastName: "Smith", firstName: "Pat", officeTitle: "House District No. 02" }),
];

describe("parseMontanaCandidateIssue", () => {
  it("maps the corpus grammar to single stance-tagged targets", () => {
    expect(parseMontanaCandidateIssue("ZACK WIRTH (SD-9)")).toEqual({
      kind: "target",
      stance: "support",
      name: "ZACK WIRTH",
      office: { kind: "legislative_upper", districtNumber: 9 },
    });
    expect(parseMontanaCandidateIssue("Oppose George Nikolakakos")).toMatchObject({
      kind: "target",
      stance: "oppose",
      name: "George Nikolakakos",
      office: null,
    });
    expect(parseMontanaCandidateIssue("Opposing Susan Geise")).toMatchObject({ stance: "oppose", name: "Susan Geise" });
    expect(parseMontanaCandidateIssue("Bedey SD43")).toMatchObject({
      stance: "support",
      name: "Bedey",
      office: { kind: "legislative_upper", districtNumber: 43 },
    });
    // Slash-then-token is punctuation, not a second candidate.
    expect(parseMontanaCandidateIssue("Shelley Vance/SD 34")).toMatchObject({
      name: "Shelley Vance",
      office: { kind: "legislative_upper", districtNumber: 34 },
    });
    // Token-first with a separator ("SD 14; Russ Tempel").
    expect(parseMontanaCandidateIssue("SD 14; Russ Tempel")).toMatchObject({
      name: "Russ Tempel",
      office: { kind: "legislative_upper", districtNumber: 14 },
    });
    expect(parseMontanaCandidateIssue("David Sanders for PSC 5")).toMatchObject({
      name: "David Sanders",
      office: { kind: "psc", districtNumber: 5 },
    });
    expect(parseMontanaCandidateIssue("Dan Wilson for Montana Supreme Court")).toMatchObject({
      name: "Dan Wilson",
      office: { kind: "supreme_court" },
    });
    expect(parseMontanaCandidateIssue("Dan Wilson in support for MT Supreme Court")).toMatchObject({
      stance: "support",
      name: "Dan Wilson",
    });
    expect(parseMontanaCandidateIssue("Barry Usher for Montana State Senate")).toMatchObject({
      name: "Barry Usher",
      office: { kind: "legislative_upper", districtNumber: null },
    });
  });

  it("quarantines blanks, attachments, ballot issues, and unresolvable office families", () => {
    expect(parseMontanaCandidateIssue(null)).toEqual({ kind: "quarantine", reason: "blank_target" });
    expect(parseMontanaCandidateIssue("  ")).toEqual({ kind: "quarantine", reason: "blank_target" });
    expect(parseMontanaCandidateIssue("none")).toEqual({ kind: "quarantine", reason: "blank_target" });
    for (const value of ["See attached", "Various - se attachment", "(see addendum)", "See Quantity field for list of candidates", "Primary candidate mailers (see list under specific services)"]) {
      expect(parseMontanaCandidateIssue(value)).toEqual({ kind: "quarantine", reason: "attachment_reference" });
    }
    for (const value of ["Oppose CI126", "CI-132", "Philipsburg Library Levy", "In support of Ballot Initiative CI-132"]) {
      expect(parseMontanaCandidateIssue(value)).toEqual({ kind: "quarantine", reason: "ballot_issue" });
    }
    // Municipal/federal targets are not on the even-year registration list;
    // a name-only match would risk the wrong person entirely.
    for (const value of ["Hunter for Kalispell Mayor", "Amy Aguirre for Billings City Council Ward 3"]) {
      expect(parseMontanaCandidateIssue(value)).toEqual({ kind: "quarantine", reason: "unsupported_office" });
    }
  });

  it("quarantines every multi-candidate join, including dual-stance rows", () => {
    for (const value of [
      "Support Gianforte / Oppose Busse",
      "Jones, Bedey, Rindal, Wirth, Love, Usher",
      "Love SD43; Gunderson SD1",
      "Emma Kerr-Carpenter and Denise Baum",
      "HD69 Oppose Ken Walsh or Support Trevor Walter",
      "Wirth/Jones",
      "Support SD 43 Bedey. Support HD 55 Barker",
      "Russ Ehnes/Bonnie Fogerty",
    ]) {
      expect(parseMontanaCandidateIssue(value)).toEqual({ kind: "quarantine", reason: "multi_candidate" });
    }
  });
});

describe("resolveMontanaIeTarget", () => {
  it("resolves nicknames one-sidedly against formal registrations", () => {
    expect(resolveMontanaIeTarget({ name: "Ken Walsh", office: null }, REGISTRATIONS)).toMatchObject({
      status: "resolved",
      cersCandidateId: 21041,
    });
    expect(
      resolveMontanaIeTarget(
        { name: "ZACK WIRTH", office: { kind: "legislative_upper", districtNumber: 9 } },
        REGISTRATIONS
      )
    ).toMatchObject({ status: "resolved", cersCandidateId: 21077 });
  });

  it("breaks race-switcher ambiguity toward the single live registration", () => {
    // CERS mints a candidateId per race: George holds SD-12 Reopened plus
    // two Closed registrations. The live one is the row auto-link binds.
    expect(resolveMontanaIeTarget({ name: "George Nikolakakos", office: null }, REGISTRATIONS)).toMatchObject({
      status: "resolved",
      cersCandidateId: 21466,
    });
  });

  it("keeps two live same-name registrations ambiguous", () => {
    expect(resolveMontanaIeTarget({ name: "Pat Smith", office: null }, REGISTRATIONS)).toEqual({
      status: "quarantined",
      reason: "ambiguous_name",
    });
  });

  it("treats a contradicting office token as a conflict, never decoration", () => {
    // Live corpus: "KATHY LOVE (SD-9)" against Love's SD-43 registration.
    expect(
      resolveMontanaIeTarget({ name: "KATHY LOVE", office: { kind: "legislative_upper", districtNumber: 9 } }, REGISTRATIONS)
    ).toEqual({ status: "quarantined", reason: "office_conflict" });
  });

  it("fails closed on typos instead of fuzzy-matching", () => {
    expect(resolveMontanaIeTarget({ name: "Buttery", office: null }, REGISTRATIONS)).toEqual({
      status: "quarantined",
      reason: "unresolved_name",
    });
    // A typo'd stance verb rides in the name tokens and blocks alignment —
    // it can never silently flip to bare-name support.
    expect(resolveMontanaIeTarget({ name: "Oppse Kathy Love", office: null }, REGISTRATIONS)).toEqual({
      status: "quarantined",
      reason: "unresolved_name",
    });
  });

  it("resolves a unique last-name-only target", () => {
    expect(resolveMontanaIeTarget({ name: "Bedey", office: null }, REGISTRATIONS)).toMatchObject({
      status: "resolved",
      cersCandidateId: 21020,
    });
  });
});

function ieRow(overrides: Partial<MontanaCersIeTransactionRow>): MontanaCersIeTransactionRow {
  return {
    transId: 1,
    transTypeDescr: "Independent Expenditure",
    amountTypeDescr: "Primary",
    cashAmtCents: 10_000,
    inKindAmtCents: 0,
    totalAmtCents: 10_000,
    datePaid: Date.UTC(2026, 5, 1),
    candidateIssue: "David Bedey",
    purposeDescr: "Mailers",
    electioneeringInd: "N",
    ...overrides,
  };
}

function sweep(rowsByCommittee: [number, string, MontanaCersIeTransactionRow[]][]): MontanaCersIeSweepArtifact {
  return {
    year: 2026,
    committees: rowsByCommittee.map(([committeeId, committeeName]) => ({
      committeeId,
      committeeName,
      committeeTypeCode: "IN",
      committeeTypeDescr: "Independent",
      electionYear: null,
    })),
    transactionsByCommitteeId: new Map(rowsByCommittee.map(([committeeId, , rows]) => [committeeId, rows])),
  };
}

describe("attachment recovery", () => {
  const C4MT_TRANS_ID = 1990730;
  const C4MT_TOTAL_CENTS = 39_275_155;

  it("expands a reconciling attachment into per-candidate entries with declared stances", () => {
    // The filed PDF discloses what "See attached" hid. The lump row is
    // replaced (never counted alongside its own breakdown), each entry runs
    // the ordinary parse + resolve path, and oppose rows stay oppose.
    const classified = classifyMontanaOutsideSpendingRows({
      sweep: sweep([
        [10641, "Conservatives4MT", [ieRow({ transId: C4MT_TRANS_ID, totalAmtCents: C4MT_TOTAL_CENTS, candidateIssue: "See attached" })]],
      ]),
      registrationRows: REGISTRATIONS,
      electionYear: 2026,
    });
    expect(classified.length).toBeGreaterThan(40);
    expect(classified.every((entry) => entry.recoveredFromAttachmentId === 4797)).toBe(true);
    // Never the lump: every entry carries its own recovered split.
    expect(classified.some((entry) => entry.recoveredAmountCents === undefined)).toBe(false);
    const total = classified.reduce((sum, entry) => sum + montanaIeClassifiedAmountCents(entry), 0);
    expect(Math.abs(total - C4MT_TOTAL_CENTS)).toBeLessThanOrEqual(MONTANA_IE_RECOVERY_TOLERANCE_CENTS);
    // Bedey is disclosed in the attachment and resolves like a typed target.
    const bedey = classified.find(
      (entry) => entry.outcome.kind === "resolved" && entry.outcome.cersCandidateName.includes("Bedey")
    );
    expect(bedey?.outcome).toMatchObject({ kind: "resolved", stance: "support" });
    expect(classified.some((entry) => entry.outcome.kind === "resolved" && entry.outcome.stance === "oppose")).toBe(true);
  });

  it("ignores a recovery that does not reconcile to the row it is attached to", () => {
    // A breakdown covering a different scope than the transaction is not
    // evidence about that transaction — the row stays quarantined.
    const classified = classifyMontanaOutsideSpendingRows({
      sweep: sweep([
        [10641, "Conservatives4MT", [ieRow({ transId: C4MT_TRANS_ID, totalAmtCents: C4MT_TOTAL_CENTS + 50_000, candidateIssue: "See attached" })]],
      ]),
      registrationRows: REGISTRATIONS,
      electionYear: 2026,
    });
    expect(classified).toHaveLength(1);
    expect(classified[0]!.outcome).toEqual({ kind: "quarantined", reason: "attachment_reference" });
    expect(classified[0]!.recoveredAmountCents).toBeUndefined();
  });

  it("keeps the stored recovery reconciled to its documented transaction", () => {
    // Guards future edits to the table: the entries must keep summing to
    // the transaction amount recorded in the module comment.
    const recovery = montanaIeAttachmentRecoveryFor(C4MT_TRANS_ID);
    expect(recovery).not.toBeNull();
    const sum = recovery!.entries.reduce((total, entry) => total + entry.amountCents, 0);
    expect(Math.abs(sum - C4MT_TOTAL_CENTS)).toBeLessThanOrEqual(MONTANA_IE_RECOVERY_TOLERANCE_CENTS);
    expect(montanaIeAttachmentRecoveryFor(424242)).toBeNull();
  });
});

describe("classifyMontanaOutsideSpendingRows", () => {
  it("scopes by date window, dedupes transIds, and excludes electioneering and non-IE rows", () => {
    const window = montanaIeCycleWindow(2026);
    expect(new Date(window.startMs).toISOString()).toBe("2025-01-01T00:00:00.000Z");
    const classified = classifyMontanaOutsideSpendingRows({
      sweep: sweep([
        [
          100,
          "Good PAC",
          [
            ieRow({ transId: 1 }),
            ieRow({ transId: 2, datePaid: Date.UTC(2020, 5, 1) }),
            ieRow({ transId: 1 }),
            ieRow({ transId: 3, electioneeringInd: "Y" }),
            ieRow({ transId: 4, transTypeDescr: "Committee Contribution" }),
          ],
        ],
      ]),
      registrationRows: REGISTRATIONS,
      electionYear: 2026,
    });
    expect(classified.map((entry) => entry.outcome)).toEqual([
      expect.objectContaining({ kind: "resolved", stance: "support", cersCandidateId: 21020 }),
      { kind: "excluded", reason: "out_of_cycle" },
      { kind: "excluded", reason: "duplicate_trans_id" },
      { kind: "excluded", reason: "electioneering" },
      { kind: "excluded", reason: "non_ie_transaction" },
    ]);
  });

  it("ignores other-year registrations during resolution", () => {
    const classified = classifyMontanaOutsideSpendingRows({
      sweep: sweep([[100, "Good PAC", [ieRow({ candidateIssue: "Old Timer" })]]]),
      registrationRows: [registration({ candidateId: 999, lastName: "Timer", firstName: "Old", electionYear: 2024 })],
      electionYear: 2026,
    });
    expect(classified[0]!.outcome).toEqual({ kind: "quarantined", reason: "unresolved_name" });
  });
});

describe("aggregateMontanaOutsideSpendingForCandidate", () => {
  const classified = classifyMontanaOutsideSpendingRows({
    sweep: sweep([
      [
        100,
        "Good PAC",
        [
          ieRow({ transId: 1, totalAmtCents: 10_000, cashAmtCents: 10_000 }),
          ieRow({ transId: 2, totalAmtCents: 5_000, cashAmtCents: 5_000, candidateIssue: "Bedey SD43" }),
          ieRow({ transId: 3, totalAmtCents: 7_500, cashAmtCents: 7_500, candidateIssue: "Oppose Kathy Love" }),
        ],
      ],
      [200, "Attack PAC ", [ieRow({ transId: 4, totalAmtCents: 2_000, cashAmtCents: 2_000, candidateIssue: "Oppose David Bedey" })]],
    ]),
    registrationRows: REGISTRATIONS,
    electionYear: 2026,
  });

  it("sums stance totals and groups for one candidate", () => {
    const result = aggregateMontanaOutsideSpendingForCandidate({
      classifiedRows: classified,
      cersCandidateId: 21020,
      sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
    });
    expect(result.supportTotal).toBe(150);
    expect(result.opposeTotal).toBe(20);
    expect(result.attributedRowCount).toBe(3);
    expect(result.outsideGroups).toEqual([
      expect.objectContaining({ committeeId: "100", committeeName: "Good PAC", supportOppose: "support", amount: 150 }),
      expect.objectContaining({ committeeId: "200", committeeName: "Attack PAC", supportOppose: "oppose", amount: 20 }),
    ]);
  });

  it("keeps absent stances null — never a fabricated zero", () => {
    const love = aggregateMontanaOutsideSpendingForCandidate({ classifiedRows: classified, cersCandidateId: 20476 });
    expect(love.supportTotal).toBeNull();
    expect(love.opposeTotal).toBe(75);
    const untargeted = aggregateMontanaOutsideSpendingForCandidate({ classifiedRows: classified, cersCandidateId: 21041 });
    expect(untargeted.supportTotal).toBeNull();
    expect(untargeted.opposeTotal).toBeNull();
    expect(untargeted.outsideGroups).toEqual([]);
  });

  it("summarizes quarantine dollars per committee for the report", () => {
    const withNoise = classifyMontanaOutsideSpendingRows({
      sweep: sweep([
        [
          100,
          "Good PAC",
          [
            ieRow({ transId: 1, totalAmtCents: 10_000, cashAmtCents: 10_000 }),
            ieRow({ transId: 2, totalAmtCents: 99_00, cashAmtCents: 99_00, candidateIssue: "See attached" }),
          ],
        ],
      ]),
      registrationRows: REGISTRATIONS,
      electionYear: 2026,
    });
    expect(summarizeMontanaOutsideSpendingByCommittee(withNoise)).toEqual([
      expect.objectContaining({
        committeeId: 100,
        rowCount: 2,
        totalAmount: 199,
        resolvedRowCount: 1,
        resolvedAmount: 100,
        quarantinedAmountByReason: { attachment_reference: 99 },
      }),
    ]);
  });
});

// Montana IE attachment recovery (docs/plans/montana-finance.md, Phase 3).
//
// Some committees disclose their independent-expenditure targets only in a
// PDF attached to the report, leaving `candidateIssue` as "See attached".
// Those rows quarantine, so the money never publishes. This table carries
// the recovered per-candidate breakdowns, machine-extracted from the filed
// PDFs (never hand-transcribed) and keyed by the transaction they belong to.
//
// Two rules make a recovery publishable, both enforced in the classifier:
//   1. It must RECONCILE — the entries must sum to the transaction amount
//      within MONTANA_IE_RECOVERY_TOLERANCE_CENTS. A breakdown covering a
//      different scope than the row it is attached to is not evidence about
//      that row.
//   2. It carries no new resolution or stance logic: every entry is written
//      as a canonical `candidateIssue` string ("Name (SD-9)", "Oppose Name
//      (HD-4)"), so the ordinary parser, stance rule, and two-stage resolver
//      judge it exactly as if the filer had typed it into the field.
//
// Harvest recipe (committee attachments are a different flow from candidate
// reports): search/committeeSearch -> searchResults/searchCommittees ->
// searchResults/listCommitteeResults -> publicReportList/
// retrieveCommitteeReports {committeeId, commSearchPage} ->
// viewFinanceReport/retrieveReport {committeeId, candidateId: "", reportId}
// -> viewFinanceReport/attachmentList -> viewFinanceReport/viewAttachment
// ?attachmentId=N, which answers JSON whose `fileContent` is the base64 PDF.
//
// Phase 3 survey of the $1.69M of attachment-referenced 2026 money: most
// attachments CANNOT be recovered, and are deliberately absent here — the
// Right to Work committees attach candidate NAME LISTS with no per-candidate
// amounts, the MTGOP addendum gives mail-piece quantities rather than
// dollars, and Conservatives4MT's April allocation ($714,360.24) matches no
// transaction or combination of transactions (nearest is $10,279.98 away).
// Splitting a lump across those would be our inference, not the filer's
// disclosure, so those rows stay quarantined and visible in the report.

export type MontanaIeAttachmentRecoveryEntry = {
  /** Canonical candidateIssue text, parsed by the ordinary target parser. */
  issue: string;
  amountCents: number;
};

export type MontanaIeAttachmentRecovery = {
  committeeId: number;
  reportId: number;
  attachmentId: number;
  fileName: string;
  /** sha256 of the decoded PDF, so a re-filed attachment is detectable. */
  sha256: string;
  retrievedAt: string;
  entries: readonly MontanaIeAttachmentRecoveryEntry[];
};

/** Reconciliation slack: filer spreadsheets round their own line items. */
export const MONTANA_IE_RECOVERY_TOLERANCE_CENTS = 100;

const RECOVERIES: readonly (MontanaIeAttachmentRecovery & { transId: number })[] = [
  {
    // Conservatives4MT, report 79711 (04/26-05/24/2026). The attachment is a
    // full per-row disclosure — date, amount, candidate, office, an explicit
    // Support/Oppose column, and a purpose description — so it publishes
    // both stances. 131 rows aggregate to these 49 candidate-stance
    // entries, summing to $392,751.52 against a $392,751.55 transaction (a
    // 3-cent rounding difference in the filer's own spreadsheet).
    transId: 1990730,
    committeeId: 10641,
    reportId: 79711,
    attachmentId: 4797,
    fileName: "Montana_PAC_Expenditure_Report_may25.pdf",
    sha256: "5c9fd6f435b48c199bdd8d9d4b993f160e0afb24c37bed63901aaad75700552d",
    retrievedAt: "2026-08-29",
    entries: [
      { issue: "David Bedey (SD-43)", amountCents: 2482688 },
      { issue: "Shelley Vance (SD-34)", amountCents: 2124585 },
      { issue: "Llew Jones (SD-9)", amountCents: 1658660 },
      { issue: "Eric Albus (SD-14)", amountCents: 1537881 },
      { issue: "Michele Binkley (HD-85)", amountCents: 1470749 },
      { issue: "Wayne Rusk (HD-86)", amountCents: 1455939 },
      { issue: "Russell Nelson (HD-67)", amountCents: 1449695 },
      { issue: "Brad Barker (HD-55)", amountCents: 1433091 },
      { issue: "Valerie Moore (HD-29)", amountCents: 1408227 },
      { issue: "Chris Rindal (SD-19)", amountCents: 1383636 },
      { issue: "John Fitzpatrick (HD-76)", amountCents: 1355213 },
      { issue: "Linda Reksten (HD-13)", amountCents: 1278751 },
      { issue: "Doug Martens (SD-18)", amountCents: 1269516 },
      { issue: "Mike Talia (SD-42)", amountCents: 1213933 },
      { issue: "Ken Walsh (HD-69)", amountCents: 1210350 },
      { issue: "Susan Geise (HD-17)", amountCents: 1166939 },
      { issue: "George Nikolakakos (SD-12)", amountCents: 1025899 },
      { issue: "Ed Buttrey (SD-11)", amountCents: 934423 },
      { issue: "Oppose Barry Usher (SD-19)", amountCents: 926774 },
      { issue: "Curtis Cochran (HD-90)", amountCents: 914471 },
      { issue: "Neil Durham (SD-1)", amountCents: 782254 },
      { issue: "Oppose Greg Kmetz (SD-18)", amountCents: 735751 },
      { issue: "Oppose Caleb Hinkle (SD-34)", amountCents: 696964 },
      { issue: "Jennifer Carlson (HD-68)", amountCents: 676141 },
      { issue: "Stacy Zinn (HD-52)", amountCents: 603395 },
      { issue: "Roy Caldwell (HD-84)", amountCents: 555769 },
      { issue: "Ole Hedstrom (HD-49)", amountCents: 537911 },
      { issue: "Brandon Ler (HD-33)", amountCents: 505385 },
      { issue: "Eric Peterson (HD-21)", amountCents: 500303 },
      { issue: "Ty Linger (HD-36)", amountCents: 497399 },
      { issue: "Gunner Cesnik (HD-40)", amountCents: 486945 },
      { issue: "Courtenay Sprunger (HD-7)", amountCents: 390097 },
      { issue: "Chisholm Christensen (HD-28)", amountCents: 375112 },
      { issue: "Troy Charbonneau (HD-53)", amountCents: 367052 },
      { issue: "Jason Lorang (HD-22)", amountCents: 363379 },
      { issue: "Oppose Mark Wicks (SD-14)", amountCents: 336510 },
      { issue: "Melissa Nikolakakos (HD-20)", amountCents: 326469 },
      { issue: "Arthur Dunn (HD-6)", amountCents: 323712 },
      { issue: "Derek Peachey (HD-3)", amountCents: 323274 },
      { issue: "Eric Tilleman (HD-23)", amountCents: 320152 },
      { issue: "Lyn Bennett (HD-4)", amountCents: 301349 },
      { issue: "Shelley Vance (HD-34)", amountCents: 300000 },
      { issue: "Ty Linger (HD-33)", amountCents: 250000 },
      { issue: "Oppose Nelly Nicol (HD-53)", amountCents: 197575 },
      { issue: "Oppose James Marshal (HD-84)", amountCents: 193146 },
      { issue: "Aurthor Dunn (HD-6)", amountCents: 169477 },
      { issue: "Chisholm B Christensen (HD-28)", amountCents: 169477 },
      { issue: "Oppose Mike Vinton (HD-40)", amountCents: 158734 },
      { issue: "Oppose Kathy Love (SD-43)", amountCents: 130000 },
    ],
  },
];

const RECOVERY_BY_TRANS_ID = new Map(RECOVERIES.map((recovery) => [recovery.transId, recovery]));

/** The recovered breakdown for a transaction, or null when none is filed. */
export function montanaIeAttachmentRecoveryFor(transId: number): MontanaIeAttachmentRecovery | null {
  return RECOVERY_BY_TRANS_ID.get(transId) ?? null;
}

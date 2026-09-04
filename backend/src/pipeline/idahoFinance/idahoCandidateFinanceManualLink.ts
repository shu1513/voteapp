// Idaho finance manual link (docs/plans/idaho-finance.md, Phase 3): one
// operator-chosen registration for one candidate election the auto-link
// could not resolve ("Eric Myricks" vs grid "Myricks II, William Eric").
// The operator supplies only the registration guid, read off the Sunshine
// profile URL; every other fact — filer name, district label, deep link —
// comes from the grid, through the same office, district, cycle, and Active
// gates the auto-link applies. Only the name gate is bypassed: that is the
// operator's judgment, and it is what the link_source 'manual' row records.
// The writer's manual-link protection shields the row from later automatic
// runs; the due sync picks it up like any other link.

import type { Pool, PoolClient } from "pg";

import {
  IDAHO_CFS_GRID_PAGE_SIZE,
  listIdahoCandidateElectionsMissingFinanceLinks,
} from "./idahoCandidateFinanceAutoLink.js";
import { idahoRegistrationDistrictLabelForRace } from "./idahoCandidateFilerResolver.js";
import {
  getAllIdahoCandidateRegistrations,
  idahoRegistrationProfileUrl,
  normalizeIdahoRegistrationGuid,
  type IdahoCandidateRegistrationRow,
  type IdahoCfsClientOptions,
} from "./idahoCfsClient.js";
import { normalizeIdahoCandidateNameForStorage, upsertIdahoFinanceLink } from "./idahoFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// The auto-link CLI's window: a manual link never reaches a race the due
// list would not sync.
const ELECTION_LOOKBACK_DAYS = 98;
const ELECTION_LOOKAHEAD_DAYS = 730;

export type IdahoFinanceManualLinkInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  registrationGuid: string;
  now: Date;
  /** Validate and report without writing. */
  dryRun?: boolean;
  /** Grid rows when the caller already pulled them. */
  registrations?: readonly IdahoCandidateRegistrationRow[];
  clientOptions?: IdahoCfsClientOptions;
  listCandidateElectionsFn?: typeof listIdahoCandidateElectionsMissingFinanceLinks;
  upsertLinkFn?: typeof upsertIdahoFinanceLink;
};

export type IdahoFinanceManualLinkResult = {
  dryRun: boolean;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  registrationGuid: string;
  filerName: string;
  district: string | null;
  sourceUrl: string;
  /** Grid figure at link time, so the operator sees what the sync will publish. */
  totalRaised: number;
  /** null in dry-run. */
  linkId: string | null;
};

function describeRegistration(row: IdahoCandidateRegistrationRow): string {
  return `${row.filerName}, ${row.office ?? "no office"}, ${row.district ?? "no district"}, ${row.electionYear}, ${row.status}`;
}

export async function linkIdahoCandidateFinanceManually(
  input: IdahoFinanceManualLinkInput
): Promise<IdahoFinanceManualLinkResult> {
  const candidateId = input.candidateId.trim().toLowerCase();
  const electionId = input.electionId.trim().toLowerCase();
  const registrationGuid = normalizeIdahoRegistrationGuid(input.registrationGuid);
  if (Number.isNaN(input.now.getTime())) {
    throw new Error("Invalid Idaho finance manual-link timestamp");
  }

  // Only a candidate election with no active link is linkable here: the
  // auto-link's own list, so eligibility and the window are decided once.
  const listCandidateElections = input.listCandidateElectionsFn ?? listIdahoCandidateElectionsMissingFinanceLinks;
  const unlinked = await listCandidateElections(input.db, {
    now: input.now,
    maxCandidates: null,
    electionLookbackDays: ELECTION_LOOKBACK_DAYS,
    electionLookaheadDays: ELECTION_LOOKAHEAD_DAYS,
  });
  const candidate = unlinked.find(
    (row) => row.candidateId.toLowerCase() === candidateId && row.electionId.toLowerCase() === electionId
  );
  if (candidate === undefined) {
    throw new Error(
      `Candidate election ${candidateId} / ${electionId} is not an unlinked Idaho-finance-eligible race (already linked, ineligible office, or outside the sync window)`
    );
  }
  const candidateName = candidate.candidateNames[0];
  if (candidateName === undefined) {
    throw new Error(`Candidate ${candidateId} has no name`);
  }

  const registrations =
    input.registrations ??
    (await getAllIdahoCandidateRegistrations({ pageSize: IDAHO_CFS_GRID_PAGE_SIZE }, input.clientOptions));
  const registration = registrations.find(
    (row) => normalizeIdahoRegistrationGuid(row.registrationGuid) === registrationGuid
  );
  if (registration === undefined) {
    throw new Error(`Idaho registration ${registrationGuid} is not in the candidate grid`);
  }
  const district = idahoRegistrationDistrictLabelForRace(registration, candidate);
  if (district === undefined) {
    throw new Error(
      `Idaho registration ${registrationGuid} (${describeRegistration(registration)}) is not on ${candidateName}'s ${candidate.officeName} ${candidate.electionYear} race`
    );
  }
  if (registration.status !== "Active") {
    throw new Error(
      `Idaho registration ${registrationGuid} (${describeRegistration(registration)}) is not Active`
    );
  }

  // The name gate is the one this path bypasses, and every opponent in the
  // same race passes the gates above — so a registration already linked to
  // another candidate is refused (Rhode Island's claim check).
  const claim = await input.db.query<{ candidate_id: string; candidate_name_normalized: string }>(
    `
      SELECT candidate_id::text, candidate_name_normalized
      FROM public.id_candidate_finance_links
      WHERE registration_guid = $1
        AND link_status = 'active'
        AND candidate_id <> $2::uuid
      LIMIT 1
    `,
    [registrationGuid, candidate.candidateId]
  );
  const owner = claim.rows[0];
  if (owner !== undefined) {
    throw new Error(
      `Idaho registration ${registrationGuid} (${registration.filerName}) is already linked to another candidate: ${owner.candidate_name_normalized} (${owner.candidate_id})`
    );
  }

  const sourceUrl = idahoRegistrationProfileUrl(registrationGuid);
  let linkId: string | null = null;
  if (input.dryRun !== true) {
    const upsertLink = input.upsertLinkFn ?? upsertIdahoFinanceLink;
    const written = await upsertLink({
      db: input.db,
      link: {
        candidateId: candidate.candidateId,
        electionId: candidate.electionId,
        electionYear: candidate.electionYear,
        candidateNameNormalized: normalizeIdahoCandidateNameForStorage(candidateName),
        officeName: candidate.officeName,
        district,
        registrationGuid,
        filerName: registration.filerName,
        linkStatus: "active",
        linkSource: "manual",
        sourceUrl,
        lastVerifiedAt: input.now,
      },
    });
    linkId = written.linkId;
  }
  return {
    dryRun: input.dryRun === true,
    candidateId: candidate.candidateId,
    electionId: candidate.electionId,
    candidateName,
    electionYear: candidate.electionYear,
    officeName: candidate.officeName,
    registrationGuid,
    filerName: registration.filerName,
    district,
    sourceUrl,
    totalRaised: registration.totalRaised,
    linkId,
  };
}

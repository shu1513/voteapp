import { Pool } from "pg";

import {
  buildCandidateRecordsConfigFromEnv,
  enrichCandidateRecords,
} from "../ai/enrichCandidateRecords.js";
import {
  buildCandidateRecordSourcesRepairConfigFromEnv,
  enrichCandidateRecordSourcesRepair,
} from "../ai/enrichCandidateRecordSourcesRepair.js";
import {
  buildCandidateRecordAreasConfigFromEnv,
  enrichCandidateRecordAreas,
} from "../ai/enrichCandidateRecordAreas.js";
import { parseCandidateRecordDiscoveryPayloadPartial } from "../contracts/candidateRecordDiscoveryPayloadContract.js";
import { verifyHttpUrlReachability } from "../ai/urlReachability.js";
import { getPipelineEnv } from "../config/env.js";
import { loadCandidateElectionOfficeContext } from "../pipeline/candidates/candidateRecordOfficeContext.js";
import {
  loadAllResearchAreas,
  loadAllowedResearchAreasForOfficeId,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";

type CandidateElectionPair = {
  candidateId: string;
  electionId: string;
};

function parseFlagValue(name: string): string | null {
  const prefix = `--${name}=`;
  const token = process.argv.find((part) => part.startsWith(prefix));
  if (!token) {
    return null;
  }
  return token.slice(prefix.length).trim() || null;
}

async function findCandidateElectionPair(pool: Pool): Promise<CandidateElectionPair | null> {
  const result = await pool.query<{
    candidate_id: string;
    election_id: string;
  }>(
    `
      SELECT ce.candidate_id, ce.election_id
      FROM public.candidate_elections ce
      JOIN public.candidates c
        ON c.id = ce.candidate_id
      JOIN public.elections e
        ON e.id = ce.election_id
      WHERE c.deleted_at IS NULL
        AND e.race_type = 'office'
      ORDER BY ce.created_at DESC, ce.id DESC
      LIMIT 1
    `
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
  };
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const candidateIdFlag = parseFlagValue("candidate-id");
    const electionIdFlag = parseFlagValue("election-id");

    const pair =
      candidateIdFlag && electionIdFlag
        ? { candidateId: candidateIdFlag, electionId: electionIdFlag }
        : await findCandidateElectionPair(pool);

    if (!pair) {
      throw new Error("No candidate/election office pair found for live probe");
    }

    const context = await loadCandidateElectionOfficeContext(pool, pair.candidateId, pair.electionId);
    if (!context) {
      throw new Error(
        `Missing office context for candidate_id=${pair.candidateId} election_id=${pair.electionId}`
      );
    }

    const allowedAreas = context.officeId
      ? await loadAllowedResearchAreasForOfficeId(pool, context.officeId)
      : await loadAllResearchAreas(pool);
    const allowedSlugs = [...new Set(allowedAreas.map((row) => row.slug))];
    if (allowedSlugs.length === 0) {
      throw new Error("No allowed research areas for live candidate-record AI probe");
    }

    const discovery = await enrichCandidateRecords(
      {
        candidateDisplayName: context.candidateDisplayName,
        districtName: context.districtName,
        districtType: context.districtType,
        state: context.state,
        electionDate: context.electionDate,
        officialBallotTitle: context.officialBallotTitle,
        electionStage: context.electionStage,
        senateClass: context.senateClass,
        termEndYear: context.termEndYear,
        discoveryContestFamily: context.discoveryContestFamily,
      },
      buildCandidateRecordsConfigFromEnv()
    );

    if (!discovery.ok) {
      throw new Error(`Discovery failed: ${discovery.errorCode} ${discovery.reason}`);
    }

    const baseRecord = discovery.records[0] ?? discovery.droppedRecords[0]?.record;
    if (!baseRecord) {
      throw new Error("Discovery returned no records to drive repair probe");
    }

    const combinedBad = [...discovery.droppedRecords];
    const hasSchemaBad = combinedBad.some((item) => item.failureKind === "schema");
    const hasUrlBad = combinedBad.some((item) => item.failureKind === "source_url");

    if (!hasSchemaBad) {
      combinedBad.push({
        record: {
          description: baseRecord.description,
          source_url: baseRecord.source_url,
          event_date: "",
        },
        reason: "schema invalid row index=synthetic: event_date must be parseable date",
        failureType: "permanent",
        failureKind: "schema",
      });
    }

    if (!hasUrlBad) {
      combinedBad.push({
        record: {
          description: baseRecord.description,
          source_url: "https://example.invalid/not-found",
          event_date: baseRecord.event_date,
        },
        reason: "citation fetch returned status 404 (synthetic)",
        failureType: "permanent",
        failureKind: "source_url",
      });
    }

    const blockedUrls = [
      ...new Set(
        combinedBad
          .filter((item) => item.failureKind === "source_url")
          .map((item) => item.record.source_url)
      ),
    ];

    const repair = await enrichCandidateRecordSourcesRepair(
      {
        candidateDisplayName: context.candidateDisplayName,
        districtName: context.districtName,
        districtType: context.districtType,
        state: context.state,
        electionDate: context.electionDate,
        officialBallotTitle: context.officialBallotTitle,
        electionStage: context.electionStage,
        senateClass: context.senateClass,
        termEndYear: context.termEndYear,
        blockedUrls,
        badRecords: combinedBad.map((item, idx) => ({
          badIndex: idx,
          description: item.record.description,
          sourceUrl: item.record.source_url,
          eventDate: item.record.event_date,
          failureReason: item.reason,
        })),
      },
      buildCandidateRecordSourcesRepairConfigFromEnv()
    );

    if (!repair.ok) {
      throw new Error(`Repair failed: ${repair.errorCode} ${repair.reason}`);
    }

    const repairedRecordCandidates = repair.repairs.map((row) => ({
      description: row.description,
      source_url: row.source_url,
      event_date: row.event_date,
    }));
    const repairedSchemaCheck = parseCandidateRecordDiscoveryPayloadPartial({
      records: repairedRecordCandidates,
    });

    let repairedVerifiedCount = 0;
    let repairedDroppedCount = 0;
    const repairedForAreas: typeof repairedRecordCandidates = [];
    for (const row of repairedRecordCandidates) {
      const verification = await verifyHttpUrlReachability(row.source_url, {
        timeoutMs: 8_000,
        allowStatusCodes: [403],
      });
      if (!verification.ok) {
        repairedDroppedCount += 1;
        continue;
      }
      repairedVerifiedCount += 1;
      repairedForAreas.push({
        ...row,
        source_url: verification.finalUrl,
      });
    }

    const recordsForAreaLabel = [
      ...discovery.records.slice(0, 10),
      ...repairedForAreas.slice(0, 10),
    ];

    const areas = await enrichCandidateRecordAreas(
      {
        candidateDisplayName: context.candidateDisplayName,
        districtName: context.districtName,
        districtType: context.districtType,
        state: context.state,
        electionDate: context.electionDate,
        officialBallotTitle: context.officialBallotTitle,
        electionStage: context.electionStage,
        senateClass: context.senateClass,
        termEndYear: context.termEndYear,
        allowedResearchAreaSlugs: allowedSlugs,
        records: recordsForAreaLabel.map((row) => ({
          description: row.description,
          sourceUrl: row.source_url,
          eventDate: row.event_date,
        })),
      },
      buildCandidateRecordAreasConfigFromEnv()
    );

    if (!areas.ok) {
      throw new Error(`Area-label failed: ${areas.errorCode} ${areas.reason}`);
    }

    const output = {
      type: "candidate_record_live_ai_probe",
      ts: new Date().toISOString(),
      candidate_id: pair.candidateId,
      election_id: pair.electionId,
      office_id: context.officeId,
      discovery: {
        provider: discovery.provider,
        model: discovery.model,
        verified_records: discovery.records.length,
        dropped_records: discovery.droppedRecords.length,
        dropped_schema: discovery.droppedRecords.filter((item) => item.failureKind === "schema").length,
        dropped_source_url: discovery.droppedRecords.filter((item) => item.failureKind === "source_url").length,
      },
      repair: {
        provider: repair.provider,
        model: repair.model,
        bad_input_count: combinedBad.length,
        repaired_rows: repair.repairs.length,
        no_replacement_count: repair.noReplacementIndexes.length,
        repaired_schema_ok:
          repairedSchemaCheck.ok && repairedSchemaCheck.invalid_rows.length === 0,
        repaired_verified_count: repairedVerifiedCount,
        repaired_dropped_count: repairedDroppedCount,
      },
      area_label: {
        provider: areas.provider,
        model: areas.model,
        records_input_count: recordsForAreaLabel.length,
        labels_count: areas.labels.length,
      },
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("candidate record live AI probe failed:", error);
  process.exit(1);
});

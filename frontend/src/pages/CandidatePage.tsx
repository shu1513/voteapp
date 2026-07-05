import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { CandidateDetail, CandidateRecord, ResearchAreaPreference } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { SourceLine } from "../components/SourceLine";
import { FollowButton } from "../components/FollowButton";
import { formatElectionDate } from "../lib/format";
import { useFollows } from "../lib/useFollows";
import { useMyResearchAreas } from "../lib/useMyResearchAreas";
import { UNRANKED_RESEARCH_AREA_RANK } from "../lib/researchAreaScoring";
import { useDocumentTitle } from "../lib/useDocumentTitle";

type RecordView = "by_issue" | "my_issues" | "newest";

type RecordGroup = {
  /** null for the untagged "Other records" pseudo-group. */
  areaId: string | null;
  areaName: string;
  records: CandidateRecord[];
};

// Records grouped by research area (a record with several tags appears under
// each; untagged records fall into "Other records"). Groups key on the
// stable research_area_id — display names are presentation, not identity.
function groupRecords(records: CandidateRecord[]): RecordGroup[] {
  const groups = new Map<string | null, RecordGroup>();
  for (const record of records) {
    const areas = record.research_area_tags.length
      ? record.research_area_tags.map((tag) => ({ areaId: tag.research_area_id, areaName: tag.name }))
      : [{ areaId: null, areaName: "Other records" }];
    for (const area of areas) {
      const group = groups.get(area.areaId) ?? { ...area, records: [] };
      group.records.push(record);
      groups.set(area.areaId, group);
    }
  }
  return [...groups.values()].sort((a, b) =>
    a.areaId === null ? 1 : b.areaId === null ? -1 : a.areaName.localeCompare(b.areaName)
  );
}

// "My issues first": saved-area groups move to the front ordered by the
// user's rank (unranked saved areas after ranked ones), everything else
// keeps the alphabetical order groupRecords produced.
function orderGroupsByPreference(
  groups: RecordGroup[],
  preferences: readonly ResearchAreaPreference[]
): RecordGroup[] {
  const rankByAreaId = new Map(
    preferences.map((preference) => [preference.research_area_id, preference.rank ?? UNRANKED_RESEARCH_AREA_RANK])
  );
  return groups
    .map((group, index) => ({
      group,
      index,
      rank: (group.areaId !== null ? rankByAreaId.get(group.areaId) : undefined) ?? Number.POSITIVE_INFINITY,
    }))
    .sort((a, b) => a.rank - b.rank || a.index - b.index)
    .map(({ group }) => group);
}

export function CandidatePage() {
  const { candidateId } = useParams();
  const { canFollow } = useFollows();
  const { hasSaved, preferences } = useMyResearchAreas();
  const [recordView, setRecordView] = useState<RecordView>("by_issue");

  const detail = useQuery({
    queryKey: ["candidate", candidateId],
    queryFn: () => apiRequest<CandidateDetail>(`/api/candidates/${candidateId}`),
    enabled: Boolean(candidateId),
  });
  useDocumentTitle(detail.data?.candidate.display_name);

  if (detail.isPending) {
    return <LoadingNotice text="Loading candidate…" />;
  }
  if (detail.isError) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-8">
        <ErrorNotice error={detail.error} />
      </div>
    );
  }

  const candidate = detail.data.candidate;
  const baseGroups = groupRecords(candidate.records);
  const recordGroups =
    recordView === "my_issues" ? orderGroupsByPreference(baseGroups, preferences) : baseGroups;

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">{candidate.display_name}</h1>
        {canFollow ? <FollowButton candidateId={candidate.candidate_id} isFollowing={candidate.is_following} /> : null}
      </div>
      <p className="mt-1 text-sm text-ink-soft">
        {candidate.party} · {candidate.state}
        {candidate.current_office ? <> · {candidate.current_office}</> : null}
      </p>
      {candidate.official_website_url ? (
        <p className="mt-1 text-sm">
          <a
            href={candidate.official_website_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-ink underline hover:text-rausch"
          >
            Official website
          </a>
        </p>
      ) : null}
      {candidate.summary ? <p className="mt-3 text-ink">{candidate.summary}</p> : null}

      {recordGroups.length > 0 ? (
        <section className="mt-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">Record</h2>
            <label className="flex items-center gap-2 text-sm text-ink-soft">
              View
              <select
                value={recordView}
                onChange={(event) => setRecordView(event.target.value as RecordView)}
                className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
              >
                <option value="by_issue">By issue</option>
                {hasSaved ? <option value="my_issues">My issues first</option> : null}
                <option value="newest">Newest first</option>
              </select>
            </label>
          </div>
          {recordView === "newest" ? (
            // Flat chronological view; the payload already arrives newest-first.
            <ul className="mt-2 space-y-3">
              {candidate.records.map((record) => (
                <li key={record.id} className="rounded-xl border border-line bg-white p-3">
                  <p className="text-sm text-ink">{record.description}</p>
                  <p className="mt-1 text-xs text-ink-soft">
                    {formatElectionDate(record.event_date)}
                    {record.research_area_tags.length > 0
                      ? ` · ${record.research_area_tags.map((tag) => tag.name).join(", ")}`
                      : ""}
                  </p>
                  <SourceLine url={record.source_url} researchedDate={record.created_at.slice(0, 10)} />
                </li>
              ))}
            </ul>
          ) : (
            recordGroups.map((group) => (
              <div key={group.areaId ?? "other"} className="mt-4">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-ink-soft">{group.areaName}</h3>
                <ul className="mt-2 space-y-3">
                  {group.records.map((record) => (
                    <li key={`${group.areaId ?? "other"}-${record.id}`} className="rounded-xl border border-line bg-white p-3">
                      <p className="text-sm text-ink">{record.description}</p>
                      <p className="mt-1 text-xs text-ink-soft">{formatElectionDate(record.event_date)}</p>
                      <SourceLine url={record.source_url} researchedDate={record.created_at.slice(0, 10)} />
                    </li>
                  ))}
                </ul>
              </div>
            ))
          )}
        </section>
      ) : null}

      {candidate.elections.length > 0 ? (
        <section className="mt-6">
          <h2 className="text-lg font-semibold">Elections</h2>
          <ul className="mt-2 divide-y divide-line rounded-xl border border-line bg-white">
            {candidate.elections.map((election) => (
              <li key={election.candidate_election_id} className="px-3 py-2 text-sm">
                <Link to={`/elections/${election.election_id}`} className="text-ink underline hover:text-rausch">
                  {election.official_ballot_title}
                </Link>{" "}
                <span className="text-ink-soft">
                  · {formatElectionDate(election.election_date)} · {election.district.name}
                  {election.is_incumbent ? " · incumbent" : ""}
                </span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {candidate.last_researched ? (
        <p className="mt-6 text-xs text-ink-soft">
          Profile last researched {formatElectionDate(candidate.last_researched.slice(0, 10))}.
        </p>
      ) : null}
    </div>
  );
}

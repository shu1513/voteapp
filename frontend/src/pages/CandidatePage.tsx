import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { CandidateDetail, CandidateRecord } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { SourceLine } from "../components/SourceLine";
import { formatElectionDate } from "../lib/format";

// Records grouped by research area (a record with several tags appears under
// each; untagged records fall into "Other records").
function groupRecords(records: CandidateRecord[]): Array<{ areaName: string; records: CandidateRecord[] }> {
  const groups = new Map<string, CandidateRecord[]>();
  for (const record of records) {
    const areaNames = record.research_area_tags.length
      ? record.research_area_tags.map((tag) => tag.name)
      : ["Other records"];
    for (const areaName of areaNames) {
      const bucket = groups.get(areaName) ?? [];
      bucket.push(record);
      groups.set(areaName, bucket);
    }
  }
  return [...groups.entries()]
    .sort(([a], [b]) => (a === "Other records" ? 1 : b === "Other records" ? -1 : a.localeCompare(b)))
    .map(([areaName, grouped]) => ({ areaName, records: grouped }));
}

export function CandidatePage() {
  const { candidateId } = useParams();

  const detail = useQuery({
    queryKey: ["candidate", candidateId],
    queryFn: () => apiRequest<CandidateDetail>(`/api/candidates/${candidateId}`),
    enabled: Boolean(candidateId),
  });

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
  const recordGroups = groupRecords(candidate.records);

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <h1 className="text-2xl font-bold">{candidate.display_name}</h1>
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
          <h2 className="text-lg font-semibold">Record</h2>
          {recordGroups.map((group) => (
            <div key={group.areaName} className="mt-4">
              <h3 className="text-sm font-semibold uppercase tracking-wide text-ink-soft">{group.areaName}</h3>
              <ul className="mt-2 space-y-3">
                {group.records.map((record) => (
                  <li key={`${group.areaName}-${record.id}`} className="rounded-xl border border-line bg-white p-3">
                    <p className="text-sm text-ink">{record.description}</p>
                    <p className="mt-1 text-xs text-ink-soft">{formatElectionDate(record.event_date)}</p>
                    <SourceLine url={record.source_url} researchedDate={record.created_at.slice(0, 10)} />
                  </li>
                ))}
              </ul>
            </div>
          ))}
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

import { useId, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { StateVotingResources, StateVotingResourcesResult } from "@voteapp/api-client";
import { DisclosureTrigger } from "./DisclosureTrigger";

/** Display form of a link's destination so voters see where they're headed. */
function linkHost(url: string): string | null {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

function OfficialLink({ url, label }: { url: string; label: string }) {
  const host = linkHost(url);
  return (
    <p className="text-sm">
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className="font-medium text-ink underline underline-offset-2 hover:text-rausch"
      >
        {label}
      </a>
      {host ? <span className="ml-2 text-xs text-ink-soft">{host}</span> : null}
    </p>
  );
}

/**
 * The mail block leads (it's the option a voter can act on from home) and its
 * wording follows the researched request type, because "sign up for mail
 * voting" is wrong in automatic vote-by-mail states — there is nothing to
 * sign up for.
 */
function MailSection({ resources }: { resources: StateVotingResources }) {
  const { mail_ballot_request_url, mail_ballot_request_type, mail_ballot_request_deadline_rule } = resources;
  if (!resources.mail_voting_available || !mail_ballot_request_url) {
    return null;
  }

  const label =
    mail_ballot_request_type === "not_required"
      ? "How vote-by-mail works"
      : mail_ballot_request_type === "online_portal"
        ? "Request your ballot online"
        : mail_ballot_request_type === "form"
          ? "Get the mail-ballot application"
          : "How to request a mail ballot";

  return (
    <div>
      <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Vote by mail</p>
      {mail_ballot_request_type === "not_required" ? (
        <p className="mt-1 text-sm text-ink-soft">
          Every registered {resources.state_name} voter is mailed a ballot automatically — no request needed.
        </p>
      ) : null}
      <div className="mt-1">
        <OfficialLink url={mail_ballot_request_url} label={label} />
      </div>
      {mail_ballot_request_deadline_rule ? (
        <p className="mt-1 text-xs text-ink-soft">{mail_ballot_request_deadline_rule}</p>
      ) : null}
    </div>
  );
}

function StateResourcesSection({ state, showStateName }: { state: string; showStateName: boolean }) {
  const query = useQuery({
    queryKey: ["state-resources", state],
    queryFn: () =>
      apiRequest<StateVotingResourcesResult>(`/api/state-resources?state=${encodeURIComponent(state)}`),
    // Official links change on a research cadence, not per visit.
    staleTime: 60 * 60 * 1000,
  });

  if (query.isPending) {
    return <p className="text-sm text-ink-soft">Loading voting resources…</p>;
  }
  if (query.isError) {
    return <p className="text-sm text-ink-soft">Voting resources for {state} aren't available right now.</p>;
  }

  const resources = query.data.state_resources;
  const hasMail = resources.mail_voting_available && resources.mail_ballot_request_url !== null;

  return (
    <div className="flex flex-col gap-2">
      {showStateName ? (
        <p className="text-sm font-medium text-ink">{resources.state_name}</p>
      ) : null}
      <MailSection resources={resources} />
      {hasMail ? <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">or</p> : null}
      <div>
        <p className="text-xs font-medium uppercase tracking-wide text-ink-soft">Vote in person</p>
        <div className="mt-1">
          <OfficialLink url={resources.polling_place_url} label="Find your polling place" />
        </div>
      </div>
    </div>
  );
}

/**
 * "How to vote in WA" disclosure for the elections list: official state links
 * for voting by mail first, then in person. Inline disclosure like
 * BallotFiltersControl — no portal or outside-click machinery. Resources load
 * lazily on first open; states normally holds one entry (a ballot's districts
 * share a state), but every distinct state gets its own section if not.
 */
export function HowToVoteControl({ states }: { states: string[] }) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const uniqueStates = [...new Set(states)];
  if (uniqueStates.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col items-end gap-2">
      <DisclosureTrigger open={open} panelId={panelId} onClick={() => setOpen(!open)}>
        How to vote{uniqueStates.length === 1 ? ` in ${uniqueStates[0]}` : ""}
      </DisclosureTrigger>
      {open ? (
        <div id={panelId} className="flex w-72 max-w-full flex-col gap-4 rounded-lg border border-line bg-white p-3">
          {uniqueStates.map((state) => (
            <StateResourcesSection key={state} state={state} showStateName={uniqueStates.length > 1} />
          ))}
          <p className="text-xs text-ink-soft">Links go to official state election sites.</p>
        </div>
      ) : null}
    </div>
  );
}

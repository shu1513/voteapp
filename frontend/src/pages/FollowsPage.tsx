import { Link } from "react-router-dom";
import { useMe } from "../lib/useMe";
import { useFollows, useSetFollow } from "../lib/useFollows";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { formatElectionDate } from "../lib/format";
import type { CandidateFollow } from "../api/types";

function NotifyToggle({
  label,
  checked,
  onChange,
  disabled,
}: {
  label: string;
  checked: boolean;
  onChange: (next: boolean) => void;
  disabled: boolean;
}) {
  return (
    <label className="flex cursor-pointer items-center gap-2 text-xs text-ink-soft">
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(event) => onChange(event.target.checked)}
        className="h-3.5 w-3.5 accent-rausch"
      />
      {label}
    </label>
  );
}

function FollowRow({ follow }: { follow: CandidateFollow }) {
  const setFollow = useSetFollow();

  function update(fields: Partial<{ notify_elections: boolean; notify_updates: boolean }>) {
    setFollow.mutate({
      candidate_id: follow.candidate_id,
      following: true,
      notify_elections: fields.notify_elections ?? follow.notify_elections,
      notify_updates: fields.notify_updates ?? follow.notify_updates,
    });
  }

  return (
    <li className="rounded-xl border border-line bg-white p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Link to={`/candidates/${follow.candidate_id}`} className="font-semibold text-ink hover:text-rausch">
            {follow.display_name}
          </Link>
          <p className="text-sm text-ink-soft">
            {follow.party} · {follow.state}
            {follow.current_office ? <> · {follow.current_office}</> : null}
          </p>
        </div>
        <button
          type="button"
          disabled={setFollow.isPending}
          onClick={() => setFollow.mutate({ candidate_id: follow.candidate_id, following: false })}
          className="rounded-lg border border-line bg-white px-3 py-1 text-xs font-semibold text-ink transition hover:border-rausch"
        >
          Unfollow
        </button>
      </div>
      {follow.active_election ? (
        <p className="mt-2 text-sm text-ink-soft">
          On the ballot:{" "}
          <Link to={`/elections/${follow.active_election.election_id}`} className="underline hover:text-ink">
            {follow.active_election.official_ballot_title}
          </Link>{" "}
          · {formatElectionDate(follow.active_election.election_date)}
        </p>
      ) : null}
      {follow.latest_record ? (
        <p className="mt-1 line-clamp-2 text-sm text-ink-soft">Latest: {follow.latest_record.description}</p>
      ) : null}
      <div className="mt-3 flex flex-wrap gap-4">
        <NotifyToggle
          label="Email me about their elections"
          checked={follow.notify_elections}
          disabled={setFollow.isPending}
          onChange={(next) => update({ notify_elections: next })}
        />
        <NotifyToggle
          label="Email me about record updates"
          checked={follow.notify_updates}
          disabled={setFollow.isPending}
          onChange={(next) => update({ notify_updates: next })}
        />
      </div>
      {setFollow.isError ? (
        <div className="mt-2">
          <ErrorNotice error={setFollow.error} />
        </div>
      ) : null}
    </li>
  );
}

export function FollowsPage() {
  const { me, isLoading } = useMe();
  const { follows, isLoading: followsLoading, isError } = useFollows();

  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <div className="mx-auto max-w-md px-4 py-10 text-center">
        <p className="text-ink-soft">Log in to manage the candidates you follow.</p>
        <p className="mt-4">
          <Link
            to="/login"
            className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Log in
          </Link>
        </p>
      </div>
    );
  }
  if (!me.email_verified) {
    return <VerifyPrompt email={me.email} />;
  }

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-2xl font-bold">Candidates you follow</h1>
      <p className="mt-1 text-sm text-ink-soft">
        Followed candidates surface first on your ballot; the toggles control the daily email digest.
      </p>
      {followsLoading ? <LoadingNotice text="Loading follows…" /> : null}
      {isError ? (
        <div className="mt-4">
          <ErrorNotice error={new Error("Could not load follows")} />
        </div>
      ) : null}
      {follows && follows.length === 0 ? (
        <EmptyNotice text="You aren't following anyone yet. Use the Follow button on any candidate page." />
      ) : null}
      {follows && follows.length > 0 ? (
        <ul className="mt-4 space-y-3">
          {follows.map((follow) => (
            <FollowRow key={follow.candidate_id} follow={follow} />
          ))}
        </ul>
      ) : null}
    </div>
  );
}

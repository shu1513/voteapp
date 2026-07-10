import { Link, useLocation, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { BALLOT_SORT_DESCRIPTIONS, PUBLIC_BALLOT_SORTS, type BallotSort, type BallotSummary } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ElectionCard } from "../components/ElectionCard";
import { useMyResearchAreas } from "../lib/useMyResearchAreas";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { formatDistrictType } from "../lib/format";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// Public page: only the sorts the anonymous endpoint can honor. A my_areas
// value (typed into the URL or copied from a signed-in session) falls back to
// vote_power so the subtitle never claims an ordering the backend cannot do.
const SORT_VALUES: readonly string[] = PUBLIC_BALLOT_SORTS.map((option) => option.value);

export function BallotPage() {
  useDocumentTitle("Your ballot");
  // Signed-in verified visitors get their saved areas highlighted even on
  // the public ballot; anonymous visitors get an empty set (no highlights).
  const { savedAreaIds } = useMyResearchAreas();
  // Set by the home page's post-search navigation so the visitor can confirm
  // the geocoder matched the right address. Router state only — the address is
  // personal data and must stay out of the URL; a refresh or shared link
  // simply omits the confirmation line.
  const location = useLocation();
  const matchedAddress =
    typeof (location.state as { matchedAddress?: unknown } | null)?.matchedAddress === "string"
      ? (location.state as { matchedAddress: string }).matchedAddress
      : null;
  const [searchParams, setSearchParams] = useSearchParams();
  const districtIds = (searchParams.get("d") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);
  const rawSort = searchParams.get("sort") ?? "";
  const sort: BallotSort = SORT_VALUES.includes(rawSort) ? (rawSort as BallotSort) : "vote_power";

  const ballot = useQuery({
    queryKey: ["ballot", districtIds.join(","), sort],
    queryFn: () =>
      apiRequest<BallotSummary>(
        `/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}&sort=${sort}`
      ),
    enabled: districtIds.length > 0,
  });

  function onSortChange(nextSort: string) {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        next.set("sort", nextSort);
        return next;
      },
      { replace: true }
    );
  }

  if (districtIds.length === 0) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-10">
        <EmptyNotice text="No districts selected." />
        <p className="text-center">
          <Link to="/" className="text-ink underline hover:text-rausch">
            Start with your address
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">Your ballot</h1>
        <label className="flex items-center gap-2 text-sm text-ink-soft">
          Sort by
          <select
            value={sort}
            onChange={(event) => onSortChange(event.target.value)}
            className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
          >
            {PUBLIC_BALLOT_SORTS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      {matchedAddress ? (
        <p className="mt-1 text-sm text-ink-soft">
          Matched address: <span className="font-medium text-ink">{matchedAddress}</span>{" "}
          <Link to="/?new=1" className="underline hover:text-rausch">
            Not your address?
          </Link>
        </p>
      ) : null}

      {ballot.isPending ? <LoadingNotice text="Loading your elections…" /> : null}
      {ballot.isError ? (
        <div className="mt-4">
          <ErrorNotice error={ballot.error} />
        </div>
      ) : null}

      {ballot.isSuccess ? (
        <>
          <p className="mt-1 text-sm text-ink-soft">
            {ballot.data.elections.length} election{ballot.data.elections.length === 1 ? "" : "s"} across{" "}
            {ballot.data.districts.length} district{ballot.data.districts.length === 1 ? "" : "s"},{" "}
            {BALLOT_SORT_DESCRIPTIONS[sort]}
          </p>
          <details className="mt-2 text-xs text-ink-soft">
            <summary className="cursor-pointer select-none underline">Which districts?</summary>
            <ul className="mt-2 divide-y divide-line rounded-lg border border-line">
              {ballot.data.districts.map((district) => (
                <li key={district.id} className="flex items-center justify-between px-3 py-2">
                  <span className="text-ink">{district.name}</span>
                  <span>{formatDistrictType(district.district_type)}</span>
                </li>
              ))}
            </ul>
          </details>
          <details className="mt-2 text-xs text-ink-soft">
            <summary className="cursor-pointer select-none underline">What do these labels mean?</summary>
            <div className="mt-2 space-y-2 rounded-lg border border-line bg-surface p-3">
              <p>
                <strong className="text-ink">Vote power</strong> estimates how much weight one vote carries in
                an election, based on district population and how decisive the contest is expected to be. It is
                an estimate for comparing elections — it does not measure the value, importance, or likely
                effect of your individual vote.
              </p>
              <p>
                <strong className="text-ink">Competitiveness</strong> labels reflect the margin of past results
                for the same contest and may be outdated after redistricting.
              </p>
              <p>
                Details and limitations:{" "}
                <Link to="/disclaimer" className="underline">
                  Disclaimer
                </Link>
                , section 8.
              </p>
            </div>
          </details>
          {ballot.data.elections.length === 0 ? (
            <EmptyNotice text="No upcoming elections found for these districts yet. Check back — new elections are added as they are announced." />
          ) : (
            <div className="mt-4 space-y-3">
              {ballot.data.elections.map((election) => (
                <ElectionCard key={election.id} election={election} savedAreaIds={savedAreaIds} />
              ))}
            </div>
          )}
        </>
      ) : null}
    </div>
  );
}

export default BallotPage;

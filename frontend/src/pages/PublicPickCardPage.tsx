import { isRouteErrorResponse, Link, useLoaderData, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import { APP_NAME, formatElectionDate } from "@voteapp/api-client";
import type { PickCard } from "@voteapp/api-client";
import { NotFoundNotice } from "../components/NotFoundNotice";
import { RouteError } from "../components/RouteError";
import { loadFromApi } from "../lib/loadFromApi";
import { pageMeta } from "../lib/pageMeta";

// The public face of a shared pick card: /picks/<token>. The token in the
// path is the whole authorization, and loadFromApi never forwards cookies.
// Shows a LIVE view of one voter's picks for one election day, headed by
// the owner's first name — the point of sharing is "these are MY picks",
// and a nameless card reads as anonymous data, not a friend's choices.
// First name is the payload's only identity field (userPickCardShares.ts).

// "Shu's choices for November 3, 2026 elections" — the page h1 and the
// share-card title both. Falls back to the unnamed form when a
// not-yet-redeployed backend omits first_name (deploy skew).
function cardTitle(card: PickCard): string {
  const date = formatElectionDate(card.election_date);
  return card.first_name
    ? `${card.first_name}'s choices for ${date} elections`
    : `Election Picks for ${date}`;
}

export async function loader({ params, request }: LoaderFunctionArgs) {
  // React Router decodes path params; re-encode so a token containing ?, #,
  // or % travels as an opaque path segment instead of re-shaping the URL.
  return loadFromApi<PickCard>(`/api/pick-cards/${encodeURIComponent(params.token ?? "")}`, request);
}

export const meta: MetaFunction<typeof loader> = ({ data, error, location }) => {
  if (!data) {
    const isNotFound = isRouteErrorResponse(error) && error.status === 404;
    return [{ title: isNotFound ? `Not found · ${APP_NAME}` : `Something went wrong · ${APP_NAME}` }];
  }
  const raceCount = data.entries.length;
  return [
    ...pageMeta({
      title: `${cardTitle(data)} · ${APP_NAME}`,
      description: `${raceCount} race${raceCount === 1 ? "" : "s"} picked for the ${formatElectionDate(data.election_date)} election on ${APP_NAME}.`,
      path: location.pathname,
    }),
    // Unguessable capability URLs must stay out of search indexes: the link
    // is shared person-to-person, not published.
    { name: "robots", content: "noindex" },
  ];
};

export function ErrorBoundary() {
  const error = useRouteError();
  if (isRouteErrorResponse(error) && error.status === 404) {
    return <NotFoundNotice subject="Pick card" />;
  }
  return <RouteError />;
}

export function PublicPickCardPage() {
  const card = useLoaderData<typeof loader>();

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <section className="rounded-xl border border-line bg-white p-5">
        <h1 className="text-2xl font-bold text-ink">{cardTitle(card)}</h1>
        <p className="mt-1 text-sm text-ink-soft">Shared from {APP_NAME}.</p>
        {card.entries.length === 0 ? (
          <p className="mt-4 text-sm text-ink-soft">This card has no picks right now.</p>
        ) : (
          <ul className="mt-4 space-y-3">
            {card.entries.map((entry) => (
              <li key={entry.election_id} className="text-sm">
                <Link to={`/elections/${entry.election_id}`} className="font-medium text-ink hover:text-rausch">
                  {entry.official_ballot_title}
                </Link>
                <span className="text-ink-soft"> · {entry.district_name}</span>
                <p className="mt-0.5">
                  {entry.measure_position !== null ? (
                    <span
                      className={
                        entry.measure_position === "yes"
                          ? "font-semibold text-green-900"
                          : "font-semibold text-red-900"
                      }
                    >
                      {entry.measure_position === "yes" ? "Yes" : "No"} on this measure
                    </span>
                  ) : (
                    <span className="font-semibold text-green-900">
                      {entry.picks.map((pick, index) => (
                        <span key={pick.candidate_id}>
                          {index > 0 ? ", " : null}
                          <Link to={`/candidates/${pick.candidate_id}`} className="hover:text-rausch">
                            {pick.display_name}
                          </Link>
                          {pick.candidacy_status === "won" ? (
                            <span className="ml-1 rounded bg-green-700 px-1.5 py-0.5 text-xs font-semibold text-white">
                              Won
                            </span>
                          ) : pick.candidacy_status === "lost" ? (
                            <span className="ml-1 rounded bg-surface px-1.5 py-0.5 text-xs font-medium text-ink-soft">
                              Lost
                            </span>
                          ) : pick.candidacy_status === "withdrawn" ? (
                            // Same flag the owner's private card shows — a
                            // recipient must not read a withdrawn candidate
                            // as still running.
                            <span className="ml-1 text-xs text-ink-soft">(withdrew)</span>
                          ) : null}
                        </span>
                      ))}
                    </span>
                  )}
                </p>
              </li>
            ))}
          </ul>
        )}
      </section>
      {/* The share loop's landing pitch: the reader arrived from a friend's
          card, so the next step is making their own. */}
      <p className="mt-6 text-center text-sm">
        <Link
          to="/"
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Research your own ballot on {APP_NAME}
        </Link>
      </p>
    </div>
  );
}

export default PublicPickCardPage;

import { useEffect } from "react";
import { Link, useSearchParams } from "react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// Shared page for the two email-link token flows. Email links land here
// (AUTH_PUBLIC_BASE_URL + /verify-email or /verify-email-change) and the
// page POSTs the token. Tokens are single-use server-side, so the post is
// modeled as a cached query keyed by token: the QueryClient deduplicates and
// caches it across React 19 StrictMode's simulated remount (a ref guard does
// not survive that — the first mount's mutation state is discarded while the
// ref stays set, leaving the page stuck).

type VerifyTokenPageProps = {
  endpoint: "/api/auth/verify-email" | "/api/auth/verify-email-change";
  title: string;
  successMessage: string;
};

export function VerifyTokenPage({ endpoint, title, successMessage }: VerifyTokenPageProps) {
  useDocumentTitle(title);
  const [searchParams] = useSearchParams();
  const token = searchParams.get("token")?.trim() ?? "";
  const queryClient = useQueryClient();

  const verify = useQuery({
    queryKey: ["verify-token", endpoint, token],
    queryFn: () => apiRequest<{ status: string }>(endpoint, { method: "POST", body: { token } }),
    enabled: token.length > 0,
    retry: false,
    staleTime: Infinity,
    gcTime: Infinity,
    // staleTime only shields the success case; an ERRORED query would
    // re-POST the single-use token on remount, window focus, or reconnect.
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });

  useEffect(() => {
    if (verify.isSuccess) {
      // If a session exists (e.g. user verified while logged in), identity
      // fields like email_verified / email may have changed.
      void queryClient.invalidateQueries({ queryKey: ["me"] });
    }
  }, [verify.isSuccess, queryClient]);

  if (!token) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <h1 className="text-2xl font-bold">Invalid link</h1>
        <p className="mt-3 text-ink-soft">This link is incomplete. Use the most recent email we sent you.</p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-2xl font-bold">{title}</h1>
      {verify.isPending ? <LoadingNotice text="Confirming…" /> : null}
      {verify.isSuccess ? (
        <>
          <p className="mt-3 text-ink-soft">{successMessage}</p>
          <p className="mt-6">
            <Link
              to="/login"
              className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
            >
              Log in
            </Link>
          </p>
        </>
      ) : null}
      {verify.isError ? (
        <div className="mt-4 space-y-3">
          <ErrorNotice error={verify.error} />
          <p className="text-sm text-ink-soft">
            The link may have expired or been replaced by a newer one. Check for the most recent email, or{" "}
            <Link to="/login" className="underline hover:text-ink">
              log in
            </Link>{" "}
            and request a new one.
          </p>
        </div>
      ) : null}
    </div>
  );
}

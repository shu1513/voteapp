import { useState } from "react";
import { Link, useNavigate } from "react-router";
import type { MetaFunction } from "react-router";
import { useMutation, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { APP_NAME, apiRequest } from "@voteapp/api-client";
import type { Me, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import { ErrorNotice } from "../components/Status";
import { purgeAccountScopedQueries } from "@voteapp/api-client";
import { useAdoptPreHydrationValue } from "../lib/preHydrationInput";
import { hasSeenWelcome } from "../lib/welcomeSeen";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// First-login onboarding hook-in: a verified user with no saved research
// areas who hasn't been through the welcome step gets routed there instead
// of the ballot. Any lookup failure falls back to the ballot — login must
// never strand the user on an error because an optional step couldn't be
// checked.
async function postLoginDestination(queryClient: QueryClient): Promise<string> {
  const me = queryClient.getQueryData<Me | null>(["me"]);
  if (!me?.email_verified || hasSeenWelcome(me.email)) {
    return "/me/ballot";
  }
  try {
    // fetchQuery, not a bare request: it seeds the cache the welcome page
    // and settings editor read from.
    const prefs = await queryClient.fetchQuery({
      queryKey: ["me", "research-area-preferences"],
      queryFn: () => apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences"),
      staleTime: 60_000,
    });
    return prefs.preferences.length === 0 ? "/me/welcome" : "/me/ballot";
  } catch {
    return "/me/ballot";
  }
}

export const meta: MetaFunction = () => [{ title: `Log in · ${APP_NAME}` }];

export function LoginPage() {
  useDocumentTitle("Log in");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  // Prerendered page: rescue input typed or autofilled before hydration.
  useAdoptPreHydrationValue("login-email", setEmail);
  useAdoptPreHydrationValue("login-password", setPassword);
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const login = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/login", {
        method: "POST",
        body: { email: email.trim(), password },
      }),
    onSuccess: async () => {
      // A previous session may have ended without a clean logout; cached
      // account data (e.g. ["me","ballot"]) must not bleed into this one.
      purgeAccountScopedQueries(queryClient);
      // Login returns only the session cookie; identity comes from /api/me.
      await queryClient.invalidateQueries({ queryKey: ["me"] });
      navigate(await postLoginDestination(queryClient));
    },
  });

  const canSubmit = email.trim().length > 0 && password.length > 0 && !login.isPending;

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-2xl font-bold">Log in</h1>

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (canSubmit) {
            login.mutate();
          }
        }}
        className="mt-6 space-y-4"
      >
        <div>
          <label htmlFor="login-email" className="block text-sm font-medium text-ink">
            Email
          </label>
          <input
            id="login-email"
            type="email"
            required
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            autoComplete="email"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
        </div>
        <div>
          <label htmlFor="login-password" className="block text-sm font-medium text-ink">
            Password
          </label>
          <input
            id="login-password"
            type="password"
            required
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            autoComplete="current-password"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
        </div>
        <button
          type="submit"
          disabled={!canSubmit}
          className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {login.isPending ? "Logging in…" : "Log in"}
        </button>
      </form>

      {login.isError ? (
        <div className="mt-4">
          <ErrorNotice error={login.error} />
        </div>
      ) : null}

      <div className="mt-6 space-y-1 text-sm text-ink-soft">
        <p>
          <Link to="/forgot-password" className="underline hover:text-ink">
            Forgot your password?
          </Link>
        </p>
        <p>
          New here?{" "}
          <Link to="/register" className="underline hover:text-ink">
            Create an account
          </Link>
        </p>
      </div>
    </div>
  );
}

export default LoginPage;

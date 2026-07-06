import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { ErrorNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function ResetPasswordPage() {
  useDocumentTitle("Choose a new password");
  const [searchParams] = useSearchParams();
  const token = searchParams.get("token")?.trim() ?? "";
  const [password, setPassword] = useState("");

  const reset = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/reset-password", {
        method: "POST",
        body: { token, password },
      }),
  });

  if (!token) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <h1 className="text-2xl font-bold">Invalid link</h1>
        <p className="mt-3 text-ink-soft">
          This password reset link is incomplete. Request a new one from the{" "}
          <Link to="/forgot-password" className="underline hover:text-ink">
            reset page
          </Link>
          .
        </p>
      </div>
    );
  }

  if (reset.isSuccess) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <h1 className="text-2xl font-bold">Password updated</h1>
        <p className="mt-3 text-ink-soft">
          Your password has been changed and you have been logged out everywhere. Log in with the new
          password.
        </p>
        <p className="mt-6">
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

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-2xl font-bold">Choose a new password</h1>

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (password.length > 0 && !reset.isPending) {
            reset.mutate();
          }
        }}
        className="mt-6 space-y-4"
      >
        <div>
          <label htmlFor="reset-password" className="block text-sm font-medium text-ink">
            New password
          </label>
          <input
            id="reset-password"
            type="password"
            required
            minLength={12}
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            autoComplete="new-password"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
          <p className="mt-1 text-xs text-ink-soft">At least 12 characters.</p>
        </div>
        <button
          type="submit"
          disabled={password.length === 0 || reset.isPending}
          className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {reset.isPending ? "Saving…" : "Set new password"}
        </button>
      </form>

      {reset.isError ? (
        <div className="mt-4">
          <ErrorNotice error={reset.error} />
        </div>
      ) : null}
    </div>
  );
}

export default ResetPasswordPage;

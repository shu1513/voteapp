import { useState } from "react";
import { Link } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { ErrorNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function ForgotPasswordPage() {
  useDocumentTitle("Reset your password");
  const [email, setEmail] = useState("");

  const forgot = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/forgot-password", {
        method: "POST",
        body: { email: email.trim() },
      }),
  });

  if (forgot.isSuccess) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <h1 className="text-2xl font-bold">Check your email</h1>
        <p className="mt-3 text-ink-soft">
          If an account exists for <strong className="text-ink">{email.trim()}</strong>, we sent a password
          reset link. It expires in 24 hours.
        </p>
        <p className="mt-6 text-sm">
          <Link to="/login" className="underline hover:text-ink">
            Back to login
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-2xl font-bold">Reset your password</h1>
      <p className="mt-2 text-sm text-ink-soft">Enter your account email and we'll send a reset link.</p>

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (email.trim() && !forgot.isPending) {
            forgot.mutate();
          }
        }}
        className="mt-6 space-y-4"
      >
        <div>
          <label htmlFor="forgot-email" className="block text-sm font-medium text-ink">
            Email
          </label>
          <input
            id="forgot-email"
            type="email"
            required
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            autoComplete="email"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
        </div>
        <button
          type="submit"
          disabled={!email.trim() || forgot.isPending}
          className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {forgot.isPending ? "Sending…" : "Send reset link"}
        </button>
      </form>

      {forgot.isError ? (
        <div className="mt-4">
          <ErrorNotice error={forgot.error} />
        </div>
      ) : null}
    </div>
  );
}

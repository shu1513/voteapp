import { useState } from "react";
import { Link } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { LegalGate } from "../components/LegalGate";
import { ErrorNotice } from "../components/Status";
import { SIGNUP_CHECKBOX_LABEL, TERMS_VERSION } from "../legal/copy";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function RegisterPage() {
  useDocumentTitle("Create your account");
  const [email, setEmail] = useState("");
  const [firstName, setFirstName] = useState("");
  const [password, setPassword] = useState("");
  const [accepted, setAccepted] = useState(false);

  const register = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/register", {
        method: "POST",
        body: {
          email: email.trim(),
          password,
          // Clickwrap record: the backend rejects registration without the
          // current terms version and stores it with a timestamp.
          accepted_terms_version: TERMS_VERSION,
          ...(firstName.trim() ? { first_name: firstName.trim() } : {}),
        },
      }),
  });

  const resend = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/resend-verification", {
        method: "POST",
        body: { email: email.trim() },
      }),
  });

  const canSubmit = accepted && email.trim().length > 0 && password.length > 0 && !register.isPending;

  if (register.isSuccess) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <h1 className="text-2xl font-bold">Check your email</h1>
        <p className="mt-3 text-ink-soft">
          We sent a verification link to <strong className="text-ink">{email.trim()}</strong>. Open it to
          verify your account, then log in.
        </p>
        <div className="mt-6 flex items-center gap-4 text-sm">
          <Link to="/login" className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark">
            Go to login
          </Link>
          <button
            type="button"
            onClick={() => resend.mutate()}
            disabled={resend.isPending}
            className="text-ink-soft underline hover:text-ink disabled:opacity-50"
          >
            {resend.isSuccess ? "Sent again" : "Resend email"}
          </button>
        </div>
        {resend.isError ? (
          <div className="mt-4">
            <ErrorNotice error={resend.error} />
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-2xl font-bold">Create your account</h1>
      <p className="mt-2 text-sm text-ink-soft">
        Save your districts, follow candidates, and get alerts when new elections appear where you live.
      </p>

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (canSubmit) {
            register.mutate();
          }
        }}
        className="mt-6 space-y-4"
      >
        <div>
          <label htmlFor="register-email" className="block text-sm font-medium text-ink">
            Email
          </label>
          <input
            id="register-email"
            type="email"
            required
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            autoComplete="email"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
        </div>
        <div>
          <label htmlFor="register-first-name" className="block text-sm font-medium text-ink">
            First name <span className="font-normal text-ink-soft">(optional)</span>
          </label>
          <input
            id="register-first-name"
            type="text"
            value={firstName}
            onChange={(event) => setFirstName(event.target.value)}
            autoComplete="given-name"
            className="mt-1 w-full rounded-md border border-line px-3 py-3 shadow-sm focus:border-ink focus:outline-none"
          />
        </div>
        <div>
          <label htmlFor="register-password" className="block text-sm font-medium text-ink">
            Password
          </label>
          <input
            id="register-password"
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

        <LegalGate
          inputId="signup-terms"
          label={SIGNUP_CHECKBOX_LABEL}
          checked={accepted}
          onChange={setAccepted}
        />

        <button
          type="submit"
          disabled={!canSubmit}
          className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {register.isPending ? "Creating account…" : "Create account"}
        </button>
      </form>

      {register.isError ? (
        <div className="mt-4">
          <ErrorNotice error={register.error} />
        </div>
      ) : null}

      <p className="mt-6 text-sm text-ink-soft">
        Already have an account?{" "}
        <Link to="/login" className="underline hover:text-ink">
          Log in
        </Link>
      </p>
    </div>
  );
}

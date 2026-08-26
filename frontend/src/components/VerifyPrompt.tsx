import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import { ErrorNotice } from "./Status";

// Interstitial for the unverified state: personalized features 403 until the
// email is verified, and GET /api/me (which never 403s) tells us the address
// to offer a resend for.

export function VerifyPrompt({ email }: { email: string }) {
  const resend = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/resend-verification", {
        method: "POST",
        body: { email },
      }),
  });

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <h1 className="text-title font-bold">Verify your email</h1>
      <p className="mt-3 text-ink-soft">
        We sent a verification link to <strong className="text-ink">{email}</strong>. Personalized features
        unlock once it's confirmed.
      </p>
      <button
        type="button"
        onClick={() => resend.mutate()}
        disabled={resend.isPending || resend.isSuccess}
        className="mt-6 rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
      >
        {resend.isSuccess ? "Email sent" : resend.isPending ? "Sending…" : "Resend verification email"}
      </button>
      {resend.isError ? (
        <div className="mt-4">
          <ErrorNotice error={resend.error} />
        </div>
      ) : null}
    </div>
  );
}

import { useState } from "react";
import { useLocation } from "react-router";
import { RENEWAL_CHECKBOX_LABEL, TERMS_VERSION, useAcceptTerms, useMe } from "@voteapp/api-client";
import { LegalGate } from "./LegalGate";

// Routes the modal must not cover: the checkbox links to these documents,
// and a user has to be able to read what they are agreeing to.
const LEGAL_ROUTES = new Set(["/terms", "/privacy", "/disclaimer"]);

/**
 * Blocking interstitial for signed-in users whose recorded terms acceptance
 * predates the current version. Registration handles first acceptance; this
 * handles re-acceptance after a bump, mirroring the same clickwrap rules
 * (unchecked box, action disabled until checked, adjacent document links).
 */
export function TermsRenewalGate() {
  const { me } = useMe();
  const location = useLocation();
  const acceptTerms = useAcceptTerms();
  const [checked, setChecked] = useState(false);

  if (!me || me.accepted_terms_version === TERMS_VERSION) {
    return null;
  }
  if (LEGAL_ROUTES.has(location.pathname)) {
    return null;
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="terms-renewal-heading"
      className="fixed inset-0 z-30 flex items-center justify-center bg-ink/40 p-4"
    >
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
        <h2 id="terms-renewal-heading" className="text-lg font-bold text-ink">
          Our terms have been updated
        </h2>
        <p className="mt-2 text-sm text-ink-soft">
          To keep using VoteApp, please review and accept the updated Terms of Use, Privacy Policy, and
          Disclaimer.
        </p>
        <div className="mt-4">
          <LegalGate
            label={RENEWAL_CHECKBOX_LABEL}
            checked={checked}
            onChange={setChecked}
            inputId="terms-renewal-checkbox"
          />
        </div>
        {acceptTerms.isError ? (
          <p className="mt-3 text-sm text-red-600" role="alert">
            Something went wrong recording your acceptance. Please try again.
          </p>
        ) : null}
        <button
          type="button"
          disabled={!checked || acceptTerms.isPending}
          onClick={() => acceptTerms.mutate(TERMS_VERSION)}
          className="mt-4 w-full rounded-lg bg-rausch px-4 py-2.5 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:opacity-50"
        >
          {acceptTerms.isPending ? "Saving…" : "Agree and continue"}
        </button>
      </div>
    </div>
  );
}

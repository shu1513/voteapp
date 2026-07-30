import { useState } from "react";
import { Link } from "react-router";
import { useAdoptPreHydrationChecked } from "../lib/preHydrationInput";
import { FullAgreementDialog } from "./FullAgreementDialog";

// Clickwrap checkbox (Meyer v. Uber / Nguyen / Berman requirements):
// unchecked by default, sits directly above the action it gates, visible
// links adjacent to the checkbox, and the caller disables its action button
// until `checked` is true. This component is controlled so pages decide
// what acceptance feeds (per-visit state for anonymous search — never
// persisted, so no future visit starts pre-ticked; the register payload
// for signup).
//
// Pass `fullAgreement` where the label is a summary: it adds a dialog holding
// the complete wording, so the short label never becomes the only thing the
// visitor was shown.

type FullAgreementContent = {
  paragraphs: readonly string[];
  privacyNotice: string;
};

type LegalGateProps = {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  inputId: string;
  fullAgreement?: FullAgreementContent;
};

export function LegalGate({ label, checked, onChange, inputId, fullAgreement }: LegalGateProps) {
  // A click that lands before hydration checks the DOM box without reaching
  // state; adopt it so the gated action unlocks. Still a user action — the
  // clickwrap record is unaffected.
  useAdoptPreHydrationChecked(inputId, onChange);
  const [dialogOpen, setDialogOpen] = useState(false);
  return (
    <div className="rounded-xl border border-line bg-surface p-4 text-sm text-ink">
      <label htmlFor={inputId} className="flex cursor-pointer items-start gap-3">
        <input
          id={inputId}
          type="checkbox"
          checked={checked}
          onChange={(event) => onChange(event.target.checked)}
          className="mt-1 h-4 w-4 shrink-0 accent-rausch"
        />
        <span>{label}</span>
      </label>
      <p className="mt-2 flex flex-wrap items-center gap-x-4 pl-7 font-medium">
        <Link to="/terms" className="text-ink underline hover:text-rausch">
          Terms of Use
        </Link>
        <Link to="/privacy" className="text-ink underline hover:text-rausch">
          Privacy Policy
        </Link>
        <Link to="/disclaimer" className="text-ink underline hover:text-rausch">
          Disclaimer
        </Link>
        {fullAgreement ? (
          <button
            type="button"
            onClick={() => setDialogOpen(true)}
            className="text-ink underline hover:text-rausch"
          >
            Read what you are agreeing to
          </button>
        ) : null}
      </p>
      {fullAgreement ? (
        <FullAgreementDialog
          open={dialogOpen}
          label={label}
          paragraphs={fullAgreement.paragraphs}
          privacyNotice={fullAgreement.privacyNotice}
          onAgree={() => {
            onChange(true);
            setDialogOpen(false);
          }}
          onClose={() => setDialogOpen(false)}
        />
      ) : null}
    </div>
  );
}

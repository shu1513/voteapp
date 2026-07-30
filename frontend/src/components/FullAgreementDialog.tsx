import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from "@headlessui/react";
import { Link } from "react-router";

// The full text behind a summarized clickwrap label. Agreeing from inside the
// dialog counts: the visitor has the complete wording on screen at the moment
// they act, which is the strongest form of the affirmative act the gate needs.
// Closing without agreeing leaves the checkbox exactly as it was — the dialog
// can never tick the box on its own.
//
// Built on Headless UI's Dialog like ReportContentButton: focus trapping,
// Escape, and scroll locking come from the library rather than hand-rolled
// effects.

type FullAgreementDialogProps = {
  open: boolean;
  label: string;
  paragraphs: readonly string[];
  privacyNotice: string;
  onAgree: () => void;
  onClose: () => void;
};

export function FullAgreementDialog({
  open,
  label,
  paragraphs,
  privacyNotice,
  onAgree,
  onClose,
}: FullAgreementDialogProps) {
  return (
    <Dialog open={open} onClose={onClose} className="relative z-40">
      <DialogBackdrop className="fixed inset-0 bg-ink/40" />
      <div className="fixed inset-0 overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center">
          <DialogPanel className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
            <DialogTitle className="text-lg font-bold text-ink">What you are agreeing to</DialogTitle>
            <div className="mt-3 text-sm text-ink">
              <p className="font-medium">{label}</p>
              {paragraphs.map((paragraph) => (
                <p key={paragraph} className="mt-3 text-ink-soft">
                  {paragraph}
                </p>
              ))}
              <p className="mt-3 text-ink-soft">{privacyNotice}</p>
              <p className="mt-4 flex flex-wrap gap-x-4 font-medium">
                <Link to="/terms" className="text-ink underline hover:text-rausch">
                  Terms of Use
                </Link>
                <Link to="/privacy" className="text-ink underline hover:text-rausch">
                  Privacy Policy
                </Link>
                <Link to="/disclaimer" className="text-ink underline hover:text-rausch">
                  Disclaimer
                </Link>
              </p>
            </div>
            <div className="mt-5 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg border border-line px-4 py-2.5 font-semibold text-ink transition hover:bg-surface"
              >
                Close
              </button>
              <button
                type="button"
                onClick={onAgree}
                className="rounded-lg bg-rausch px-5 py-2.5 font-semibold text-white transition hover:bg-rausch-dark"
              >
                I agree
              </button>
            </div>
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
}

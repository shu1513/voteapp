import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from "@headlessui/react";
import { useId } from "react";
import {
  PRE_SEARCH_AGREEMENT_PARAGRAPHS,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
} from "@voteapp/api-client";

// The anonymous clickwrap, deferred to the moment it gates something. The
// landing page carries no checkbox and no legal box: assent is asked for when
// the visitor asks for the service, which is where the clickwrap cases put it
// (Meyer v. Uber). The weak pattern is notice sitting apart from the action —
// Nicosia v. Amazon turned on exactly that.
//
// Requirements this encodes:
// - checkbox starts empty, every single time the dialog opens
// - the action stays disabled until it is ticked
// - all three documents are named in the label and linked next to it
// - the arbitration and class-waiver terms are visible here, not only inside
//   the Terms of Use
// - the button names what it does ("Agree and search"), not "Continue"
// - documents open in a new tab, so reading one does not discard the dialog
//   or the address already typed
// - Cancel, Escape, and the backdrop close without agreeing to anything
//
// Built on Headless UI's Dialog like ReportContentButton: focus trapping,
// Escape, and scroll locking come from the library.

const DOCUMENT_LINKS = [
  { label: "Terms of Use", href: "/terms" },
  { label: "Privacy Policy", href: "/privacy" },
  { label: "Disclaimer", href: "/disclaimer" },
] as const;

type PreSearchTermsDialogProps = {
  open: boolean;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  onAgree: () => void;
  onCancel: () => void;
  pending: boolean;
};

export function PreSearchTermsDialog({
  open,
  checked,
  onCheckedChange,
  onAgree,
  onCancel,
  pending,
}: PreSearchTermsDialogProps) {
  const checkboxId = useId();
  return (
    <Dialog
      open={open}
      // A search in flight must not be interrupted by a stray Escape or
      // backdrop click; the buttons below are disabled for the same reason.
      onClose={pending ? () => undefined : onCancel}
      className="relative z-40"
    >
      <DialogBackdrop className="fixed inset-0 bg-ink/40" />
      <div className="fixed inset-0 overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center">
          <DialogPanel className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
            <DialogTitle className="text-lg font-bold text-ink">Before we search</DialogTitle>
            <div className="mt-3 text-sm text-ink-soft">
              {PRE_SEARCH_AGREEMENT_PARAGRAPHS.map((paragraph) => (
                <p key={paragraph} className="mt-2 first:mt-0">
                  {paragraph}
                </p>
              ))}
              <p className="mt-2">{PRIVACY_NOTICE}</p>
            </div>

            <div className="mt-4 rounded-xl border border-line bg-surface p-4 text-sm text-ink">
              <label htmlFor={checkboxId} className="flex cursor-pointer items-start gap-3">
                <input
                  id={checkboxId}
                  type="checkbox"
                  checked={checked}
                  onChange={(event) => onCheckedChange(event.target.checked)}
                  className="mt-1 h-4 w-4 shrink-0 accent-rausch"
                />
                <span>{PRE_SEARCH_CHECKBOX_LABEL}</span>
              </label>
              <p className="mt-2 flex flex-wrap gap-x-4 pl-7 font-medium">
                {DOCUMENT_LINKS.map((document) => (
                  <a
                    key={document.href}
                    href={document.href}
                    target="_blank"
                    rel="noreferrer"
                    className="text-ink underline hover:text-rausch"
                  >
                    {document.label}
                  </a>
                ))}
              </p>
            </div>

            <div className="mt-5 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
              <button
                type="button"
                onClick={onCancel}
                disabled={pending}
                className="rounded-lg border border-line px-4 py-2.5 font-semibold text-ink transition hover:bg-surface disabled:cursor-not-allowed disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={onAgree}
                disabled={!checked || pending}
                className="rounded-lg bg-rausch px-5 py-2.5 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-rausch"
              >
                {pending ? "Searching…" : "Agree and search"}
              </button>
            </div>
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
}

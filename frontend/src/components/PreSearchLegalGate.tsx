import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from "@headlessui/react";
import { PRE_SEARCH_CHECKBOX_LABEL, PRIVACY_NOTICE } from "@voteapp/api-client";
import { LegalGate } from "./LegalGate";

type PreSearchLegalGateProps = {
  open: boolean;
  checked: boolean;
  onChange: (checked: boolean) => void;
  onCancel: () => void;
  onAgree: () => void;
  pending: boolean;
  error: boolean;
};

export function PreSearchLegalGate({
  open,
  checked,
  onChange,
  onCancel,
  onAgree,
  pending,
  error,
}: PreSearchLegalGateProps) {
  return (
    <Dialog open={open} onClose={pending ? () => undefined : onCancel} className="relative z-40">
      <DialogBackdrop className="fixed inset-0 bg-ink/50" />
      <div className="fixed inset-0 overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center">
          <DialogPanel className="w-full max-w-xl rounded-2xl bg-white p-6 shadow-xl">
            <DialogTitle className="text-xl font-bold text-ink">Review and agree</DialogTitle>
            <p className="mt-2 text-sm text-ink-soft">{PRIVACY_NOTICE}</p>
            <div className="mt-4">
              <LegalGate
                inputId="pre-search-terms"
                label={PRE_SEARCH_CHECKBOX_LABEL}
                checked={checked}
                onChange={onChange}
              />
            </div>
            {error ? (
              <p role="alert" className="mt-3 text-sm text-red-700">
                We could not record your agreement. Please try again.
              </p>
            ) : null}
            <div className="mt-5 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
              <button
                type="button"
                onClick={onCancel}
                disabled={pending}
                className="rounded-lg border border-line px-4 py-2.5 font-semibold text-ink disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={onAgree}
                disabled={!checked || pending}
                className="rounded-lg bg-rausch px-5 py-2.5 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:opacity-50"
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

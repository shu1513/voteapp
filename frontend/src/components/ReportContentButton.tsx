import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from "@headlessui/react";
import { useRef, useState, type FormEvent } from "react";
import { ApiError, apiRequest } from "@voteapp/api-client";
import type { ContentReportEntityType, CreateContentReportResponse } from "@voteapp/api-client";

type ReportContentButtonProps = {
  entityType: ContentReportEntityType;
  entityId: string;
  contextLabel: string;
  reporterEmail?: string | null;
};

function formatError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "Report could not be sent. Try again later.";
}

export function ReportContentButton({ entityType, entityId, contextLabel, reporterEmail }: ReportContentButtonProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [message, setMessage] = useState("");
  const [suggestedSourceUrl, setSuggestedSourceUrl] = useState("");
  const [email, setEmail] = useState(reporterEmail ?? "");
  const [status, setStatus] = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const submissionGeneration = useRef(0);

  function openDialog() {
    submissionGeneration.current += 1;
    setEmail(reporterEmail ?? "");
    setStatus("idle");
    setErrorMessage(null);
    // Preserve unsent draft text when a user closes the dialog accidentally.
    setIsOpen(true);
  }

  function closeDialog() {
    setIsOpen(false);
    if (status === "success") {
      setMessage("");
      setSuggestedSourceUrl("");
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedMessage = message.trim();
    if (!trimmedMessage) {
      setErrorMessage("Tell us what looks wrong.");
      setStatus("error");
      return;
    }

    const generation = ++submissionGeneration.current;
    setStatus("submitting");
    setErrorMessage(null);
    try {
      await apiRequest<CreateContentReportResponse>("/api/content-reports", {
        method: "POST",
        body: {
          entity_type: entityType,
          entity_id: entityId,
          message: trimmedMessage,
          ...(suggestedSourceUrl.trim() ? { suggested_source_url: suggestedSourceUrl.trim() } : {}),
          ...(email.trim() ? { reporter_email: email.trim() } : {}),
        },
      });
      if (submissionGeneration.current !== generation) {
        return;
      }
      setStatus("success");
    } catch (error) {
      if (submissionGeneration.current !== generation) {
        return;
      }
      setErrorMessage(formatError(error));
      setStatus("error");
    }
  }

  return (
    <>
      <button
        type="button"
        onClick={openDialog}
        className="text-xs text-ink-soft underline underline-offset-2 hover:text-ink"
        aria-label={`Report an issue with ${contextLabel}`}
      >
        Report an issue
      </button>
      <Dialog
        open={isOpen}
        onClose={(open) => {
          if (!open) {
            closeDialog();
          }
        }}
        className="relative z-50"
      >
        <DialogBackdrop className="fixed inset-0 bg-ink/30" />
        <div className="fixed inset-0 flex items-center justify-center px-4 py-6">
          <DialogPanel className="w-full max-w-md rounded-2xl border border-line bg-white p-5 shadow-xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <DialogTitle className="text-lg font-semibold text-ink">
                  What's wrong?
                </DialogTitle>
                <p className="mt-1 text-xs text-ink-soft">
                  Reports help us investigate accuracy issues. Don't include sensitive personal information; your message is stored as-is.
                </p>
              </div>
              <button type="button" onClick={closeDialog} className="text-sm text-ink-soft hover:text-ink">
                Close
              </button>
            </div>

            {status === "success" ? (
              <div className="mt-4 rounded-lg bg-green-50 p-3 text-sm text-green-900">
                Report sent. Thank you.
              </div>
            ) : (
              <form onSubmit={onSubmit} className="mt-4 space-y-3">
                <label className="block text-sm font-medium text-ink">
                  Details
                  <textarea
                    value={message}
                    onChange={(event) => setMessage(event.target.value)}
                    autoFocus
                    required
                    maxLength={2000}
                    rows={4}
                    className="mt-1 w-full rounded-lg border border-line px-3 py-2 text-sm text-ink focus:border-ink focus:outline-none"
                  />
                </label>
                <label className="block text-sm font-medium text-ink">
                  Optional source URL
                  <input
                    value={suggestedSourceUrl}
                    onChange={(event) => setSuggestedSourceUrl(event.target.value)}
                    type="url"
                    placeholder="https://example.gov/source"
                    className="mt-1 w-full rounded-lg border border-line px-3 py-2 text-sm text-ink focus:border-ink focus:outline-none"
                  />
                </label>
                <label className="block text-sm font-medium text-ink">
                  Optional email
                  <input
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    type="email"
                    className="mt-1 w-full rounded-lg border border-line px-3 py-2 text-sm text-ink focus:border-ink focus:outline-none"
                  />
                </label>
                {errorMessage ? <p className="text-sm text-rausch-dark">{errorMessage}</p> : null}
                <div className="flex justify-end gap-2">
                  <button
                    type="button"
                    onClick={closeDialog}
                    className="rounded-lg border border-line px-3 py-2 text-sm text-ink-soft hover:text-ink"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={status === "submitting"}
                    className="rounded-lg bg-rausch px-3 py-2 text-sm font-semibold text-white hover:bg-rausch-dark disabled:opacity-60"
                  >
                    {status === "submitting" ? "Sending…" : "Send report"}
                  </button>
                </div>
              </form>
            )}
          </DialogPanel>
        </div>
      </Dialog>
    </>
  );
}

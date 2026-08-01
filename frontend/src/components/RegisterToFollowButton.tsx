import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from "@headlessui/react";
import { useState } from "react";
import { Link } from "react-router";

// Stand-in for FollowButton shown to logged-out visitors: styled like the
// real Follow button, but clicking opens a register prompt instead of
// writing a follow (the follows endpoint is verified-email-gated anyway).

type RegisterToFollowButtonProps = {
  candidateName: string;
  size?: "sm" | "md";
};

export function RegisterToFollowButton({ candidateName, size = "md" }: RegisterToFollowButtonProps) {
  const [isOpen, setIsOpen] = useState(false);
  const base =
    size === "sm"
      ? "rounded-lg px-3 py-1 text-xs font-semibold transition"
      : "rounded-lg px-4 py-2 text-sm font-semibold transition";

  return (
    <>
      <button
        type="button"
        onClick={() => setIsOpen(true)}
        className={`${base} bg-rausch text-white hover:bg-rausch-dark`}
      >
        Follow
      </button>
      <Dialog open={isOpen} onClose={() => setIsOpen(false)} className="relative z-50">
        <DialogBackdrop className="fixed inset-0 bg-ink/30" />
        <div className="fixed inset-0 flex items-center justify-center px-4 py-6">
          <DialogPanel className="w-full max-w-md rounded-2xl border border-line bg-white p-5 shadow-xl">
            <div className="flex items-start justify-between gap-4">
              <DialogTitle className="text-lg font-semibold text-ink">
                Follow {candidateName}
              </DialogTitle>
              <button
                type="button"
                onClick={() => setIsOpen(false)}
                className="text-sm text-ink-soft hover:text-ink"
              >
                Close
              </button>
            </div>
            <p className="mt-2 text-sm text-ink">
              Register for free to get updates about this candidate.
            </p>
            <div className="mt-4 flex items-center justify-end gap-3">
              <Link
                to="/login"
                className="text-sm text-ink-soft underline underline-offset-2 hover:text-ink"
              >
                Log in
              </Link>
              <Link
                to="/register"
                className="rounded-lg bg-rausch px-4 py-2 text-sm font-semibold text-white hover:bg-rausch-dark"
              >
                Register for free
              </Link>
            </div>
          </DialogPanel>
        </div>
      </Dialog>
    </>
  );
}

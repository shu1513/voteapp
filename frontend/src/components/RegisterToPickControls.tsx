import { useState } from "react";
import { RegisterPromptDialog } from "./RegisterPromptDialog";

// Stand-ins for the "my choice" pick controls shown to logged-out visitors:
// styled like the real (unpicked) controls, but clicking opens a register
// prompt instead of writing a choice (the choices endpoint needs a session).
// Mirrors RegisterToFollowButton. Callers apply the same visibility rules as
// the real controls (upcoming election, office race, not withdrawn/lost) so
// the anonymous page shows a control exactly where the signed-in page would.

// One pitch for both shapes: says what a pick IS (a saved plan, not a cast
// vote) and where it lands. Choices are login-gated but not verification-
// gated, so "signing up" is the only hurdle worth naming.
function pickPitch(candidateName: string) {
  return (
    <>
      Save {candidateName} as your planned pick and keep your whole ballot in one place. Signing up is
      free.
    </>
  );
}

type RegisterToPickButtonProps = {
  candidateName: string;
  size?: "sm" | "md";
};

/** Stand-in for CandidatePickButton (election page candidate cards). */
export function RegisterToPickButton({ candidateName, size = "md" }: RegisterToPickButtonProps) {
  const [isOpen, setIsOpen] = useState(false);
  const base =
    size === "sm"
      ? "rounded-lg px-3 py-1 text-xs font-semibold transition"
      : "rounded-lg px-4 py-2 text-sm font-semibold transition";

  return (
    <>
      {/* Every candidate card renders this button, so the accessible name
          carries the candidate — otherwise screen-reader button lists and
          voice control see N identical "Make my pick"s. Visible-label-first
          ("Make my pick: …") keeps the spoken text a prefix of the name
          (WCAG 2.5.3) and echoes the ballot cards' "My pick: {name}" chip. */}
      <button
        type="button"
        aria-label={`Make my pick: ${candidateName}`}
        onClick={() => setIsOpen(true)}
        className={`${base} border border-line bg-white text-ink hover:border-green-700`}
      >
        Make my pick
      </button>
      <RegisterPromptDialog
        open={isOpen}
        onClose={() => setIsOpen(false)}
        title={`Pick ${candidateName}`}
        description={pickPitch(candidateName)}
      />
    </>
  );
}

type RegisterToPickRowProps = {
  candidateName: string;
  /** The election's official ballot title, e.g. "Commissioner of Agriculture". */
  raceName: string;
  /** Pre-formatted election date, e.g. "August 18, 2026". */
  dateLabel: string;
};

/**
 * Stand-in for CandidatePickRow (candidate page): same sentence-shaped row
 * as the real unpicked control, so the anonymous and signed-in pages read
 * identically until the click.
 */
export function RegisterToPickRow({ candidateName, raceName, dateLabel }: RegisterToPickRowProps) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        onClick={() => setIsOpen(true)}
        className="w-full rounded-xl border border-line bg-white p-3 text-left text-sm text-ink transition hover:border-green-700"
      >
        Pick <span className="font-semibold">{candidateName}</span> as my pick for {raceName} ·{" "}
        {dateLabel}
      </button>
      <RegisterPromptDialog
        open={isOpen}
        onClose={() => setIsOpen(false)}
        title={`Pick ${candidateName}`}
        description={pickPitch(candidateName)}
      />
    </>
  );
}

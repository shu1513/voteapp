import { Link } from "react-router";

// The pick gate's state-3 conversion nudge (docs/plans/pick-district-gate.md):
// rendered where the pick controls would sit when the viewer's districts are
// unknown — a shared candidate/election link is the moment to turn a reader
// into a voter with a ballot. One sentence, no dismissal state. Links to the
// home address form; a guest lookup lands on /ballot, which stores the
// district context that unlocks the controls here.
export function AddressNudge() {
  return (
    <p className="rounded-md border border-nudge-line bg-nudge px-3 py-2 text-sm text-ink">
      <Link to="/" className="font-medium text-nudge-deep underline hover:text-ink">
        Enter your address
      </Link>{" "}
      to check if you can vote in this race.
    </p>
  );
}

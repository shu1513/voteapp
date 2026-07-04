import { Link } from "react-router-dom";

// Clickwrap checkbox (Meyer v. Uber / Nguyen / Berman requirements):
// unchecked by default, sits directly above the action it gates, visible
// links adjacent to the checkbox, and the caller disables its action button
// until `checked` is true. This component is controlled so pages decide how
// acceptance persists (localStorage for anonymous search, register payload
// for signup).

type LegalGateProps = {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  inputId: string;
};

export function LegalGate({ label, checked, onChange, inputId }: LegalGateProps) {
  return (
    <div className="rounded-lg border border-line bg-surface p-4 text-sm text-ink">
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
      <p className="mt-2 flex flex-wrap gap-x-4 pl-7 font-medium">
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
  );
}

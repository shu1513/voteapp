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
    <div className="rounded-lg border border-amber-300 bg-amber-50 p-4 text-sm text-gray-800">
      <label htmlFor={inputId} className="flex cursor-pointer items-start gap-3">
        <input
          id={inputId}
          type="checkbox"
          checked={checked}
          onChange={(event) => onChange(event.target.checked)}
          className="mt-1 h-4 w-4 shrink-0"
        />
        <span>{label}</span>
      </label>
      <p className="mt-2 pl-7">
        <Link to="/disclaimer" className="font-medium text-blue-700 underline">
          Read the full Disclaimer
        </Link>
      </p>
    </div>
  );
}

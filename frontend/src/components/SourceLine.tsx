import { formatElectionDate, formatSourceHost } from "../lib/format";

// Per-record provenance line required by the legal copy:
// "Source: [link] · researched [date]".

type SourceLineProps = {
  url: string;
  researchedDate?: string | null;
};

export function SourceLine({ url, researchedDate }: SourceLineProps) {
  return (
    <p className="mt-1 text-xs text-ink-soft">
      Source:{" "}
      <a href={url} target="_blank" rel="noopener noreferrer" className="underline hover:text-ink">
        {formatSourceHost(url)}
      </a>
      {researchedDate ? <> · researched {formatElectionDate(researchedDate)}</> : null}
    </p>
  );
}

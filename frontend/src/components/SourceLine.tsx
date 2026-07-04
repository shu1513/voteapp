import { formatElectionDate, formatSourceHost } from "../lib/format";

// Per-record provenance line required by the legal copy:
// "Source: [link] · researched [date]".

type SourceLineProps = {
  url: string;
  researchedDate?: string | null;
};

export function SourceLine({ url, researchedDate }: SourceLineProps) {
  return (
    <p className="mt-1 text-xs text-gray-500">
      Source:{" "}
      <a href={url} target="_blank" rel="noopener noreferrer" className="underline">
        {formatSourceHost(url)}
      </a>
      {researchedDate ? <> · researched {formatElectionDate(researchedDate)}</> : null}
    </p>
  );
}

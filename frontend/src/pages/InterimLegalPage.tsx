import { Link } from "react-router-dom";

// Honest interim pages for the Terms of Use and Privacy Policy named in the
// clickwrap copy: the final documents are pending attorney review (see
// docs/legal/). Publishing real documents at these routes is a launch
// blocker tracked in plan.md Phase 5. Until then the Disclaimer governs.

type InterimLegalPageProps = {
  title: string;
};

export function InterimLegalPage({ title }: InterimLegalPageProps) {
  return (
    <article className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-2xl font-bold">{title}</h1>
      <p className="mt-4 text-gray-700">
        This document is being finalized and will be published here before the Service launches publicly.
        Until it is published, your use of the Service is governed by the{" "}
        <Link to="/disclaimer" className="text-blue-700 underline">
          AI Research and Election Information Disclaimer
        </Link>
        , including its warranty disclaimers, limitation of liability, and acknowledgment sections.
      </p>
    </article>
  );
}

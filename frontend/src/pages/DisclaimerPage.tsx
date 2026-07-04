import ReactMarkdown from "react-markdown";
// Vite raw import: the repo's versioned legal text is the single source of
// truth; the frontend renders it, never copies it.
import disclaimerMarkdown from "../../../docs/legal/disclaimer.md?raw";

export function DisclaimerPage() {
  return (
    <article className="prose prose-sm mx-auto max-w-3xl px-4 py-8 [&_h1]:text-2xl [&_h1]:font-bold [&_h2]:mt-6 [&_h2]:text-lg [&_h2]:font-semibold [&_p]:mt-2 [&_p]:leading-relaxed">
      <ReactMarkdown>{disclaimerMarkdown}</ReactMarkdown>
    </article>
  );
}

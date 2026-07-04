import ReactMarkdown from "react-markdown";
// Vite raw imports: the repo's versioned legal texts in docs/legal/ are the
// single source of truth; the frontend renders them, never copies them.
import disclaimerMarkdown from "../../../docs/legal/disclaimer.md?raw";
import termsMarkdown from "../../../docs/legal/terms-of-use.md?raw";
import privacyMarkdown from "../../../docs/legal/privacy-policy.md?raw";

const DOCUMENTS = {
  disclaimer: disclaimerMarkdown,
  terms: termsMarkdown,
  privacy: privacyMarkdown,
} as const;

type LegalDocumentPageProps = {
  document: keyof typeof DOCUMENTS;
};

export function LegalDocumentPage({ document }: LegalDocumentPageProps) {
  return (
    <article className="prose prose-sm mx-auto max-w-3xl px-4 py-8 [&_h1]:text-2xl [&_h1]:font-bold [&_h2]:mt-6 [&_h2]:text-lg [&_h2]:font-semibold [&_p]:mt-2 [&_p]:leading-relaxed [&_li]:mt-1 [&_ul]:list-disc [&_ul]:pl-6 [&_a]:text-blue-700 [&_a]:underline">
      <ReactMarkdown>{DOCUMENTS[document]}</ReactMarkdown>
    </article>
  );
}

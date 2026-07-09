// Schema.org JSON-LD rendered inline so it lands in the server HTML that
// non-JS crawlers read (an effect-injected script would only be seen by
// JS-rendering crawlers). Escapes "<" so payload text can never close the
// script tag early.
export function JsonLdScript({ data }: { data: Record<string, unknown> }) {
  const json = JSON.stringify({ "@context": "https://schema.org", ...data }).replace(/</g, "\\u003c");
  return <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: json }} />;
}

// Positioned-text extraction for scanned KPDC PDFs (plan-kansas-finance.md).
//
// KPDC serves every filing — even e-filed reports — as print-then-scan PDFs
// with an OCR text layer. The layer is noisy (stray border digits, broken
// decimal points), so Phase 0 uses it only where an arithmetic cross-check
// can validate the reads. The Houston/Phoenix precedent applies: pdfjs
// getTextContent() items must be grouped into lines by y and ordered by x.

export type KansasPdfLine = { y: number; text: string };
export type KansasPdfPage = { pageNumber: number; lines: KansasPdfLine[] };

/** pdfjs rejects Node Buffers — hand this a plain Uint8Array view. */
export async function extractKansasPdfPages(data: Uint8Array): Promise<KansasPdfPage[]> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const loadingTask = getDocument({ data });
  const pdf = await loadingTask.promise;
  try {
    const pages: KansasPdfPage[] = [];
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      // Rotation detection: scanned filings are often rotated 90 degrees, in
      // which case item transforms carry the rotation ([a,b] ~ [0,±1]) and
      // grouping by y scrambles every visual line. When the dominant writing
      // direction is vertical, visual lines share x instead, and reading
      // order follows the sign of b (verified against live KPDC scans).
      const items: { text: string; a: number; b: number; x: number; y: number }[] = [];
      for (const item of content.items) {
        if (!("str" in item)) continue;
        const text = item.str.replace(/\s+/g, " ").trim();
        if (!text) continue;
        items.push({
          text,
          a: item.transform[0] ?? 1,
          b: item.transform[1] ?? 0,
          x: item.transform[4] ?? 0,
          y: item.transform[5] ?? 0,
        });
      }
      const verticalCount = items.filter((item) => Math.abs(item.b) > Math.abs(item.a)).length;
      const rotated = verticalCount > items.length / 2;
      const upward = rotated && items.filter((item) => item.b > 0).length > verticalCount / 2;

      const groups: { key: number; cells: { text: string; pos: number }[] }[] = [];
      for (const item of items) {
        const key = rotated ? item.x : item.y;
        const pos = rotated ? item.y : item.x;
        let group = groups.find((candidate) => Math.abs(candidate.key - key) < 2);
        if (!group) {
          group = { key, cells: [] };
          groups.push(group);
        }
        group.cells.push({ text: item.text, pos });
      }
      // Upright pages read top-to-bottom (y descending), left-to-right (x
      // ascending). Rotated-with-b>0 pages read x ascending, y ascending.
      groups.sort((a, b) => (rotated ? (upward ? a.key - b.key : b.key - a.key) : b.key - a.key));
      for (const group of groups) {
        group.cells.sort((a, b) => (rotated ? (upward ? a.pos - b.pos : b.pos - a.pos) : a.pos - b.pos));
      }
      pages.push({
        pageNumber,
        lines: groups.map((group) => ({
          y: group.key,
          text: group.cells.map((cell) => cell.text).join(" "),
        })),
      });
    }
    return pages;
  } finally {
    await pdf.destroy();
  }
}

export function kansasPdfFullText(pages: KansasPdfPage[]): string {
  return pages.map((page) => page.lines.map((line) => line.text).join("\n")).join("\n");
}

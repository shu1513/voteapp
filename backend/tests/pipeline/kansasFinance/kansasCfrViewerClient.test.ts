import { describe, expect, it } from "vitest";

import {
  buildKansasCfrUrl,
  collectKansasCfrGridPages,
  createKansasCfrSession,
  type KansasCfrPage,
} from "../../../src/pipeline/kansasFinance/kansasCfrViewerClient.js";
import { parseKansasHiddenFields } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";

const GRID = "grdviewCfrResults";
const RESULTS_URL = buildKansasCfrUrl("cfr_examiner_search_results.aspx");

/**
 * Minimal live-shaped results page: record count, viewstate, grid rows, and
 * (for multi-page grids) the pager row — the current page as a bare
 * `<td><span>N</span></td>` among `Page$M` links, as rendered live.
 */
function gridPageHtml(
  names: readonly string[],
  recordCount: number,
  viewState: string,
  pager?: { current: number; pageCount: number }
): string {
  const rows = names
    .map(
      (name, index) =>
        `<span id="${GRID}_lblDate_${index}">07/27/2026</span>` +
        `<a id="${GRID}_lnkbtnLastName_${index}" href="javascript:__doPostBack(&#39;${GRID}$ctl0${index + 2}$lnkbtnLastName&#39;,&#39;&#39;)">${name}</a>`
    )
    .join("\n");
  const pagerCells = pager
    ? Array.from({ length: pager.pageCount }, (_, i) => i + 1)
        .map((page) =>
          page === pager.current
            ? `<td><span>${page}</span></td>`
            : `<td><a href="javascript:__doPostBack(&#39;${GRID}&#39;,&#39;Page$${page}&#39;)">${page}</a></td>`
        )
        .join("")
    : "";
  const pagerRow = pager ? `<tr><td colspan="5"><table><tr>${pagerCells}</tr></table></td></tr>` : "";
  return `<span id="lblRecordCount">${recordCount}</span>
<input type="hidden" name="__VIEWSTATE" value="${viewState}" />
<input type="hidden" name="__EVENTVALIDATION" value="ev-${viewState}" />
${rows}
${pagerRow}`;
}

function pageFromHtml(html: string): KansasCfrPage {
  return { url: RESULTS_URL, html, hiddenFields: parseKansasHiddenFields(html) };
}

function pagingSession(pagesByArgument: Record<string, string>, log: URLSearchParams[]) {
  return createKansasCfrSession({
    spacingMs: 0,
    sleep: async () => {},
    fetchImpl: async (_url, init) => {
      const fields = new URLSearchParams(init.body ?? "");
      log.push(fields);
      const html = pagesByArgument[fields.get("__EVENTARGUMENT") ?? ""];
      if (html === undefined) return new Response("Runtime Error", { status: 500 });
      return new Response(html, { status: 200 });
    },
  });
}

describe("collectKansasCfrGridPages", () => {
  it("walks the pager sequentially, chaining each page's own hidden fields", async () => {
    const page1 = gridPageHtml(["ALPHA", "BRAVO"], 5, "vs-1", { current: 1, pageCount: 3 });
    const page2 = gridPageHtml(["CHARLIE", "DELTA"], 5, "vs-2", { current: 2, pageCount: 3 });
    const page3 = gridPageHtml(["ECHO"], 5, "vs-3", { current: 3, pageCount: 3 });
    const log: URLSearchParams[] = [];
    const session = pagingSession({ Page$2: page2, Page$3: page3 }, log);

    const collected = await collectKansasCfrGridPages(session, pageFromHtml(page1), GRID);

    expect(collected.recordCount).toBe(5);
    expect(collected.pages.map((entry) => entry.pageNumber)).toEqual([1, 2, 3]);
    expect(collected.pages.map((entry) => entry.rows.length)).toEqual([2, 2, 1]);
    expect(collected.pages[2]!.rows[0]!.name).toBe("ECHO");
    // Two pager postbacks, each addressed to the grid…
    expect(log.map((fields) => fields.get("__EVENTTARGET"))).toEqual([GRID, GRID]);
    expect(log.map((fields) => fields.get("__EVENTARGUMENT"))).toEqual(["Page$2", "Page$3"]);
    // …and each carrying the PREVIOUS page's viewstate (sequential chaining).
    expect(log.map((fields) => fields.get("__VIEWSTATE"))).toEqual(["vs-1", "vs-2"]);
  });

  it("returns the single page as-is when it already holds every record", async () => {
    const page1 = gridPageHtml(["ALPHA", "BRAVO"], 2, "vs-1");
    const log: URLSearchParams[] = [];
    const session = pagingSession({}, log);

    const collected = await collectKansasCfrGridPages(session, pageFromHtml(page1), GRID);

    expect(collected.pages).toHaveLength(1);
    expect(log).toHaveLength(0);
  });

  it("fails closed when a page renders no rows instead of looping", async () => {
    const page1 = gridPageHtml(["ALPHA", "BRAVO"], 5, "vs-1", { current: 1, pageCount: 3 });
    const empty = gridPageHtml([], 5, "vs-2", { current: 2, pageCount: 3 });
    const session = pagingSession({ Page$2: empty }, []);

    await expect(collectKansasCfrGridPages(session, pageFromHtml(page1), GRID)).rejects.toThrow(
      "page 2 rendered no rows"
    );
  });

  it("fails closed when a stale postback re-renders the same page instead of advancing", async () => {
    // A 40-record grid whose Page$2 postback answers page 1 again: without
    // the pager check, two copies of page 1 would pass total === recordCount.
    const names = Array.from({ length: 20 }, (_, i) => `ROW${i}`);
    const page1 = gridPageHtml(names, 40, "vs-1", { current: 1, pageCount: 2 });
    const session = pagingSession({ Page$2: page1 }, []);

    await expect(collectKansasCfrGridPages(session, pageFromHtml(page1), GRID)).rejects.toThrow(
      "pager shows page 1, expected 2"
    );
  });

  it("fails closed when a multi-page grid renders no pager at all", async () => {
    const page1 = gridPageHtml(["ALPHA", "BRAVO"], 5, "vs-1");
    const session = pagingSession({}, []);

    await expect(collectKansasCfrGridPages(session, pageFromHtml(page1), GRID)).rejects.toThrow(
      "pager shows page none, expected 1"
    );
  });

  it("fails closed when the record count span is missing", async () => {
    const page = pageFromHtml('<input type="hidden" name="__VIEWSTATE" value="vs" />');
    const session = pagingSession({}, []);

    await expect(collectKansasCfrGridPages(session, page, GRID)).rejects.toThrow("no lblRecordCount");
  });
});

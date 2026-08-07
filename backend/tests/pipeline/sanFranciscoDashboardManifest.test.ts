import { describe, expect, it } from "vitest";
import {
  buildSanFranciscoContestManifestUrl,
  parseSanFranciscoContestManifest,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";

// Compact excerpt of the real elections/2026-06-02/contests/bos04.md
// frontmatter (captured 2026-08-06), keeping the shapes that matter:
// a wrapped committee name, a committee that supports one candidate while
// opposing another, and an unknown top-level key the parser must tolerate.
const BOS04_FIXTURE = `---
layout: contest_candidate
election: '2026-06-02'
candidate: true
title: Board of Supervisors D04
unknown_future_key:
- something: else
candidates:
- filer_nid: '215112140'
  filer_id: '1485709'
  committee_name: ALAN WONG FOR SUPERVISOR 2026
  candidate_name: ALAN WONG
  funds: 412371.0
  expenses: 410727.79
- filer_nid: '214896050'
  filer_id: '1484806'
  committee_name: NATALIE GEE FOR SUPERVISOR 2026
  candidate_name: NATALIE GEE
  funds: 405160.12
  expenses: 409056.2
ie_candidates:
- candidate_name: ALAN WONG
  filer_id: '1485709'
  committees:
  - position: OPPOSE
    filer_id: '1488862'
    committee_name: AFFORDABLE SF NOW SUPPORTING NATALIE GEE FOR SUPERVISOR 2026,
      SPONSORED BY LABOR ORGANIZATIONS
    funds: 0.0
    expenses: 24753.92
  - position: SUPPORT
    filer_id: '1488188'
    committee_name: GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026
    funds: 323556.0
    expenses: 209221.7
- candidate_name: NATALIE GEE
  filer_id: '1484806'
  committees:
  - position: OPPOSE
    filer_id: '1488188'
    committee_name: GROWSF SUPPORTING ALAN WONG FOR SUPERVISOR 2026
    funds: 0.0
    expenses: 56989.43
contributors:
- - Michael Moritz
  - 250000.0

---
`;

const PARSE_INPUT = {
  electionDate: "2026-06-02",
  contestCode: "bos04",
  sourceUrl:
    "https://raw.githubusercontent.com/sfethics/dashboards-2025/main/elections/2026-06-02/contests/bos04.md",
};

describe("parseSanFranciscoContestManifest", () => {
  it("parses candidates with reconciled totals in cents", () => {
    const manifest = parseSanFranciscoContestManifest({
      ...PARSE_INPUT,
      markdown: BOS04_FIXTURE,
    });
    expect(manifest.title).toBe("Board of Supervisors D04");
    expect(manifest.candidates).toHaveLength(2);
    expect(manifest.candidates[0]).toEqual({
      filerNid: "215112140",
      fppcId: "1485709",
      committeeName: "ALAN WONG FOR SUPERVISOR 2026",
      candidateName: "ALAN WONG",
      fundsCents: 41237100,
      expensesCents: 41072779,
    });
  });

  it("parses outside relations per candidate and direction", () => {
    const manifest = parseSanFranciscoContestManifest({
      ...PARSE_INPUT,
      markdown: BOS04_FIXTURE,
    });
    expect(manifest.outsideRelations).toHaveLength(3);
    // YAML folds the wrapped committee name back onto one line.
    expect(manifest.outsideRelations[0]).toEqual({
      candidateName: "ALAN WONG",
      candidateFppcId: "1485709",
      position: "oppose",
      spenderFppcId: "1488862",
      spenderName:
        "AFFORDABLE SF NOW SUPPORTING NATALIE GEE FOR SUPERVISOR 2026, SPONSORED BY LABOR ORGANIZATIONS",
      amountCents: 2475392,
    });
    // The same committee legitimately supports one candidate and opposes
    // another; direction is per relation, never per committee.
    const growSf = manifest.outsideRelations.filter(
      (relation) => relation.spenderFppcId === "1488188",
    );
    expect(growSf.map((relation) => [relation.candidateName, relation.position]))
      .toEqual([
        ["ALAN WONG", "support"],
        ["NATALIE GEE", "oppose"],
      ]);
  });

  it("keeps an outside relation whose committee id is missing", () => {
    const markdown = BOS04_FIXTURE.replace(
      "  - position: SUPPORT\n    filer_id: '1488188'\n",
      "  - position: SUPPORT\n",
    );
    const manifest = parseSanFranciscoContestManifest({
      ...PARSE_INPUT,
      markdown,
    });
    const relation = manifest.outsideRelations.find(
      (row) => row.position === "support",
    );
    expect(relation?.spenderFppcId).toBeNull();
    expect(relation?.amountCents).toBe(20922170);
  });

  it("tolerates a contest without ie_candidates", () => {
    const markdown = BOS04_FIXTURE.replace(/ie_candidates:[\s\S]*?contributors:/, "contributors:");
    const manifest = parseSanFranciscoContestManifest({
      ...PARSE_INPUT,
      markdown,
    });
    expect(manifest.outsideRelations).toEqual([]);
    expect(manifest.candidates).toHaveLength(2);
  });

  it("rejects a candidate entry missing committee identity", () => {
    const markdown = BOS04_FIXTURE.replace("  filer_id: '1485709'\n", "");
    expect(() =>
      parseSanFranciscoContestManifest({ ...PARSE_INPUT, markdown }),
    ).toThrow(/ALAN WONG is missing committee identity/);
  });

  it("rejects a non-money funds value", () => {
    const markdown = BOS04_FIXTURE.replace("funds: 412371.0", "funds: n/a");
    expect(() =>
      parseSanFranciscoContestManifest({ ...PARSE_INPUT, markdown }),
    ).toThrow(/non-money funds/);
  });

  it("rejects an unknown position", () => {
    const markdown = BOS04_FIXTURE.replace("position: OPPOSE", "position: ASSIST");
    expect(() =>
      parseSanFranciscoContestManifest({ ...PARSE_INPUT, markdown }),
    ).toThrow(/unknown position: assist/);
  });

  it("rejects a file without frontmatter", () => {
    expect(() =>
      parseSanFranciscoContestManifest({
        ...PARSE_INPUT,
        markdown: "# not a manifest",
      }),
    ).toThrow(/no YAML frontmatter/);
  });
});

describe("buildSanFranciscoContestManifestUrl", () => {
  it("builds the raw GitHub URL from defaults", () => {
    expect(
      buildSanFranciscoContestManifestUrl({
        electionDate: "2024-11-05",
        contestCode: "myr",
      }),
    ).toBe(
      "https://raw.githubusercontent.com/sfethics/dashboards-2025/main/elections/2024-11-05/contests/myr.md",
    );
  });

  it("rejects malformed election dates and contest codes", () => {
    expect(() =>
      buildSanFranciscoContestManifestUrl({
        electionDate: "11/05/2024",
        contestCode: "myr",
      }),
    ).toThrow(/Invalid San Francisco dashboard election date/);
    expect(() =>
      buildSanFranciscoContestManifestUrl({
        electionDate: "2024-11-05",
        contestCode: "../secrets",
      }),
    ).toThrow(/Invalid San Francisco dashboard contest code/);
  });
});

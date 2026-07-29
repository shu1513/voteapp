import { describe, expect, it, vi } from "vitest";

import { committeeLabelKey } from "../../src/pipeline/address/financeCommitteeLabels.js";
import {
  checkLabelSourceUrls,
  checkRowsAgainstLiveCommittees,
  parseCommitteeLabelPayload,
} from "../../src/scripts/manualFinanceCommitteeLabels.js";

function validRow() {
  return {
    source: "LOS_ANGELES_CITY_ETHICS",
    committee_id: "1461461",
    cycle: 2026,
    committee_name: "Streets for All Los Angeles PAC",
    label: "Transportation-advocacy PAC focused on bike and bus infrastructure",
    source_urls: ["https://ethics.lacity.org/data/campaigns/"],
  };
}

describe("parseCommitteeLabelPayload", () => {
  it("accepts a valid payload and trims fields", () => {
    const rows = parseCommitteeLabelPayload({
      labels: [{ ...validRow(), label: `  ${validRow().label}  ` }],
    });
    expect(rows).toHaveLength(1);
    expect(rows[0].label).toBe(validRow().label);
  });

  it("rejects a payload without a labels array", () => {
    expect(() => parseCommitteeLabelPayload({})).toThrow(/labels/);
    expect(() => parseCommitteeLabelPayload({ labels: [] })).toThrow(/empty/);
  });

  it("collects every problem across rows in one error", () => {
    const bad = () =>
      parseCommitteeLabelPayload({
        labels: [
          { ...validRow(), source: "NOT_A_SOURCE" },
          { ...validRow(), label: "" },
          { ...validRow(), source_urls: ["ftp://example.gov"] },
          { ...validRow(), label: "line one\nline two" },
        ],
      });
    expect(bad).toThrow(/unknown source "NOT_A_SOURCE"/);
    expect(() =>
      parseCommitteeLabelPayload({ labels: [{ ...validRow(), cycle: "2026" }] })
    ).toThrow(/cycle must be an integer/);
    expect(() =>
      parseCommitteeLabelPayload({ labels: [{ ...validRow(), cycle: 1234 }] })
    ).toThrow(/cycle must be an integer/);
    expect(bad).toThrow(/labels\[1\]: label is required/);
    expect(bad).toThrow(/not a valid http\(s\) URL/);
    expect(bad).toThrow(/must be a single line/);
  });

  it("rejects duplicate (source, committee_id, cycle) rows but allows the same committee across cycles", () => {
    expect(() => parseCommitteeLabelPayload({ labels: [validRow(), validRow()] })).toThrow(/duplicate/);
    expect(parseCommitteeLabelPayload({ labels: [validRow(), { ...validRow(), cycle: 2028 }] })).toHaveLength(2);
  });

  it("rejects labels over the length cap", () => {
    expect(parseCommitteeLabelPayload({ labels: [{ ...validRow(), label: "x".repeat(130) }] })).toHaveLength(1);
    expect(() =>
      parseCommitteeLabelPayload({ labels: [{ ...validRow(), label: "x".repeat(131) }] })
    ).toThrow(/exceeds 130 characters/);
  });

  it("rejects non-string source_urls entries instead of silently dropping them", () => {
    expect(() =>
      parseCommitteeLabelPayload({
        labels: [{ ...validRow(), source_urls: ["https://ethics.lacity.org/data/campaigns/", 123] }],
      })
    ).toThrow(/source_urls entries must all be strings/);
  });
});

describe("checkRowsAgainstLiveCommittees", () => {
  const row = validRow();
  const liveKey = committeeLabelKey(row.source, row.committee_id, row.cycle);

  it("passes when the triple exists and the name matches modulo whitespace/case", () => {
    const live = new Map([[liveKey, { committee_name: "  STREETS FOR ALL  Los Angeles PAC " }]]);
    expect(checkRowsAgainstLiveCommittees([row], live)).toEqual([]);
  });

  it("rejects a triple absent from live finance data, including a cycle mismatch", () => {
    const live = new Map([[liveKey, { committee_name: row.committee_name }]]);
    const wrongId = checkRowsAgainstLiveCommittees([{ ...row, committee_id: "9999999" }], live);
    expect(wrongId).toHaveLength(1);
    expect(wrongId[0].kind).toBe("missing_committee");
    expect(wrongId[0].reason).toMatch(/not in any upcoming election's finance summaries/);
    const wrongCycle = checkRowsAgainstLiveCommittees([{ ...row, cycle: 2028 }], live);
    expect(wrongCycle).toHaveLength(1);
  });

  it("rejects a committee_name that names a different committee, quoting the known name", () => {
    const live = new Map([[liveKey, { committee_name: "Some Other PAC" }]]);
    const issues = checkRowsAgainstLiveCommittees([row], live);
    expect(issues).toHaveLength(1);
    expect(issues[0].kind).toBe("name_mismatch");
    expect(issues[0].reason).toMatch(/does not match the known committee name "Some Other PAC"/);
  });

  it("keeps an already-labeled triple writable after its elections pass, name-checked against the stored snapshot", () => {
    const stored = new Map([[liveKey, { committee_name: row.committee_name }]]);
    expect(checkRowsAgainstLiveCommittees([row], new Map(), stored)).toEqual([]);
    const renamed = checkRowsAgainstLiveCommittees(
      [{ ...row, committee_name: "Different PAC" }],
      new Map(),
      stored
    );
    expect(renamed).toHaveLength(1);
    expect(renamed[0].kind).toBe("name_mismatch");
  });
});

describe("checkLabelSourceUrls", () => {
  it("passes reachable URLs, verifying each unique URL once", async () => {
    const verify = vi.fn().mockResolvedValue({ ok: true });
    const row = validRow();
    const errors = await checkLabelSourceUrls([row, { ...row, cycle: 2028 }], verify);
    expect(errors).toEqual([]);
    expect(verify).toHaveBeenCalledTimes(1);
    expect(verify).toHaveBeenCalledWith(row.source_urls[0], {
      timeoutMs: 8_000,
      allowStatusCodes: [403],
    });
  });

  it("reports permanent failures without retrying them, carrying the URL and failure type", async () => {
    const verify = vi.fn().mockResolvedValue({ ok: false, reason: "status 404" });
    const row = validRow();
    const issues = await checkLabelSourceUrls([row], verify);
    expect(issues).toHaveLength(1);
    expect(issues[0].reason).toMatch(/source URL unreachable \(status 404\)/);
    expect(issues[0].kind).toBe("source_url");
    expect(issues[0].sourceUrl).toBe(row.source_urls[0]);
    expect(issues[0].failureType).toBe("permanent");
    expect(verify).toHaveBeenCalledTimes(1);
  });

  it("retries a transient failure once and passes when the retry succeeds", async () => {
    const verify = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, reason: "request timed out" })
      .mockResolvedValueOnce({ ok: true });
    const errors = await checkLabelSourceUrls([validRow()], verify);
    expect(errors).toEqual([]);
    expect(verify).toHaveBeenCalledTimes(2);
  });
});

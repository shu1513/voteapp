import { describe, expect, it } from "vitest";

import { resolveReviewApproveFailureDebugJson } from "../../src/scripts/injectManualElections.js";

describe("resolveReviewApproveFailureDebugJson", () => {
  it("returns null when review approval is not requested", () => {
    expect(resolveReviewApproveFailureDebugJson({}, false)).toBeNull();
    expect(
      resolveReviewApproveFailureDebugJson({ review_decision: "approve", review_reason: "ok" }, false)
    ).toBeNull();
  });

  it("stages soft_retry_count=1 so the validator's review-approve branch applies", () => {
    const json = resolveReviewApproveFailureDebugJson(
      {
        review_decision: "approve",
        review_reason: "official Alaska ballot title verified against the Division of Elections list",
      },
      true
    );
    expect(json).not.toBeNull();
    const parsed = JSON.parse(json!) as Record<string, unknown>;
    expect(parsed.soft_retry_count).toBe(1);
    expect(parsed.manual_review_approved).toBe(true);
    expect(typeof parsed.manual_approve_at).toBe("string");
    expect(parsed.soft_retry_at).toBeUndefined();
  });

  it("rejects review approval without an approve decision or reason", () => {
    expect(() =>
      resolveReviewApproveFailureDebugJson({ review_reason: "reason without decision" }, true)
    ).toThrow('--review-approve requires the payload to carry review_decision: "approve"');

    expect(() =>
      resolveReviewApproveFailureDebugJson({ review_decision: "approve", review_reason: "   " }, true)
    ).toThrow("--review-approve requires a non-empty payload review_reason");

    expect(() =>
      resolveReviewApproveFailureDebugJson({ review_decision: "reject", review_reason: "no" }, true)
    ).toThrow('--review-approve requires the payload to carry review_decision: "approve"');
  });
});

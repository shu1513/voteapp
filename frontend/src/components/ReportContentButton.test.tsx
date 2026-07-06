import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReportContentButton } from "./ReportContentButton";
import { apiError, stubApiRoutes } from "../test/mockApi";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ReportContentButton", () => {
  it("submits the selected entity and shows success", async () => {
    const fetchMock = stubApiRoutes({
      "/api/content-reports": (_url, init) => {
        expect(JSON.parse(String(init?.body))).toEqual({
          entity_type: "candidate_record",
          entity_id: "record-1",
          message: "The vote summary is outdated.",
          suggested_source_url: "https://example.gov/source",
          reporter_email: "voter@example.com",
        });
        return { status: 201, body: { report: { id: "report-1" } } };
      },
    });

    render(
      <ReportContentButton
        entityType="candidate_record"
        entityId="record-1"
        contextLabel="candidate record"
        reporterEmail="voter@example.com"
      />
    );

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Report an issue with candidate record" }));
    await user.type(screen.getByLabelText("What's wrong?"), "The vote summary is outdated.");
    await user.type(screen.getByLabelText("Optional source URL"), "https://example.gov/source");
    expect(screen.getByLabelText("Optional email")).toHaveValue("voter@example.com");
    await user.click(screen.getByRole("button", { name: "Send report" }));

    expect(await screen.findByText("Report sent. Thank you.")).toBeInTheDocument();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("shows the backend error message", async () => {
    stubApiRoutes({
      "/api/content-reports": apiError(429, "rate_limited", "Too many reports. Try later."),
    });

    render(<ReportContentButton entityType="election" entityId="e-1" contextLabel="election" />);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Report an issue with election" }));
    await user.type(screen.getByLabelText("What's wrong?"), "Wrong date.");
    await user.click(screen.getByRole("button", { name: "Send report" }));

    expect(await screen.findByText("Too many reports. Try later.")).toBeInTheDocument();
  });
});

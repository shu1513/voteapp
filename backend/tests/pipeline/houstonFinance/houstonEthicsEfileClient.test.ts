import { describe, expect, it, vi } from "vitest";
import { listHoustonEthicsEfileReports } from "../../../src/pipeline/houstonFinance/houstonEthicsEfileClient.js";

describe("Houston eFile client", () => {
  it("uses only validated Houston config and maps report URLs", async () => {
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ hostConfigs: { "reporting.cityofhouston.ethicsefile.com": { apiBaseUrl: "https://abc.lambda-url.us-east-1.on.aws/", rptBaseUrl: "https://cityofhouston.ethicsefile.com/public/cf", clients: "cityofhouston" } } }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([{ report_info_ident: "28", filer_ident: "70", filer_name: "Neeloy Azad", filer_type_cd: "COH", report_type_cd: "SEMIJUL", received_dt: "2026/07/11", filed_ts: "2026/07/11", period_start_dt: "2026/01/01", period_end_dt: "2026/06/30", seek_office_descr: "MAYOR" }]), { status: 200 }));
    const reports = await listHoustonEthicsEfileReports({ fetchImpl });
    expect(String(fetchImpl.mock.calls[1]?.[0])).toContain("all_reports?client=cityofhouston");
    expect(reports[0]).toMatchObject({ reportId: "28", officeDescription: "MAYOR", pdfUrl: "https://cityofhouston.ethicsefile.com/public/cf/2026/pdfs/ScrubbedReport_28.PDF" });
  });
});

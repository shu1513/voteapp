import { strToU8, zipSync } from "fflate";
import { describe, expect, it, vi } from "vitest";

import {
  fetchNewYorkCityCfbIndependentSpending,
  fetchNewYorkCityCfbIndependentSpenderFunders,
  parseNewYorkCityCfbIndependentSpendingCsv,
  parseNewYorkCityCfbIndependentSpenderFunderCsv,
  resolveNewYorkCityCfbCandidateElectionCycles,
} from "../../../src/pipeline/newYorkCityFinance/newYorkCityCfbIndependentSpendingClient.js";

const HEADER = "ELECTION,SPENDERID,SPENDER_NAME,COMMUNICATION_ID,CANDID,CANDNAME,ALLOCATION,POSITION";

describe("newYorkCityCfbIndependentSpendingClient", () => {
  it("parses exact target allocations, ignores undetermined rows, and deduplicates identical rows", () => {
    const result = parseNewYorkCityCfbIndependentSpendingCsv({
      electionYear: 2025,
      csv: [
        HEADER,
        '2025,Z1,"Good Government",101,2899,"Mamdani, Zohran K",12.345,Support',
        '2025,Z1,"Good Government",101,2899,"Mamdani, Zohran K",12.345,Support',
        '2025,Z2,"Unknown Group",102,2899,"Mamdani, Zohran K",4,Not Determined',
      ].join("\n"),
    });
    expect(result).toEqual({
      rows: [{
        electionYear: 2025,
        electionCycle: "2025",
        spenderId: "Z1",
        spenderName: "Good Government",
        communicationId: "101",
        candidateId: "2899",
        candidateName: "Mamdani, Zohran K",
        allocation: 12.345,
        supportOppose: "support",
      }],
      rawRowCount: 3,
      malformedRowCount: 0,
      ignoredPositionRowCount: 1,
    });
  });

  it("rejects conflicting duplicate allocations", () => {
    expect(() => parseNewYorkCityCfbIndependentSpendingCsv({
      electionYear: 2025,
      csv: [
        HEADER,
        "2025,Z1,Group,101,2899,Candidate,10,Support",
        "2025,Z1,Group,101,2899,Candidate,11,Support",
      ].join("\n"),
    })).toThrow("conflicting duplicate communication allocation");
  });

  it("posts once, downloads the returned ZIP, and reads its communication CSV", async () => {
    const archive = zipSync({
      "CFB-IE-COMM_202507010000.csv": strToU8(`${HEADER}\n2025,Z1,Group,101,A1,Candidate,25,Oppose\n`),
      "CFB-IE-EXP_202507010000.csv": strToU8("ignored\n"),
    });
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response("..\\Temp\\IndependentSpendersExpenditures\\CFB-IE_202507010000.zip"))
      .mockResolvedValueOnce(new Response(archive, { headers: { "content-type": "application/zip" } }));
    const result = await fetchNewYorkCityCfbIndependentSpending({ electionYear: 2025, fetchImpl });
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ candidateId: "A1", supportOppose: "oppose", allocation: 25 });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(fetchImpl.mock.calls[0]?.[1]).toMatchObject({ method: "POST" });
    expect(String(fetchImpl.mock.calls[1]?.[0])).toContain("/FTMSearch/Temp/IndependentSpendersExpenditures/");
  });

  it("stops reading a chunked spending export as soon as it exceeds the byte limit", async () => {
    const chunk = new Uint8Array(1024 * 1024);
    let chunksEmitted = 0;
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        chunksEmitted += 1;
        controller.enqueue(chunk);
      },
      cancel() {
        cancelled = true;
      },
    });
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response("..\\Temp\\IndependentSpendersExpenditures\\CFB-IE_202507010000.zip"))
      .mockResolvedValueOnce(new Response(body));

    await expect(fetchNewYorkCityCfbIndependentSpending({ electionYear: 2025, fetchImpl }))
      .rejects.toThrow("independent-spending export had invalid size");
    expect(chunksEmitted).toBeLessThanOrEqual(102);
    expect(cancelled).toBe(true);
  });

  it("parses organization funders and signed refunds without retaining address fields", () => {
    const result = parseNewYorkCityCfbIndependentSpenderFunderCsv({
      electionYear: 2025,
      csv: [
        "ELECTION,RECIPID,SCHEDULE,REFNO,NAME,C_CODE,AMNT,STRNAME,CITY",
        "2025,Z1,ICONT,R1,Example LLC,LLC,100,Private Street,New York",
        "2025,Z1,IREF,R2,Example LLC,LLC,-25,Private Street,New York",
        "2025,Z1,IEXPF,R3,Forgiven Vendor,CORP,10,Private Street,New York",
      ].join("\n"),
    });
    expect(result).toEqual({
      rows: [
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "ICONT:R1", funderName: "Example LLC", funderType: "LLC", amount: 100 },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "IREF:R2", funderName: "Example LLC", funderType: "LLC", amount: -25 },
      ],
      rawRowCount: 3,
      ignoredRowCount: 1,
    });
    expect(JSON.stringify(result)).not.toContain("Private Street");
  });

  it("downloads the official independent-spender funder CSV with the independent recipient filter", async () => {
    const csv = "ELECTION,RECIPID,SCHEDULE,REFNO,NAME,C_CODE,AMNT\n2025,Z1,ICONT,R1,Example LLC,LLC,100\n";
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response("..\\Temp\\IndependentSpendersContributions\\CFB_202507010000.csv"))
      .mockResolvedValueOnce(new Response(csv));
    const result = await fetchNewYorkCityCfbIndependentSpenderFunders({ electionYear: 2025, fetchImpl });
    expect(result.rows).toHaveLength(1);
    expect(String(fetchImpl.mock.calls[0]?.[1]?.body)).toContain("RecipientType=ind");
  });

  it("rejects an oversized funder export from Content-Length before consuming its body", async () => {
    let pullCount = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        pullCount += 1;
        controller.enqueue(new Uint8Array([1]));
        controller.close();
      },
    });
    const download = new Response(body, { headers: { "content-length": String(100 * 1024 * 1024 + 1) } });
    await Promise.resolve();
    const pullsBeforeFetch = pullCount;
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response("..\\Temp\\IndependentSpendersContributions\\CFB_202507010000.csv"))
      .mockResolvedValueOnce(download);

    await expect(fetchNewYorkCityCfbIndependentSpenderFunders({ electionYear: 2025, fetchImpl }))
      .rejects.toThrow("independent-spender funder export was too large");
    expect(pullCount).toBe(pullsBeforeFetch);
  });

  it("resolves a candidate to an exact lettered cycle and reports cross-cycle ambiguity", async () => {
    const fetchImpl = vi.fn().mockImplementation(async (urlValue: string | URL | Request) => {
      const url = new URL(String(urlValue));
      if (url.pathname.endsWith("GetElectionCycle")) {
        return new Response(JSON.stringify([{ options: [
          { id: "2020", label: "2020: Ballot Proposals" },
          { id: "2020A", label: "2020: Queens Borough President" },
          { id: "2020B", label: "2020: Council District 37" },
          { id: "2020T", label: "2020: Transition/Inauguration" },
        ] }]));
      }
      const cycle = url.searchParams.get("ec");
      if (cycle === "2020") return new Response(JSON.stringify([{ id: "B2" }]));
      if (cycle === "2020A") return new Response(JSON.stringify([{ id: "A1" }, { id: "B2" }]));
      if (cycle === "2020B") return new Response(JSON.stringify([]));
      return new Response("unexpected", { status: 500 });
    });
    const result = await resolveNewYorkCityCfbCandidateElectionCycles({
      electionYear: 2020,
      candidateIds: new Set(["A1", "B2", "C3"]),
      fetchImpl,
    });
    expect(result.resolved).toEqual(new Map([["A1", "2020A"]]));
    expect(result.ambiguousCandidateIds).toEqual(new Set(["B2"]));
    expect(result.missingCandidateIds).toEqual(new Set(["C3"]));
  });
});

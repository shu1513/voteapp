import { describe, expect, it } from "vitest";
import {
  parseLosAngelesEthicsCandidateTotals,
  parseLosAngelesEthicsElectionIndex,
  parseLosAngelesIndependentSpendingRows,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesCityEthicsClient.js";

const metric = (amount: string): string =>
  `<td align="right"><table><tr><td></td><td align="right" style="border-top: 1px solid #666 !important;">$${amount}</td></tr></table></td>`;
const fixture = `<a name="S276"></a><h4><strong>Mayor:</strong></h4><table><tbody><tr><td><a name="C172"></a><a class="candidatelink"><strong>Karen Bass</strong></a></td><td align="right">05/27/26</td>${metric("3,214,043.01")}${metric("4,114,009.00")}${metric("278,667.52")}${metric("1,257,000.00")}${metric("2,553,259.88")}${metric("14,871.54")}${metric("145,512.00")}${metric("0.00")}</tr><tr class="C172_detail"><td><a href="?elec_seat_cand_id=1509&cmt_per_id=24713">contact</a></td></tr><tr class="C172_detail"><td>1471359 - Re-Elect Karen Bass for Mayor 2026</td></tr></tbody></table><a name="S277"></a>`;

describe("Los Angeles Ethics parsers", () => {
  it("parses election index", () =>
    expect(
      parseLosAngelesEthicsElectionIndex(
        '<select name="election_id"><option value="76">2026 City and LAUSD Elections</option></select>',
      ),
    ).toEqual([
      {
        electionId: "76",
        description: "2026 City and LAUSD Elections",
        electionYear: 2026,
      },
    ]));
  it("keeps independent and membership totals separate", () => {
    const [candidate] = parseLosAngelesEthicsCandidateTotals({
      html: fixture,
      electionId: "76",
      officeName: "Mayor",
    });
    expect(candidate).toMatchObject({
      candidatePersonId: "172",
      electionSeatCandidateId: "1509",
      fppcCommitteeId: "1471359",
      internalCommitteePersonId: "24713",
      totalContributions: 3214043.01,
      outsideSupportTotal: 2553259.88,
      membershipSupportTotal: 145512,
    });
  });
  it("parses and validates independent-spending JSON", () => {
    expect(
      parseLosAngelesIndependentSpendingRows(
        {
          data: [
            {
              ie_id: 9,
              filer_name: "People PAC",
              ind_spender: "People PAC [1491022] <br>Los Angeles",
              cand_fname: "Karen",
              cand_lname: "Bass",
              ofc_desc: "Mayor",
              support_oppose: "Supporting ",
              ie_amt_xl: 125.5,
              form496_document_id: 44,
            },
          ],
        },
        "support",
      )[0],
    ).toMatchObject({
      spenderId: "1491022",
      amount: 125.5,
      supportOppose: "support",
    });
  });
  it("skips a malformed candidate without fabricating totals", () =>
    expect(
      parseLosAngelesEthicsCandidateTotals({
        html: fixture.replace(metric("0.00"), ""),
        electionId: "76",
        officeName: "Mayor",
      }),
    ).toEqual([]));
});

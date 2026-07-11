import type { FinanceBreakdown, FinanceOutsideGroup, FinanceSummary } from "@voteapp/api-client";
import {
  financeSourceLabel,
  firstFinanceSourceUrl,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatSourceHost,
  hasFinanceContent,
  hasOutsideFinanceContent,
} from "@voteapp/api-client";

// Campaign finance disclosure panel shared by the election page (per
// candidate card) and the candidate profile page. Wording is claims-precise:
// these are amounts and categories reported to the disclosing agency, so
// headings say "disclosed" / "reporting", and outside committees are
// "outside groups" (state terminology differs; not every one is a Super PAC).

// "Anything to render" logic lives in the shared package (the mobile card
// uses the same definition); re-exported here for this component's callers.
export { hasFinanceContent };

function MoneyStat({ label, amount }: { label: string; amount: number | null }) {
  if (amount === null) {
    return null;
  }
  return (
    <div>
      <dt className="text-xs text-ink-soft">{label}</dt>
      <dd className="text-sm font-semibold text-ink">{formatMoney(amount)}</dd>
    </div>
  );
}

function AmountRow({ name, right }: { name: string; right: string }) {
  return (
    <li className="flex justify-between gap-3 text-sm">
      <span className="text-ink">{name}</span>
      <span className="shrink-0 text-ink-soft">{right}</span>
    </li>
  );
}

function BreakdownList({ heading, rows }: { heading: string; rows: FinanceBreakdown[] }) {
  if (rows.length === 0) {
    return null;
  }
  return (
    <div className="mt-3">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{heading}</h4>
      <ul className="mt-1 space-y-0.5">
        {rows.map((row) => (
          // contributor_count is deliberately not rendered: state adapters
          // disagree on its meaning (Colorado counts contribution rows, Utah
          // counts distinct contributors, FEC counts itemized receipts), so
          // any single label ("donors", "contributions") would be wrong for
          // some sources. Show it only once the backend guarantees one
          // semantic across every loader.
          <AmountRow key={row.category_name} name={formatFinanceCategory(row.category_name)} right={formatMoney(row.amount)} />
        ))}
      </ul>
    </div>
  );
}

function OutsideColumn({
  direction,
  total,
  groups,
  industries,
}: {
  direction: "support" | "opposition";
  total: number | null;
  groups: FinanceOutsideGroup[];
  industries: FinanceBreakdown[];
}) {
  // An all-empty direction renders nothing rather than a bare header — a
  // race often has disclosed support with no disclosed opposition.
  if (total === null && groups.length === 0 && industries.length === 0) {
    return null;
  }
  const support = direction === "support";
  return (
    <div className={support ? "rounded border border-green-200 bg-green-50 p-2" : "rounded border border-red-200 bg-red-50 p-2"}>
      <p className={support ? "text-sm font-medium text-green-900" : "text-sm font-medium text-red-900"}>
        Reported {direction}
        {total !== null ? `: ${formatMoney(total)}` : ""}
      </p>
      {groups.length > 0 ? (
        <div className="mt-2">
          <h5 className="text-xs font-medium text-ink-soft">Outside groups reporting {direction}</h5>
          <ul className="mt-1 space-y-0.5">
            {groups.map((row) => (
              <AmountRow key={row.committee_id} name={row.committee_name} right={formatMoney(row.amount)} />
            ))}
          </ul>
        </div>
      ) : null}
      {industries.length > 0 ? (
        <div className="mt-2">
          {/* These amounts are contributions INTO the groups (aggregated
              from committee income across the cycle), while the total above
              is candidate-specific expenditure — an industry can have given
              a group more than the group spent on this race. The heading and
              the card's shared note keep the two from being conflated. */}
          <h5 className="text-xs font-medium text-ink-soft">Industries funding groups reporting {direction}</h5>
          <ul className="mt-1 space-y-0.5">
            {industries.map((row) => (
              <AmountRow
                key={row.category_name}
                name={formatFinanceCategory(row.category_name)}
                right={formatMoney(row.amount)}
              />
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

export function FinanceSummaryCard({ summary }: { summary: FinanceSummary }) {
  const direct = summary.direct_campaign;
  const outside = summary.outside_spending;
  const hasMoneyRow =
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null;
  const sourceUrl = firstFinanceSourceUrl(summary);

  return (
    <div className="text-sm">
      {hasMoneyRow ? (
        <dl className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <MoneyStat label="Raised" amount={direct.total_raised} />
          <MoneyStat label="Spent" amount={direct.total_spent} />
          <MoneyStat label="Cash on hand" amount={direct.cash_on_hand} />
          <MoneyStat label="Debts" amount={direct.debts_owed} />
        </dl>
      ) : null}

      <BreakdownList heading="Top disclosed occupations of direct donors" rows={direct.top_occupations} />
      <BreakdownList heading="Industries represented among direct contributions" rows={direct.top_industries} />

      {hasOutsideFinanceContent(summary) ? (
        <div className="mt-3">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Outside spending</h4>
          {outside.top_supporting_industries.length > 0 || outside.top_opposing_industries.length > 0 ? (
            <p className="mt-1 text-xs text-ink-soft">
              Industry amounts are contributions to these groups, not amounts necessarily spent on this
              candidate.
            </p>
          ) : null}
          <div className="mt-1 grid gap-3 sm:grid-cols-2">
            <OutsideColumn
              direction="support"
              total={outside.support_total}
              groups={outside.top_supporting_groups}
              industries={outside.top_supporting_industries}
            />
            <OutsideColumn
              direction="opposition"
              total={outside.oppose_total}
              groups={outside.top_opposing_groups}
              industries={outside.top_opposing_industries}
            />
          </div>
        </div>
      ) : null}

      <p className="mt-3 text-xs text-ink-soft">
        Source: {financeSourceLabel(summary.source)} · {summary.cycle} cycle · synced{" "}
        {formatElectionDate(summary.last_synced_at.slice(0, 10))}
        {sourceUrl ? (
          <>
            {" · "}
            <a href={sourceUrl} target="_blank" rel="noopener noreferrer" className="underline hover:text-ink">
              {formatSourceHost(sourceUrl)}
            </a>
          </>
        ) : null}
      </p>
    </div>
  );
}

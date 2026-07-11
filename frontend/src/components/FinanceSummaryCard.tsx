import type { FinanceBreakdown, FinanceOutsideGroup, FinanceSummary } from "@voteapp/api-client";
import {
  financeSourceLabel,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatSourceHost,
} from "@voteapp/api-client";

// Campaign finance disclosure panel shared by the election page (per
// candidate card) and the candidate profile page. Wording is claims-precise:
// these are amounts and categories reported to the disclosing agency, so
// headings say "disclosed" / "reporting", and outside committees are
// "outside groups" (state terminology differs; not every one is a Super PAC).

/**
 * Whether a summary has anything worth rendering. null money values mean
 * "not reported" and hide; an explicit 0 is a real disclosed amount and
 * counts as content.
 */
export function hasFinanceContent(summary: FinanceSummary | null | undefined): summary is FinanceSummary {
  if (!summary) {
    return false;
  }
  const direct = summary.direct_campaign;
  return (
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null ||
    direct.top_occupations.length > 0 ||
    direct.top_industries.length > 0 ||
    hasOutsideContent(summary)
  );
}

function hasOutsideContent(summary: FinanceSummary): boolean {
  const outside = summary.outside_spending;
  return (
    outside.support_total !== null ||
    outside.oppose_total !== null ||
    outside.top_supporting_groups.length > 0 ||
    outside.top_opposing_groups.length > 0 ||
    outside.top_supporting_industries.length > 0 ||
    outside.top_opposing_industries.length > 0
  );
}

function firstSourceUrl(summary: FinanceSummary): string | null {
  const rows: Array<{ source_url: string | null }> = [
    ...summary.direct_campaign.top_occupations,
    ...summary.direct_campaign.top_industries,
    ...summary.outside_spending.top_supporting_groups,
    ...summary.outside_spending.top_opposing_groups,
    ...summary.outside_spending.top_supporting_industries,
    ...summary.outside_spending.top_opposing_industries,
  ];
  for (const row of rows) {
    if (row.source_url) {
      return row.source_url;
    }
  }
  return null;
}

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
          <AmountRow
            key={row.category_name}
            name={formatFinanceCategory(row.category_name)}
            right={
              row.contributor_count !== null
                ? `${formatMoney(row.amount)} · ${row.contributor_count} donor${row.contributor_count === 1 ? "" : "s"}`
                : formatMoney(row.amount)
            }
          />
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
          <h5 className="text-xs font-medium text-ink-soft">Industries funding outside {direction}</h5>
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
  const sourceUrl = firstSourceUrl(summary);

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

      {hasOutsideContent(summary) ? (
        <div className="mt-3">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Outside spending</h4>
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

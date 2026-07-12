import type { FinanceBreakdown, FinanceOutsideGroup, FinanceSummary } from "@voteapp/api-client";
import {
  financeSourceLabel,
  firstFinanceSourceUrl,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatSourceHost,
  hasOutsideFinanceContent,
} from "@voteapp/api-client";
import { Text, View } from "react-native";
import { openExternalUrl } from "../lib/openExternalUrl";

// Port of the web FinanceSummaryCard (frontend/src/components). Wording is
// claims-precise: these are amounts and categories reported to the
// disclosing agency, so headings say "disclosed" / "reporting", and outside
// committees are "outside groups" (state terminology differs; not every one
// is a Super PAC). "Anything to render" gating (hasFinanceContent) lives in
// the shared package.

function MoneyStat({ label, amount }: { label: string; amount: number | null }) {
  if (amount === null) {
    return null;
  }
  return (
    <View className="w-1/2 pb-3 pr-3">
      <Text className="text-xs text-ink-soft">{label}</Text>
      <Text className="text-sm font-semibold text-ink">{formatMoney(amount)}</Text>
    </View>
  );
}

function AmountRow({ name, right }: { name: string; right: string }) {
  return (
    <View className="flex-row justify-between gap-3">
      <Text className="flex-1 text-sm text-ink">{name}</Text>
      <Text className="shrink-0 text-sm text-ink-soft">{right}</Text>
    </View>
  );
}

function BreakdownList({ heading, rows }: { heading: string; rows: FinanceBreakdown[] }) {
  if (rows.length === 0) {
    return null;
  }
  return (
    <View className="mt-3">
      <Text className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{heading}</Text>
      <View className="mt-1 gap-0.5">
        {rows.map((row) => (
          // contributor_count is deliberately not rendered: state adapters
          // disagree on its meaning, so any single label would be wrong for
          // some sources. Same policy as the web card.
          <AmountRow
            key={row.category_name}
            name={formatFinanceCategory(row.category_name)}
            right={formatMoney(row.amount)}
          />
        ))}
      </View>
    </View>
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
    <View
      className={
        support
          ? "rounded border border-green-200 bg-green-50 p-2"
          : "rounded border border-red-200 bg-red-50 p-2"
      }
    >
      <Text className={support ? "text-sm font-medium text-green-900" : "text-sm font-medium text-red-900"}>
        Reported {direction}
        {total !== null ? `: ${formatMoney(total)}` : ""}
      </Text>
      {groups.length > 0 ? (
        <View className="mt-2">
          <Text className="text-xs font-medium text-ink-soft">Outside groups reporting {direction}</Text>
          <View className="mt-1 gap-0.5">
            {groups.map((row) => (
              <AmountRow key={row.committee_id} name={row.committee_name} right={formatMoney(row.amount)} />
            ))}
          </View>
        </View>
      ) : null}
      {industries.length > 0 ? (
        <View className="mt-2">
          {/* These amounts are contributions INTO the groups, while the
              total above is candidate-specific expenditure — the heading and
              the card's shared note keep the two from being conflated. */}
          <Text className="text-xs font-medium text-ink-soft">
            Industries funding groups reporting {direction}
          </Text>
          <View className="mt-1 gap-0.5">
            {industries.map((row) => (
              <AmountRow
                key={row.category_name}
                name={formatFinanceCategory(row.category_name)}
                right={formatMoney(row.amount)}
              />
            ))}
          </View>
        </View>
      ) : null}
    </View>
  );
}

export function FinanceSummaryCard({ summary }: { summary: FinanceSummary }) {
  const direct = summary.direct_campaign;
  const outside = summary.outside_spending;
  const hasMoneyRow =
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null ||
    direct.public_funds_received != null;
  const sourceUrl = firstFinanceSourceUrl(summary);

  return (
    <View>
      {hasMoneyRow ? (
        <View className="flex-row flex-wrap">
          <MoneyStat label="Raised" amount={direct.total_raised} />
          <MoneyStat label="Spent" amount={direct.total_spent} />
          <MoneyStat label="Cash on hand" amount={direct.cash_on_hand} />
          <MoneyStat label="Debts" amount={direct.debts_owed} />
          <MoneyStat label="Public funds" amount={direct.public_funds_received ?? null} />
        </View>
      ) : null}

      <BreakdownList heading="Top disclosed occupations of direct donors" rows={direct.top_occupations} />
      <BreakdownList heading="Top disclosed employers of direct donors" rows={direct.top_employers ?? []} />
      <BreakdownList heading="Industries represented among direct contributions" rows={direct.top_industries} />
      <BreakdownList heading="Direct contributions by size" rows={direct.contribution_size_buckets ?? []} />

      {hasOutsideFinanceContent(summary) ? (
        <View className="mt-3">
          <Text className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Outside spending</Text>
          {outside.top_supporting_industries.length > 0 || outside.top_opposing_industries.length > 0 ? (
            <Text className="mt-1 text-xs text-ink-soft">
              Industry amounts are contributions to these groups, not amounts necessarily spent on this
              candidate.
            </Text>
          ) : null}
          <View className="mt-1 gap-3">
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
          </View>
        </View>
      ) : null}

      <Text className="mt-3 text-xs text-ink-soft">
        Source: {financeSourceLabel(summary.source)} · {summary.cycle} cycle · synced{" "}
        {formatElectionDate(summary.last_synced_at.slice(0, 10))}
        {sourceUrl ? (
          <>
            {" · "}
            <Text className="underline" accessibilityRole="link" onPress={() => openExternalUrl(sourceUrl)}>
              {formatSourceHost(sourceUrl)}
            </Text>
          </>
        ) : null}
      </Text>
    </View>
  );
}

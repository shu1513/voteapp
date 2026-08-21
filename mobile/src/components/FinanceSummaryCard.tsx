import type {
  FinanceBreakdown,
  FinanceOutsideGroup,
  FinanceOutsideIndustrySupport,
  FinanceSummary,
  FinanceUnallocatedOutsideEdge,
} from "@voteapp/api-client";
import {
  financeSourceLabel,
  firstFinanceSourceUrl,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatOutsideEvidenceLines,
  formatSourceHost,
  hasOutsideDirectionContent,
  hasOutsideFinanceContent,
  sortContributionSizeBuckets,
  spendingExceedsCycleFunds,
} from "@voteapp/api-client";
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { openExternalUrl } from "../lib/openExternalUrl";

// Port of the web FinanceSummaryCard (frontend/src/components). Wording is
// claims-precise: these are amounts and categories reported to the
// disclosing agency, so headings say "disclosed" / "reporting", and outside
// committees are "outside groups" (state terminology differs; not every one
// is a Super PAC). "Anything to render" gating (hasFinanceContent) lives in
// the shared package.
//
// Same brief-by-default policy as the web card: top occupations collapse
// past the first few, size buckets sort largest-first, outside spending gets
// a plain-language explanation, and employers / direct-donor industries are
// deliberately not rendered.

// How many occupation rows show before the rest collapse behind "Show more".
const VISIBLE_OCCUPATIONS = 4;

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

function BreakdownRows({ rows }: { rows: FinanceBreakdown[] }) {
  return (
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
  );
}

function BreakdownList({
  heading,
  rows,
  visibleCount,
}: {
  heading: string;
  rows: FinanceBreakdown[];
  /** When set, rows beyond this count collapse behind a "Show more" toggle. */
  visibleCount?: number;
}) {
  const [expanded, setExpanded] = useState(false);
  if (rows.length === 0) {
    return null;
  }
  const hiddenCount = visibleCount !== undefined ? Math.max(rows.length - visibleCount, 0) : 0;
  const visible = hiddenCount > 0 && !expanded ? rows.slice(0, visibleCount) : rows;
  return (
    <View className="mt-3">
      <Text className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{heading}</Text>
      <BreakdownRows rows={visible} />
      {hiddenCount > 0 ? (
        <Pressable
          onPress={() => setExpanded((value) => !value)}
          accessibilityRole="button"
          accessibilityState={{ expanded }}
        >
          <Text className="mt-1 text-xs text-ink-soft underline">
            {expanded ? "Show fewer" : `Show ${hiddenCount} more`}
          </Text>
        </Pressable>
      ) : null}
    </View>
  );
}

/**
 * One outside-spending industry with, when the backend supplies evidence,
 * a plain-language line naming the organizations behind it.
 */
function OutsideIndustryRow({ industry }: { industry: FinanceBreakdown | FinanceOutsideIndustrySupport }) {
  const organizations = "supporting_organizations" in industry ? industry.supporting_organizations : [];
  // One line per receiving committee; "employer" evidence reads as money from
  // that employer's contributors, never as the company donating (the shared
  // helper owns the distinction).
  const evidenceLines = formatOutsideEvidenceLines(organizations);
  return (
    <View>
      <View className="flex-row justify-between gap-3">
        <Text className="flex-1 text-sm text-ink">{formatFinanceCategory(industry.category_name)}</Text>
        <Text className="shrink-0 text-sm text-ink-soft">{formatMoney(industry.amount)}</Text>
      </View>
      {evidenceLines.map((line) => (
        <Text key={line} className="text-xs text-ink-soft">
          {line}
        </Text>
      ))}
    </View>
  );
}

function OutsideSection({
  direction,
  total,
  groups,
  industries,
}: {
  direction: "support" | "opposition";
  total: number | null;
  groups: FinanceOutsideGroup[];
  industries: (FinanceBreakdown | FinanceOutsideIndustrySupport)[];
}) {
  const [groupsExpanded, setGroupsExpanded] = useState(false);
  // An empty direction renders nothing rather than a bare header — a race
  // often has disclosed support with no disclosed opposition. A disclosed
  // $0 total counts as empty (the shared helper's rule): "$0 opposing this
  // candidate" is noise, not information.
  if (!hasOutsideDirectionContent(total, groups, industries)) {
    return null;
  }
  const shownTotal = total !== null && total > 0 ? total : null;
  // Deliberately neutral styling (no green/red fill): the ballot-measure
  // "A YES/NO vote means" boxes own that palette, and this section must not
  // read as a voting recommendation.
  return (
    <View className="mt-2 rounded border border-line p-2">
      <Text className="text-sm font-medium text-ink">
        Reported {direction}
        {shownTotal !== null ? `: ${formatMoney(shownTotal)}` : ""}
      </Text>
      {industries.length > 0 ? (
        <View className="mt-2">
          {/* These amounts are contributions INTO the groups, while the
              total above is candidate-specific expenditure — the heading and
              the section's shared note keep the two from being conflated. */}
          <Text className="text-xs font-medium text-ink-soft">
            Industries funding groups reporting {direction}
          </Text>
          <View className="mt-1 gap-1">
            {industries.map((row) => (
              <OutsideIndustryRow key={row.category_name} industry={row} />
            ))}
          </View>
        </View>
      ) : null}
      {groups.length > 0 ? (
        <View className="mt-2">
          <Pressable
            onPress={() => setGroupsExpanded((value) => !value)}
            accessibilityRole="button"
            accessibilityState={{ expanded: groupsExpanded }}
          >
            <Text className="text-xs text-ink-soft underline">
              Outside groups reporting {direction} ({groups.length})
            </Text>
          </Pressable>
          {groupsExpanded ? (
            <View className="mt-1 gap-0.5">
              {groups.map((row) => (
                <AmountRow key={row.committee_id} name={row.committee_name} right={formatMoney(row.amount)} />
              ))}
            </View>
          ) : null}
        </View>
      ) : null}
    </View>
  );
}

function UnallocatedOutsideEdges({ edges }: { edges: FinanceUnallocatedOutsideEdge[] }) {
  if (edges.length === 0) {
    return null;
  }
  return (
    <View className="mt-2 rounded border border-line p-2">
      <Text className="text-sm font-medium text-ink">
        Outside spending reported without a candidate amount
      </Text>
      <Text className="mt-1 text-xs text-ink-soft">
        These filings identify this candidate as supported or opposed, but do not report how much
        of the filing&apos;s spending applies to this candidate.
      </Text>
      <View className="mt-2 gap-2">
        {edges.map((edge) => {
          const reportDate = formatElectionDate(edge.report_date);
          return (
            <View key={`${edge.filing_id}:${edge.committee_id}:${edge.support_oppose}`}>
              <Text className="text-sm font-medium text-ink">{edge.committee_name}</Text>
              <Text className="text-xs text-ink-soft">
                Reported as {edge.support_oppose === "support" ? "supporting" : "opposing"} this
                candidate · {reportDate} ·{" "}
                <Text
                  className="underline"
                  accessibilityRole="link"
                  accessibilityLabel={`View filing for ${edge.committee_name} dated ${reportDate}`}
                  onPress={() => openExternalUrl(edge.source_url)}
                >
                  View filing
                </Text>
              </Text>
            </View>
          );
        })}
      </View>
    </View>
  );
}

/**
 * Member communications: an organization spending to reach its own members
 * about a candidate (a union mailing its membership, for example). Legally
 * distinct from independent expenditures — it may even be coordinated with
 * the campaign — so it gets its own section instead of joining the outside
 * totals. Neutral styling like this card's outside boxes. A $0 side renders
 * nothing (the LA sync writes 0 for every linked candidate).
 */
function MemberCommunicationsSection({
  supportTotal,
  opposeTotal,
}: {
  supportTotal: number | null | undefined;
  opposeTotal: number | null | undefined;
}) {
  const rows = [
    { direction: "supporting", amount: supportTotal },
    { direction: "opposing", amount: opposeTotal },
  ].filter((row) => (row.amount ?? 0) > 0);
  if (rows.length === 0) {
    return null;
  }
  return (
    <View className="mt-3">
      <Text className="text-xs font-semibold uppercase tracking-wide text-ink-soft">
        Member communications
      </Text>
      <Text className="mt-1 text-xs text-ink-soft">
        Money organizations — such as unions or trade associations — reported spending to talk to
        their own members about this candidate. Disclosed separately from outside spending, and it
        does not go to the candidate.
      </Text>
      {rows.map((row) => (
        <View key={row.direction} className="mt-2 rounded border border-line p-2">
          <Text className="text-sm font-medium text-ink">
            Spent {row.direction} this candidate: {formatMoney(row.amount ?? 0)}
          </Text>
        </View>
      ))}
    </View>
  );
}

export function FinanceSummaryCard({ summary }: { summary: FinanceSummary }) {
  const direct = summary.direct_campaign;
  const outside = summary.outside_spending;
  // Loans render only when positive: a "Loans $0" stat tells voters nothing,
  // and sources that don't report loans leave the field absent entirely.
  const loansReceived = direct.loans_received ?? null;
  const hasLoans = loansReceived !== null && loansReceived > 0;
  const hasMoneyRow =
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null ||
    hasLoans ||
    direct.public_funds_received != null;
  const sourceUrl = firstFinanceSourceUrl(summary);
  // Supporting industries prefer the backing-summary rows, which carry the
  // organizations behind each industry; fall back to the plain breakdown.
  const supportingIndustries: (FinanceBreakdown | FinanceOutsideIndustrySupport)[] =
    summary.backing_summary?.top_outside_supporting_industries?.length
      ? summary.backing_summary.top_outside_supporting_industries
      : outside.top_supporting_industries;

  return (
    <View>
      <Text className="text-xs text-ink-soft">
        Data last updated {formatElectionDate(summary.last_synced_at.slice(0, 10))}
      </Text>

      {hasMoneyRow ? (
        <View className="mt-3 flex-row flex-wrap">
          <MoneyStat label="Raised" amount={direct.total_raised} />
          {hasLoans ? <MoneyStat label="Loans" amount={loansReceived} /> : null}
          <MoneyStat label="Spent" amount={direct.total_spent} />
          <MoneyStat label="Cash on hand" amount={direct.cash_on_hand} />
          <MoneyStat label="Debts" amount={direct.debts_owed} />
          <MoneyStat label="Public funds" amount={direct.public_funds_received ?? null} />
        </View>
      ) : null}
      {hasLoans ? (
        <Text className="mt-1 text-xs text-ink-soft">
          Loans are borrowed money the campaign reported receiving — often from the candidate
          themselves — and are not counted in Raised.
        </Text>
      ) : null}
      {spendingExceedsCycleFunds(summary) ? (
        <Text className="mt-1 text-xs text-ink-soft">
          Spent can be higher than Raised because campaigns can also use money not counted in
          Raised, like funds from earlier years or loans.
        </Text>
      ) : null}

      {/* Same rule as the outside note (and as the web card): stated with the
          breakdowns it qualifies, only for sources with a known gap, and only
          when there are breakdowns to qualify — under totals alone the card
          asserts nothing about itemization. */}
      {direct.direct_coverage_note &&
      (direct.top_occupations.length > 0 || (direct.contribution_size_buckets ?? []).length > 0) ? (
        <Text className="mt-1 text-xs text-ink-soft">{direct.direct_coverage_note}</Text>
      ) : null}
      <BreakdownList
        heading="Top disclosed occupations of direct donors"
        rows={direct.top_occupations}
        visibleCount={VISIBLE_OCCUPATIONS}
      />
      <BreakdownList
        heading="Direct contributions by size"
        rows={sortContributionSizeBuckets(direct.contribution_size_buckets ?? [])}
      />

      {hasOutsideFinanceContent(summary) ? (
        <View className="mt-3">
          <Text className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Outside spending</Text>
          <Text className="mt-1 text-xs text-ink-soft">
            Outside spending is money spent on this race by outside groups — such as PACs and super
            PACs — not by the candidate&apos;s own campaign. This spending is not coordinated with the
            candidate&apos;s campaign and does not go directly to the candidate.
          </Text>
          {/* Stated with the outside evidence, not in the footnote: a reader
              needs the source's known coverage gap beside the claim it
              qualifies. Deliberately inside the hasOutsideFinanceContent
              gate: with no outside data shown the card asserts nothing about
              outside money, and a disclaimer under an empty heading would
              imply there is data. */}
          {outside.outside_coverage_note ? (
            <Text className="mt-1 text-xs text-ink-soft">{outside.outside_coverage_note}</Text>
          ) : null}
          <OutsideSection
            direction="support"
            total={outside.support_total}
            groups={outside.top_supporting_groups}
            industries={supportingIndustries}
          />
          <OutsideSection
            direction="opposition"
            total={outside.oppose_total}
            groups={outside.top_opposing_groups}
            industries={outside.top_opposing_industries}
          />
          <UnallocatedOutsideEdges edges={outside.unallocated_candidate_edges ?? []} />
          {supportingIndustries.length > 0 || outside.top_opposing_industries.length > 0 ? (
            <Text className="mt-1 text-xs text-ink-soft">
              Industry amounts are contributions to these groups, not amounts necessarily spent on this
              candidate.
            </Text>
          ) : null}
        </View>
      ) : null}

      <MemberCommunicationsSection
        supportTotal={outside.membership_support_total}
        opposeTotal={outside.membership_oppose_total}
      />

      <Text className="mt-3 text-xs text-ink-soft">
        Source: {financeSourceLabel(summary.source)} · {summary.cycle} cycle
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

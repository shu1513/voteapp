import type {
  FinanceBreakdown,
  FinanceOutsideGroup,
  FinanceOutsideIndustrySupport,
  FinanceSummary,
} from "@voteapp/api-client";
import {
  financeSourceLabel,
  firstFinanceSourceUrl,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatOutsideEvidenceLines,
  formatSourceHost,
  hasFinanceContent,
  hasOutsideFinanceContent,
  sortContributionSizeBuckets,
} from "@voteapp/api-client";

// Campaign finance disclosure panel, rendered on the candidate profile page
// only (the election page's candidate cards deliberately omit finance).
// Wording is claims-precise:
// these are amounts and categories reported to the disclosing agency, so
// headings say "disclosed" / "reporting", and outside committees are
// "outside groups" (state terminology differs; not every one is a Super PAC).
//
// The card keeps a brief default view: top occupations (first few, rest
// behind a disclosure), size buckets largest-first, and outside spending
// with a plain-language explanation. Employers and direct-donor industries
// are deliberately not rendered — they don't help voters decide.

// "Anything to render" logic lives in the shared package (the mobile card
// uses the same definition); re-exported here for this component's callers.
export { hasFinanceContent };

// How many occupation rows show before the rest collapse behind "Show more".
const VISIBLE_OCCUPATIONS = 4;

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

function BreakdownRows({ rows }: { rows: FinanceBreakdown[] }) {
  return (
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
  );
}

function BreakdownList({
  heading,
  rows,
  visibleCount,
}: {
  heading: string;
  rows: FinanceBreakdown[];
  /** When set, rows beyond this count collapse behind a "Show more" disclosure. */
  visibleCount?: number;
}) {
  if (rows.length === 0) {
    return null;
  }
  const visible = visibleCount !== undefined ? rows.slice(0, visibleCount) : rows;
  const hidden = visibleCount !== undefined ? rows.slice(visibleCount) : [];
  return (
    <div className="mt-3">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{heading}</h4>
      <BreakdownRows rows={visible} />
      {hidden.length > 0 ? (
        <details className="mt-1">
          <summary className="cursor-pointer select-none text-xs text-ink-soft underline hover:text-ink">
            Show {hidden.length} more
          </summary>
          <BreakdownRows rows={hidden} />
        </details>
      ) : null}
    </div>
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
    <li className="text-sm">
      <div className="flex justify-between gap-3">
        <span className="text-ink">{formatFinanceCategory(industry.category_name)}</span>
        <span className="shrink-0 text-ink-soft">{formatMoney(industry.amount)}</span>
      </div>
      {evidenceLines.map((line) => (
        <p key={line} className="text-xs text-ink-soft">
          {line}
        </p>
      ))}
    </li>
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
  // An all-empty direction renders nothing rather than a bare header — a
  // race often has disclosed support with no disclosed opposition.
  if (total === null && groups.length === 0 && industries.length === 0) {
    return null;
  }
  // Support/opposition are color-coded (green/red tint, same palette as the
  // ballot-measure YES/NO boxes) so the two directions read apart at a
  // glance. Labels say what the money did ("spent supporting/opposing this
  // candidate") — "reported support" was disclosure jargon.
  const isSupport = direction === "support";
  const directionLabel = isSupport ? "supporting" : "opposing";
  return (
    <div
      className={`mt-2 rounded border p-2 ${
        isSupport ? "border-green-200 bg-green-50" : "border-red-200 bg-red-50"
      }`}
    >
      <p className={`text-sm font-medium ${isSupport ? "text-green-900" : "text-red-900"}`}>
        Outside money spent {directionLabel} this candidate
        {total !== null ? `: ${formatMoney(total)}` : ""}
      </p>
      {industries.length > 0 ? (
        <div className="mt-2">
          {/* These amounts are contributions INTO the groups (aggregated
              from committee income across the cycle), while the total above
              is candidate-specific expenditure — an industry can have given
              a group more than the group spent on this race. The heading and
              the section's shared note keep the two from being conflated. */}
          <h5 className="text-xs font-medium text-ink-soft">Industries funding these {directionLabel} groups</h5>
          <ul className="mt-1 space-y-1">
            {industries.map((row) => (
              <OutsideIndustryRow key={row.category_name} industry={row} />
            ))}
          </ul>
        </div>
      ) : null}
      {groups.length > 0 ? (
        <details className="mt-2">
          <summary className="cursor-pointer select-none text-xs text-ink-soft underline hover:text-ink">
            Groups that spent money {directionLabel} this candidate ({groups.length})
          </summary>
          <ul className="mt-1 space-y-1">
            {groups.map((row) => (
              <li key={row.committee_id} className="text-sm">
                <div className="flex justify-between gap-3">
                  <span className="text-ink">{row.committee_name}</span>
                  <span className="shrink-0 text-ink-soft">{formatMoney(row.amount)}</span>
                </div>
                {/* Researched one-line description of who is behind the
                    committee — the name alone ("Streets for All Los Angeles
                    PAC") tells a voter nothing. Absent until researched.
                    The label is a factual claim, so the evidence behind it
                    rides along as quiet host links. */}
                {row.label ? (
                  <p className="text-xs text-ink-soft">
                    {row.label}
                    {(row.label_source_urls ?? []).map((url) => (
                      <span key={url}>
                        {" · "}
                        <a
                          href={url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="underline hover:text-ink"
                        >
                          {formatSourceHost(url)}
                        </a>
                      </span>
                    ))}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        </details>
      ) : null}
    </div>
  );
}

export function FinanceSummaryCard({ summary }: { summary: FinanceSummary }) {
  const direct = summary.direct_campaign;
  const outside = summary.outside_spending;
  const publicFundsReceived = direct.public_funds_received;
  const hasPublicFunds = publicFundsReceived != null;
  const hasMoneyRow =
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null ||
    direct.public_funds_received != null;
  const sourceUrl = firstFinanceSourceUrl(summary);
  // Supporting industries prefer the backing-summary rows, which carry the
  // organizations behind each industry; fall back to the plain breakdown.
  const supportingIndustries: (FinanceBreakdown | FinanceOutsideIndustrySupport)[] =
    summary.backing_summary?.top_outside_supporting_industries?.length
      ? summary.backing_summary.top_outside_supporting_industries
      : outside.top_supporting_industries;

  return (
    <div className="text-sm">
      <p className="text-xs text-ink-soft">
        Data last updated {formatElectionDate(summary.last_synced_at.slice(0, 10))}
      </p>

      {hasMoneyRow ? (
        <dl className={`mt-3 grid grid-cols-2 gap-3 ${hasPublicFunds ? "sm:grid-cols-5" : "sm:grid-cols-4"}`}>
          <MoneyStat label="Raised" amount={direct.total_raised} />
          <MoneyStat label="Spent" amount={direct.total_spent} />
          <MoneyStat label="Cash on hand" amount={direct.cash_on_hand} />
          <MoneyStat label="Debts" amount={direct.debts_owed} />
          {hasPublicFunds ? <MoneyStat label="Public funds" amount={publicFundsReceived} /> : null}
        </dl>
      ) : null}

      <BreakdownList
        heading="Top disclosed occupations of direct donors"
        rows={direct.top_occupations}
        visibleCount={VISIBLE_OCCUPATIONS}
      />
      {(direct.contribution_size_buckets ?? []).length > 0 ? (
        // Collapsed by default: size buckets are secondary detail next to
        // the money row and occupations.
        <details className="mt-3">
          <summary className="cursor-pointer select-none text-xs font-semibold uppercase tracking-wide text-ink-soft underline hover:text-ink">
            Direct contributions by size
          </summary>
          <BreakdownRows rows={sortContributionSizeBuckets(direct.contribution_size_buckets ?? [])} />
        </details>
      ) : null}

      {hasOutsideFinanceContent(summary) ? (
        <div className="mt-3">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-ink-soft">
            Spending by outside groups
          </h4>
          <p className="mt-1 text-xs text-ink-soft">
            Money spent on this race by outside groups — such as PACs and super PACs — not by the
            candidate's own campaign. This spending is not coordinated with the candidate's campaign
            and does not go directly to the candidate.
          </p>
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
          {supportingIndustries.length > 0 || outside.top_opposing_industries.length > 0 ? (
            <p className="mt-1 text-xs text-ink-soft">
              Industry amounts are contributions to these groups, not amounts necessarily spent on this
              candidate.
            </p>
          ) : null}
        </div>
      ) : null}

      <p className="mt-3 text-xs text-ink-soft">
        Source: {financeSourceLabel(summary.source)} · {summary.cycle} cycle
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

export const HOUSTON_FINANCE_OFFICE_NAMES = [
  "Mayor",
  "City Controller",
  "City Council Member",
] as const;

export type HoustonFinanceOfficeName = (typeof HOUSTON_FINANCE_OFFICE_NAMES)[number];
export type HoustonFinanceCouncilSeat = `District ${string}` | `At-Large ${number}`;

export type HoustonFinanceOfficeTarget = {
  officeName: HoustonFinanceOfficeName;
  seat: "Houston" | HoustonFinanceCouncilSeat;
};

const OFFICE_NAMES = new Set<string>(HOUSTON_FINANCE_OFFICE_NAMES);

function normalized(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function councilSeat(value: string): HoustonFinanceCouncilSeat | null {
  const text = normalized(value);
  const district = /\b(?:DISTRICT|DIST)\s*([A-K])\b/.exec(text)?.[1];
  if (district) return `District ${district}`;
  const atLarge = /\b(?:AT\s*LARGE(?:\s+POSITION)?|AL|POSITION)(?:\s+NO)?\s*([1-5])\b/.exec(text)?.[1];
  return atLarge ? `At-Large ${Number(atLarge)}` : null;
}

export function isHoustonFinanceOfficeName(value: string | null | undefined): value is HoustonFinanceOfficeName {
  return Boolean(value && OFFICE_NAMES.has(value.trim()));
}

export function resolveHoustonElectionOfficeTarget(input: {
  officeCanonicalName: string | null | undefined;
  officialBallotTitle: string | null | undefined;
}): HoustonFinanceOfficeTarget | null {
  const officeName = input.officeCanonicalName?.trim();
  if (officeName === "Mayor" || officeName === "City Controller") {
    return { officeName, seat: "Houston" };
  }
  if (officeName !== "City Council Member") return null;
  const seat = councilSeat(input.officialBallotTitle ?? "");
  return seat ? { officeName, seat } : null;
}

export function parseHoustonDisclosureOfficeTarget(value: string): HoustonFinanceOfficeTarget | null {
  const text = normalized(value);
  const hasMayor = /\bMAYOR\b/.test(text);
  const hasController = /\bCONTROLLER\b/.test(text);
  const hasCouncil = /\b(COUNCIL|COUNCILMEMBER)\b/.test(text);
  if ([hasMayor, hasController, hasCouncil].filter(Boolean).length !== 1) return null;
  if (hasMayor) {
    return { officeName: "Mayor", seat: "Houston" };
  }
  if (hasController) {
    return { officeName: "City Controller", seat: "Houston" };
  }
  const seat = councilSeat(text);
  return seat ? { officeName: "City Council Member", seat } : null;
}

export function parseHoustonEfileOfficeTarget(value: string | null | undefined): HoustonFinanceOfficeTarget | null {
  const code = value?.trim().toUpperCase() ?? "";
  if (code === "MAYOR") return { officeName: "Mayor", seat: "Houston" };
  if (/^(?:CITY_?)?CONTROLLER$/.test(code)) return { officeName: "City Controller", seat: "Houston" };
  const atLarge = /^CCM_(?:AL|AT_LARGE_?)([1-5])$/.exec(code)?.[1];
  if (atLarge) return { officeName: "City Council Member", seat: `At-Large ${Number(atLarge)}` };
  const district = /^CCM_(?:DIST(?:RICT)?_?)?([A-K])$/.exec(code)?.[1];
  if (district) return { officeName: "City Council Member", seat: `District ${district}` };
  return parseHoustonDisclosureOfficeTarget(code);
}

export function parseStoredHoustonFinanceOfficeTarget(input: {
  officeName: string | null | undefined;
  district: string | null | undefined;
}): HoustonFinanceOfficeTarget | null {
  const officeName = input.officeName?.trim();
  if (officeName === "Mayor" || officeName === "City Controller") {
    return { officeName, seat: "Houston" };
  }
  if (officeName !== "City Council Member") return null;
  const seat = councilSeat(input.district ?? "");
  return seat ? { officeName, seat } : null;
}

export function houstonFinanceOfficeTargetsEqual(
  left: HoustonFinanceOfficeTarget,
  right: HoustonFinanceOfficeTarget
): boolean {
  return left.officeName === right.officeName && left.seat === right.seat;
}

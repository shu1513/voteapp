import { firstNamesConflict, firstNameVariants } from "../finance/personFirstNameNicknames.js";
import type { AlaskaApocCampaignIncomeRow } from "./alaskaApocClient.js";
import { parseAlaskaApocDateYear } from "./alaskaApocClient.js";

export type AlaskaCandidateCommitteeResolution =
  | {
      status: "matched";
      candidateFilerId: string;
      candidateFilerName: string;
      confidence: "exact";
      source: "apoc_csv";
      sourceUrl: string | null;
      matchedRowCount: number;
    }
  | {
      status: "unmatched";
      reason: "no_candidate_filer_match";
      candidateNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      candidateFilerIds: string[];
    };

type FilerAggregate = {
  candidateFilerId: string;
  candidateFilerName: string;
  matchedRowCount: number;
  // At least one matched row hit a base key (the stored name or its quoted
  // call name), as opposed to only nickname-expansion keys.
  hasBaseKeyMatch: boolean;
  // At least one matched row carried a recognized office matching the
  // candidate's office class.
  hasOfficeEvidence: boolean;
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeAlaskaCandidateNameForStorage(value: string): string {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    throw new Error("candidate name is required");
  }
  return normalized;
}

export type AlaskaCandidateNameKeyOptions = {
  // Adds keys for common first-name nicknames ("Nick Capozzi" also keys
  // NICHOLAS CAPOZZI). Expansion is one-sided: only the VoteApp candidate
  // name is expanded, APOC filer names always match literally, so two
  // distinct formal names cannot meet at a shared nickname key.
  expandNicknames?: boolean;
};

const NAME_SUFFIX_PATTERN = /^(?:JR|SR|II|III|IV|V)$/;
const LAST_COMMA_FIRST_PATTERN = /^\s*([^,]+),\s*(.+?)\s*$/;

// "Last, First M." -> "First M. Last"; non-comma names pass through.
function toFirstNameFirst(value: string): string {
  const match = LAST_COMMA_FIRST_PATTERN.exec(value);
  return match?.[1] && match[2] ? `${match[2]} ${match[1]}` : value;
}

// VoteApp stores "Last, First M." and "First M. Last"; order the tokens
// first-name-first and drop generational suffixes so the first and last
// tokens are the given name and surname.
function orderedNameTokens(value: string): string[] {
  return normalizeTextKey(toFirstNameFirst(value))
    .split(" ")
    .filter((token) => token.length > 0 && !NAME_SUFFIX_PATTERN.test(token));
}

// Quoted call name in the roster spelling: Glenn M. "Mike" Prax -> MIKE.
function quotedCallName(value: string): string | null {
  const match = /["“”']([^"“”']{2,}?)["“”']/.exec(value);
  const token = match ? normalizeTextKey(match[1]).split(" ")[0] : undefined;
  return token && token.length >= 2 ? token : null;
}

export function normalizeAlaskaCandidateNameKeys(
  value: string,
  options: AlaskaCandidateNameKeyOptions = {}
): Set<string> {
  const keys = new Set<string>();
  const normalized = normalizeTextKey(value);
  if (normalized) {
    keys.add(normalized);
  }

  const firstNameFirst = toFirstNameFirst(value);
  if (firstNameFirst !== value) {
    const reversed = normalizeTextKey(firstNameFirst);
    if (reversed) {
      keys.add(reversed);
    }
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts.at(-1)} ${parts.slice(0, -1).join(" ")}`);
  }

  const ordered = orderedNameTokens(value);
  if (ordered.length >= 2) {
    const surname = ordered.at(-1) as string;
    const givenNames = new Set<string>([ordered[0]]);
    const callName = quotedCallName(value);
    if (callName && callName !== surname) {
      givenNames.add(callName);
    }
    if (options.expandNicknames === true) {
      for (const givenName of [...givenNames]) {
        for (const variant of firstNameVariants(givenName)) {
          givenNames.add(variant);
        }
      }
    }
    for (const givenName of givenNames) {
      if (givenName === surname) {
        continue;
      }
      keys.add(`${givenName} ${surname}`);
      keys.add(`${surname} ${givenName}`);
    }
  }

  return keys;
}

/**
 * Keys that exist only because of nickname expansion, mapped to the formal
 * given name that seeds them ("PATRICIA SMITH" -> "PATRICIA" for a stored
 * "Pat Smith"). Base keys - the stored first token and any quoted call name -
 * are deliberately absent: they carry no family evidence. Callers use this to
 * detect when nickname-expanded matches span two conflicting formal families
 * and must refuse rather than pick a side.
 */
export function alaskaCandidateNicknameKeyFamilies(value: string): Map<string, string> {
  const families = new Map<string, string>();
  const ordered = orderedNameTokens(value);
  if (ordered.length < 2) {
    return families;
  }
  const surname = ordered.at(-1) as string;
  const baseGivens = new Set<string>([ordered[0]]);
  const callName = quotedCallName(value);
  if (callName && callName !== surname) {
    baseGivens.add(callName);
  }
  const baseKeys = normalizeAlaskaCandidateNameKeys(value);
  for (const givenName of baseGivens) {
    for (const variant of firstNameVariants(givenName)) {
      if (variant === surname || baseGivens.has(variant)) {
        continue;
      }
      for (const key of [`${variant} ${surname}`, `${surname} ${variant}`]) {
        if (!baseKeys.has(key)) {
          families.set(key, variant);
        }
      }
    }
  }
  return families;
}

/**
 * True when the linked filer's own given name identifies which formal family
 * the candidate belongs to and it conflicts with the given family. A stored
 * "Pat Smith" linked to filer "Patrick Smith" makes the PATRICIA family
 * wrong by evidence, so nickname-expanded matching can drop it. The
 * constraint only engages when the filer's leading token is recognizable as
 * one of the candidate's given names or their nickname variants - a
 * committee-style filer name ("Smith for Alaska") carries no given-name
 * signal and never constrains.
 */
export function alaskaNicknameFamilyConflictsWithFiler(input: {
  candidateName: string;
  candidateFilerName: string;
  familyGivenName: string;
}): boolean {
  const filerTokens = orderedNameTokens(input.candidateFilerName);
  const ordered = orderedNameTokens(input.candidateName);
  if (filerTokens.length < 2 || ordered.length < 2) {
    return false;
  }
  const filerGiven = filerTokens[0];
  const surname = ordered.at(-1) as string;
  const knownGivens = new Set<string>([ordered[0]]);
  const callName = quotedCallName(input.candidateName);
  if (callName && callName !== surname) {
    knownGivens.add(callName);
  }
  for (const givenName of [...knownGivens]) {
    for (const variant of firstNameVariants(givenName)) {
      knownGivens.add(variant);
    }
  }
  if (!knownGivens.has(filerGiven)) {
    return false;
  }
  return firstNamesConflict(filerGiven, input.familyGivenName);
}

function rowYear(row: AlaskaApocCampaignIncomeRow): number | null {
  return row.reportYear ?? parseAlaskaApocDateYear(row.date);
}

function isCycleYear(input: { row: AlaskaApocCampaignIncomeRow; electionYear: number }): boolean {
  const year = rowYear(input.row);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function filerId(row: AlaskaApocCampaignIncomeRow): string {
  const explicit = row.filerId.trim();
  return explicit || normalizeTextKey(row.filerName);
}

// APOC filer names often carry middle names the VoteApp side lacks
// ("Nicholas James Capozzi" vs "Nick Capozzi"), so a key matches a field when
// its tokens appear in order among the field's tokens. Fields are matched
// separately: joining them first would let a key match across the seam
// ("...MCDONALD JIEUN..." from filerName + name both holding the same value).
function isOrderedTokenSubsequence(keyTokens: readonly string[], fieldTokens: readonly string[]): boolean {
  let index = 0;
  for (const token of fieldTokens) {
    if (token === keyTokens[index]) {
      index += 1;
      if (index === keyTokens.length) {
        return true;
      }
    }
  }
  return false;
}

function rowMatchedCandidateKeys(input: {
  row: AlaskaApocCampaignIncomeRow;
  candidateNameKeys: ReadonlySet<string>;
}): string[] {
  const fields = [input.row.filerName, input.row.name].map((field) => normalizeTextKey(field).split(" ").filter(Boolean));
  const matched: string[] = [];
  for (const key of input.candidateNameKeys) {
    if (key.length === 0) {
      continue;
    }
    const keyTokens = key.split(" ");
    if (fields.some((fieldTokens) => isOrderedTokenSubsequence(keyTokens, fieldTokens))) {
      matched.push(key);
    }
  }
  return matched;
}

function isCandidateFilerType(value: string): boolean {
  return normalizeTextKey(value).includes("CANDIDATE");
}

// Office compatibility classes. APOC income rows carry an Office column on
// ~87% of candidate rows; when both the row's office and the VoteApp office
// map to a known class, they must agree. Governor and Lieutenant Governor
// share a class because Alaska tickets file jointly. Unknown or blank office
// text never blocks a match - the constraint only uses evidence it can read.
type AlaskaOfficeClass = "governor_ticket" | "state_upper" | "state_lower" | "municipal";

function officeClassOfApocOffice(value: string): AlaskaOfficeClass | null {
  switch (normalizeTextKey(value)) {
    case "GOVERNOR":
    case "LT GOVERNOR":
    case "LIEUTENANT GOVERNOR":
      return "governor_ticket";
    case "SENATE":
    case "STATE SENATE":
      return "state_upper";
    case "HOUSE":
    case "STATE HOUSE":
      return "state_lower";
    case "ASSEMBLY":
    case "SCHOOL BOARD":
    case "MAYOR":
    case "CITY COUNCIL":
    case "BOROUGH ASSEMBLY":
      return "municipal";
    default:
      return null;
  }
}

function officeClassOfCandidateOffice(value: string | null | undefined): AlaskaOfficeClass | null {
  switch (normalizeTextKey(value ?? "")) {
    case "GOVERNOR":
    case "LIEUTENANT GOVERNOR":
      return "governor_ticket";
    case "STATE SENATOR":
      return "state_upper";
    case "STATE LOWER CHAMBER LEGISLATOR":
    case "STATE REPRESENTATIVE":
      return "state_lower";
    default:
      return null;
  }
}

// Alaska governor candidates file twice: a standalone filer ("Tom Begich")
// and a joint ticket filer once a running mate is named ("Tom Begich/Julia
// Hnilicka"). Both are the same campaign, so two matched filers are not real
// ambiguity when every extra filer's name is the standalone filer's name
// extended by another person's name (two or more tokens - a one-token
// extension such as "JR" could be a different person and stays ambiguous)
// AND the extra filer's raw name carries the joint-ticket "/" or "\"
// delimiter APOC uses. Callers only invoke this for governor-ticket races;
// a committee-style extension in another office ("Jane Doe" plus "Jane Doe
// for State House") must stay ambiguous rather than silently pick a side.
// Returns the standalone filer, or null when the shape does not hold.
const TICKET_DELIMITER_PATTERN = /[/\\]/;

function collapseTicketFilers(filers: readonly FilerAggregate[]): FilerAggregate | null {
  for (const base of filers) {
    const baseName = normalizeTextKey(base.candidateFilerName);
    if (!baseName) {
      continue;
    }
    const othersAreTickets = filers.every((filer) => {
      if (filer === base) {
        return true;
      }
      if (!TICKET_DELIMITER_PATTERN.test(filer.candidateFilerName)) {
        return false;
      }
      const name = normalizeTextKey(filer.candidateFilerName);
      if (!name.startsWith(`${baseName} `)) {
        return false;
      }
      return name.slice(baseName.length).trim().split(" ").filter(Boolean).length >= 2;
    });
    if (othersAreTickets) {
      return base;
    }
  }
  return null;
}

export function resolveAlaskaCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  officeName?: string | null;
  sourceUrl?: string | null;
}): AlaskaCandidateCommitteeResolution {
  const candidateNameNormalized = normalizeAlaskaCandidateNameForStorage(input.candidateName);
  // VoteApp side expands nicknames; APOC filer names always key literally.
  const candidateNameKeys = normalizeAlaskaCandidateNameKeys(input.candidateName, { expandNicknames: true });
  const nicknameFamilies = alaskaCandidateNicknameKeyFamilies(input.candidateName);
  const candidateOfficeClass = officeClassOfCandidateOffice(input.officeName);
  const filers = new Map<string, FilerAggregate>();

  for (const row of input.incomeRows) {
    if (!isCandidateFilerType(row.filerType) || !isCycleYear({ row, electionYear: input.electionYear })) {
      continue;
    }
    const matchedKeys = rowMatchedCandidateKeys({ row, candidateNameKeys });
    if (matchedKeys.length === 0) {
      continue;
    }
    const rowOfficeClass = officeClassOfApocOffice(row.office);
    if (candidateOfficeClass && rowOfficeClass && rowOfficeClass !== candidateOfficeClass) {
      continue;
    }
    const candidateFilerId = filerId(row);
    const candidateFilerName = row.filerName.trim() || row.name.trim();
    if (!candidateFilerId || !candidateFilerName) {
      continue;
    }
    const hasBaseKeyMatch = matchedKeys.some((matchedKey) => !nicknameFamilies.has(matchedKey));
    const hasOfficeEvidence = candidateOfficeClass !== null && rowOfficeClass === candidateOfficeClass;
    const key = normalizeTextKey(candidateFilerId);
    const existing = filers.get(key);
    if (existing) {
      existing.matchedRowCount += 1;
      existing.hasBaseKeyMatch ||= hasBaseKeyMatch;
      existing.hasOfficeEvidence ||= hasOfficeEvidence;
      continue;
    }
    filers.set(key, {
      candidateFilerId,
      candidateFilerName,
      matchedRowCount: 1,
      hasBaseKeyMatch,
      hasOfficeEvidence,
    });
  }

  // A nickname-expansion match is the weakest name evidence, so it must be
  // corroborated: a filer matched only through nickname keys needs at least
  // one row whose recognized office matches the candidate's office class.
  // Base-key matches (the stored name or its quoted call name) stand alone.
  for (const [key, filer] of [...filers]) {
    if (!filer.hasBaseKeyMatch && !filer.hasOfficeEvidence) {
      filers.delete(key);
    }
  }

  if (filers.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized,
    };
  }
  if (filers.size > 1) {
    // Joint tickets exist only for the governor race; other offices with
    // multiple matching filers stay ambiguous.
    const standalone =
      candidateOfficeClass === "governor_ticket" ? collapseTicketFilers([...filers.values()]) : null;
    if (!standalone) {
      return {
        status: "ambiguous",
        reason: "multiple_matching_filers",
        candidateNameNormalized,
        candidateFilerIds: [...filers.values()]
          .map((filer) => filer.candidateFilerId)
          .sort((left, right) => left.localeCompare(right)),
      };
    }
    filers.clear();
    filers.set(standalone.candidateFilerId, standalone);
  }

  const match = [...filers.values()][0];
  return {
    status: "matched",
    candidateFilerId: match.candidateFilerId,
    candidateFilerName: match.candidateFilerName,
    confidence: "exact",
    source: "apoc_csv",
    sourceUrl: input.sourceUrl ?? null,
    matchedRowCount: match.matchedRowCount,
  };
}

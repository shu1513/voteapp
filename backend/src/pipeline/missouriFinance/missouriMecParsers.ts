import { buildMissouriMecUrl, MISSOURI_MEC_PAGES } from "./missouriMecClient.js";

const CANDIDATE_EXPORT_HEADER = [
  "MECID",
  "Committee Name",
  "Candidate Name",
  "Party",
  "Office Sought",
  "Status",
] as const;

const MISSOURI_MEC_ID_PATTERN = /^[A-Z]\d{6}$/;

export type MissouriMecCandidateExportRow = {
  mecid: string;
  committeeName: string;
  candidateName: string;
  party: string | null;
  officeSought: string;
  status: string;
};

export type MissouriMecElectionHistoryRow = {
  electionDate: string;
  electionType: string;
  office: string;
  politicalSubdivision: string;
};

export type MissouriMecCommitteeInfo = {
  mecid: string;
  committeeName: string;
  candidateName: string;
  electionHistory: MissouriMecElectionHistoryRow[];
  sourceUrl: string;
};

export type MissouriMecSelectOption = {
  value: string;
  label: string;
  selected: boolean;
};

function decodeHtmlEntities(value: string): string {
  return value.replace(
    /&(?:#x([0-9a-fA-F]+)|#(\d+)|nbsp|amp|lt|gt|quot|apos);/g,
    (entity, hex?: string, decimal?: string) => {
      if (hex !== undefined) {
        return String.fromCodePoint(Number.parseInt(hex, 16));
      }
      if (decimal !== undefined) {
        return String.fromCodePoint(Number.parseInt(decimal, 10));
      }
      switch (entity) {
        case "&nbsp;":
          return " ";
        case "&amp;":
          return "&";
        case "&lt;":
          return "<";
        case "&gt;":
          return ">";
        case "&quot;":
          return '"';
        case "&apos;":
          return "'";
        default:
          return entity;
      }
    }
  );
}

function textContent(value: string): string {
  return decodeHtmlEntities(value.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

export function parseMissouriMecSelectOptions(html: string, controlId: string): MissouriMecSelectOption[] {
  const escapedId = controlId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const select = new RegExp(
    `<select\\b[^>]*id="[^"]*${escapedId}"[^>]*>([\\s\\S]*?)<\\/select>`,
    "i"
  ).exec(html);
  if (select === null) {
    return [];
  }
  return [...select[1]!.matchAll(/<option\b([^>]*)value="([^"]*)"[^>]*>([\s\S]*?)<\/option>/gi)].map(
    (match) => ({
      value: decodeHtmlEntities(match[2]!),
      label: textContent(match[3]!),
      selected: /\bselected(?:="selected")?/i.test(match[1]!),
    })
  );
}

function normalizeMecId(value: string): string {
  const mecid = value.trim().toUpperCase();
  if (!MISSOURI_MEC_ID_PATTERN.test(mecid)) {
    throw new Error(`Invalid Missouri MECID in source: ${value}`);
  }
  return mecid;
}

function parseFirstHtmlTable(html: string): string[][] {
  const table = /<table\b[^>]*>([\s\S]*?)<\/table>/i.exec(html);
  if (table === null) {
    throw new Error("Missouri MEC export has no HTML table");
  }

  const rows: string[][] = [];
  for (const row of table[1]!.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...row[1]!.matchAll(/<t[hd]\b[^>]*>([\s\S]*?)<\/t[hd]>/gi)].map((cell) =>
      textContent(cell[1]!)
    );
    if (cells.length > 0) {
      rows.push(cells);
    }
  }
  return rows;
}

export function parseMissouriMecCandidateExport(html: string): MissouriMecCandidateExportRow[] {
  const rows = parseFirstHtmlTable(html);
  const header = rows[0] ?? [];
  if (header.join("\u0000") !== CANDIDATE_EXPORT_HEADER.join("\u0000")) {
    throw new Error(`Unexpected Missouri MEC candidate export header: ${header.join(" | ")}`);
  }

  return rows.slice(1).map((row, index) => {
    if (row.length !== CANDIDATE_EXPORT_HEADER.length) {
      throw new Error(`Unexpected Missouri MEC candidate export row ${index + 2}: ${row.length} columns`);
    }
    const [rawMecid, committeeName = "", candidateName = "", party = "", officeSought = "", status = ""] = row;
    if (!committeeName || !candidateName || !officeSought || !status) {
      throw new Error(`Incomplete Missouri MEC candidate export row ${index + 2}`);
    }
    return {
      mecid: normalizeMecId(rawMecid ?? ""),
      committeeName,
      candidateName,
      party: party || null,
      officeSought,
      status,
    };
  });
}

function parseSpanText(html: string, label: string): string {
  const pattern = new RegExp(`id="[^"]*${label}"[^>]*>([\\s\\S]*?)<\\/span>`, "i");
  return textContent(pattern.exec(html)?.[1] ?? "");
}

function parseHistorySeries(html: string, label: string): string[] {
  const pattern = new RegExp(
    `id="[^"]*gvElecHistory_${label}_(\\d+)"[^>]*>([\\s\\S]*?)<\\/span>`,
    "gi"
  );
  const byIndex = new Map<number, string>();
  for (const match of html.matchAll(pattern)) {
    const index = Number.parseInt(match[1]!, 10);
    if (byIndex.has(index)) {
      throw new Error(`Duplicate Missouri MEC election-history ${label} row ${index}`);
    }
    byIndex.set(index, textContent(match[2]!));
  }
  return [...byIndex.entries()].sort((left, right) => left[0] - right[0]).map((entry) => entry[1]);
}

export function normalizeMissouriMecElectionDate(value: string): string {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value.trim());
  if (match === null) {
    throw new Error(`Invalid Missouri MEC election date: ${value}`);
  }
  const month = Number.parseInt(match[1]!, 10);
  const day = Number.parseInt(match[2]!, 10);
  const year = Number.parseInt(match[3]!, 10);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) {
    throw new Error(`Invalid Missouri MEC election date: ${value}`);
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

export function parseMissouriMecCommitteeInfo(html: string): MissouriMecCommitteeInfo {
  const mecid = normalizeMecId(parseSpanText(html, "lblMECID"));
  const committeeName = parseSpanText(html, "lblCommName");
  const candidateName = parseSpanText(html, "lblCandName");
  if (!committeeName || !candidateName) {
    throw new Error(`Incomplete Missouri MEC committee profile for ${mecid}`);
  }

  const dates = parseHistorySeries(html, "lblElecYear");
  const types = parseHistorySeries(html, "lblElectionType");
  const offices = parseHistorySeries(html, "lblSub");
  const subdivisions = parseHistorySeries(html, "lblPolSub");
  if (
    dates.length === 0 ||
    dates.length !== types.length ||
    dates.length !== offices.length ||
    dates.length !== subdivisions.length
  ) {
    throw new Error(
      `Misaligned Missouri MEC election history for ${mecid}: dates=${dates.length}, types=${types.length}, offices=${offices.length}, subdivisions=${subdivisions.length}`
    );
  }

  return {
    mecid,
    committeeName,
    candidateName,
    electionHistory: dates.map((electionDate, index) => {
      const electionType = types[index] ?? "";
      const office = offices[index] ?? "";
      const politicalSubdivision = subdivisions[index] ?? "";
      if (!electionType || !office || !politicalSubdivision) {
        throw new Error(`Incomplete Missouri MEC election-history row ${index + 1} for ${mecid}`);
      }
      return {
        electionDate: normalizeMissouriMecElectionDate(electionDate),
        electionType,
        office,
        politicalSubdivision,
      };
    }),
    sourceUrl: buildMissouriMecUrl(MISSOURI_MEC_PAGES.committeeInfo, { MECID: mecid }),
  };
}

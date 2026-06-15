export function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    field += char;
  }

  if (inQuotes) {
    throw new Error("CSV has an unterminated quoted field");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((cells) => cells.some((cell) => cell.trim().length > 0));
}

export function buildCsvHeaderIndex(header: readonly string[]): Map<string, number> {
  const headerIndex = new Map<string, number>();
  header.forEach((name, index) => {
    const normalized = normalizeCsvHeader(name);
    if (normalized) {
      headerIndex.set(normalized, index);
    }
  });
  return headerIndex;
}

export function normalizeCsvHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim().toLowerCase();
}

export function requireCsvColumn(headerIndex: ReadonlyMap<string, number>, column: string): number {
  const index = headerIndex.get(column);
  if (index === undefined) {
    throw new Error(`Missing required MEDSL CSV column: ${column}`);
  }
  return index;
}

export function requireAnyCsvColumn(headerIndex: ReadonlyMap<string, number>, columns: readonly string[]): number {
  for (const column of columns) {
    const index = headerIndex.get(column);
    if (index !== undefined) {
      return index;
    }
  }
  throw new Error(`Missing required MEDSL CSV column: ${columns.join(" or ")}`);
}

export function csvCell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

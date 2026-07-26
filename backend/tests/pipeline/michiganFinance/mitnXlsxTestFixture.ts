import { deflateRawSync } from "node:zlib";

function crc32(buffer: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/** Builds a minimal real zip (deflate) holding the given files. */
export function buildZip(files: { name: string; content: string }[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;

  for (const file of files) {
    const nameBuffer = Buffer.from(file.name, "utf8");
    const raw = Buffer.from(file.content, "utf8");
    const compressed = deflateRawSync(raw);
    const checksum = crc32(raw);

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0, 6);
    local.writeUInt16LE(8, 8); // deflate
    local.writeUInt32LE(0, 10);
    local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(raw.length, 22);
    local.writeUInt16LE(nameBuffer.length, 26);
    local.writeUInt16LE(0, 28);
    localParts.push(local, nameBuffer, compressed);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(0, 8);
    central.writeUInt16LE(8, 10);
    central.writeUInt32LE(0, 12);
    central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(compressed.length, 20);
    central.writeUInt32LE(raw.length, 24);
    central.writeUInt16LE(nameBuffer.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt32LE(offset, 42);
    centralParts.push(central, nameBuffer);

    offset += local.length + nameBuffer.length + compressed.length;
  }

  const centralSize = centralParts.reduce((sum, part) => sum + part.length, 0);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(files.length, 8);
  eocd.writeUInt16LE(files.length, 10);
  eocd.writeUInt32LE(centralSize, 12);
  eocd.writeUInt32LE(offset, 16);

  return Buffer.concat([...localParts, ...centralParts, eocd]);
}

export function sheetXml(rows: string[][]): string {
  const body = rows
    .map(
      (cells, rowIndex) =>
        `<row r="${rowIndex + 1}">` +
        cells
          .map((value, columnIndex) => {
            const ref = `${String.fromCharCode(65 + columnIndex)}${rowIndex + 1}`;
            return value === ""
              ? `<c r="${ref}" t="inlineStr"/>`
              : `<c r="${ref}" t="inlineStr"><is><t xml:space="preserve">${value}</t></is></c>`;
          })
          .join("") +
        `</row>`
    )
    .join("");
  return `<?xml version="1.0"?><worksheet><sheetData>${body}</sheetData></worksheet>`;
}

/** The real export's header row, in column order (subset used by mapping). */
export const MITN_EXPORT_HEADER = [
  "Record Type (1=Parent, 2=Child)",
  "Receipt ID",
  "Filing Status (C=Complete,I=Incomplete)",
  "Document Type",
  "Document Statement Year",
  "Document Statement Type",
  "Receiving Committee Name",
  "Receiving Committee ID#",
  "Receiving Committee Type",
  "Receiving Candidate First Name",
  "Receiving Candidate Last Name",
  "Type of Contribution",
  "Contributor First Name",
  "Organization Name/Contributor Last Name",
  "Contributor Occupation",
  "Contributor Employer",
  "Date of Contribution",
  "Amount of Contribution",
  "Cumulative from this person/org",
];

export function buildMitnExportXlsx(dataRows: string[][]): Buffer {
  return buildZip([
    { name: "xl/worksheets/sheet1.xml", content: sheetXml([[...MITN_EXPORT_HEADER], ...dataRows]) },
  ]);
}

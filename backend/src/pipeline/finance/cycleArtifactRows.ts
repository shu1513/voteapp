export function mergeCycleArtifactRows<Row>(input: {
  artifacts: readonly (readonly Row[])[];
  rowIdentity?: (row: Row) => string;
}): Row[] {
  const rows: Row[] = [];
  if (!input.rowIdentity) {
    for (const artifactRows of input.artifacts) {
      for (const row of artifactRows) {
        rows.push(row);
      }
    }
    return rows;
  }

  // Prefer the newest artifact when the same stable row appears in more than
  // one filing-year export. Keep repeated rows inside that winning artifact:
  // some sources do not expose a transaction id, so exact duplicate-looking
  // transactions within one official export cannot safely be collapsed.
  const latestArtifactIndexByIdentity = new Map<string, number>();
  for (let artifactIndex = 0; artifactIndex < input.artifacts.length; artifactIndex += 1) {
    for (const row of input.artifacts[artifactIndex] ?? []) {
      const identity = input.rowIdentity(row).trim();
      if (identity) {
        latestArtifactIndexByIdentity.set(identity, artifactIndex);
      }
    }
  }

  for (let artifactIndex = 0; artifactIndex < input.artifacts.length; artifactIndex += 1) {
    for (const row of input.artifacts[artifactIndex] ?? []) {
      const identity = input.rowIdentity(row).trim();
      if (!identity || latestArtifactIndexByIdentity.get(identity) === artifactIndex) {
        rows.push(row);
      }
    }
  }
  return rows;
}

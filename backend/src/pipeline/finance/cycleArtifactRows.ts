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
  const rowsByArtifact: Row[][] = Array.from({ length: input.artifacts.length }, () => []);
  const identitiesInNewerArtifacts = new Set<string>();
  for (let artifactIndex = input.artifacts.length - 1; artifactIndex >= 0; artifactIndex -= 1) {
    const identitiesInArtifact = new Set<string>();
    for (const row of input.artifacts[artifactIndex] ?? []) {
      const identity = input.rowIdentity(row).trim();
      if (!identity || !identitiesInNewerArtifacts.has(identity)) {
        rowsByArtifact[artifactIndex]?.push(row);
      }
      if (identity) {
        identitiesInArtifact.add(identity);
      }
    }
    for (const identity of identitiesInArtifact) {
      identitiesInNewerArtifacts.add(identity);
    }
  }

  for (const artifactRows of rowsByArtifact) {
    for (const row of artifactRows) {
      rows.push(row);
    }
  }
  return rows;
}

export function parseNewJerseyCampaignFinanceBooleanFlag(args: readonly string[], name: string): boolean {
  return args.includes(name);
}

export function assertKnownNewJerseyCampaignFinanceFlags(
  args: readonly string[],
  knownFlags: ReadonlySet<string>,
  valueFlags: ReadonlySet<string>
): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    const flagName = arg.startsWith("--") ? arg.split("=", 1)[0] ?? arg : null;
    if (!flagName || !knownFlags.has(flagName)) {
      throw new Error(`Unknown New Jersey campaign finance argument: ${arg}`);
    }
    if (arg.includes("=") && !valueFlags.has(flagName)) {
      throw new Error(`Unexpected ${flagName} value`);
    }
    if (valueFlags.has(flagName) && !arg.includes("=")) {
      index += 1;
    }
  }
}

export function parseNewJerseyCampaignFinanceFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
}

export function parseNewJerseyCampaignFinancePositiveIntegerFlag(
  args: readonly string[],
  name: string
): number | undefined {
  const raw = parseNewJerseyCampaignFinanceFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

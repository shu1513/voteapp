export type IndianaCampaignFinanceCliFlagSpec = {
  name: string;
  takesValue: boolean;
};

export function assertKnownIndianaCampaignFinanceCliArgs(
  args: readonly string[],
  specs: readonly IndianaCampaignFinanceCliFlagSpec[]
): void {
  const specByName = new Map(specs.map((spec) => [spec.name, spec]));

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected Indiana campaign finance argument: ${arg}`);
    }

    const equalsIndex = arg.indexOf("=");
    const flagName = equalsIndex === -1 ? arg : arg.slice(0, equalsIndex);
    const spec = specByName.get(flagName);
    if (!spec) {
      throw new Error(`Unknown Indiana campaign finance flag: ${flagName}`);
    }

    if (!spec.takesValue) {
      if (equalsIndex !== -1) {
        throw new Error(`Flag ${flagName} does not accept a value`);
      }
      continue;
    }

    if (equalsIndex === -1) {
      index += 1;
    }
  }
}

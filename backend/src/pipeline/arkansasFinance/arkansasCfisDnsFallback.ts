// The CFIS hostnames return NXDOMAIN on some DNS resolvers while public
// resolvers answer (plan-arkansas-finance.md, gate 1). This fetch resolves
// the hostnames through 8.8.8.8 / 1.1.1.1 for a CLI run that opts in with
// --dns-fallback; production fixes the host resolver instead of pinning
// Azure Front Door IPs (they rotate).
export async function buildArkansasCfisDnsFallbackFetch(): Promise<typeof fetch> {
  const { Agent, fetch: undiciFetch } = await import("undici");
  const { Resolver } = await import("node:dns");
  const resolver = new Resolver();
  resolver.setServers(["8.8.8.8", "1.1.1.1"]);
  const lookup = (
    hostname: string,
    options: { all?: boolean },
    callback: (error: NodeJS.ErrnoException | null, address?: unknown, family?: number) => void
  ): void => {
    resolver.resolve4(hostname, (error, addresses) => {
      if (error || !addresses || addresses.length === 0) {
        callback(error ?? Object.assign(new Error(`No A records for ${hostname}`), { code: "ENOTFOUND" }));
        return;
      }
      if (options.all) {
        callback(
          null,
          addresses.map((address) => ({ address, family: 4 }))
        );
        return;
      }
      callback(null, addresses[0], 4);
    });
  };
  const dispatcher = new Agent({ connect: { lookup: lookup as never } });
  const fetchWithDispatcher = (input: unknown, init?: unknown): Promise<unknown> =>
    undiciFetch(input as never, { ...((init as object) ?? {}), dispatcher } as never);
  return fetchWithDispatcher as unknown as typeof fetch;
}

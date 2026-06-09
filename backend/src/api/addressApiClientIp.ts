export type HeaderRecord = Record<string, string | string[] | undefined>;

export type AddressApiClientIpInput = {
  headers: HeaderRecord | undefined;
  remoteAddress: string | undefined;
};

function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  const lowerName = name.toLowerCase();
  const value = headers?.[lowerName] ?? headers?.[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function parseTrustedClientIpHeader(value: string | undefined): string | null {
  const firstValue = value
    ?.split(",")
    .map((part) => part.trim())
    .find((part) => part.length > 0);
  return firstValue ?? null;
}

export function createTrustedClientIpResolver(
  trustedClientIpHeader: string | null | undefined
): (input: AddressApiClientIpInput) => string {
  return (input) => {
    if (trustedClientIpHeader) {
      const trustedHeaderValue = parseTrustedClientIpHeader(readHeader(input.headers, trustedClientIpHeader));
      if (trustedHeaderValue) {
        return trustedHeaderValue;
      }
    }
    return input.remoteAddress?.trim() || "unknown";
  };
}

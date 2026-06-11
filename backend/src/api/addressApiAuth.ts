import type { HeaderRecord } from "./addressApiClientIp.js";

export type AddressApiAuthenticatedUserInput = {
  headers: HeaderRecord | undefined;
};

function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  const lowerName = name.toLowerCase();
  const value = headers?.[lowerName] ?? headers?.[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function parseTrustedUserIdHeader(value: string | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

export function createTrustedUserIdResolver(
  trustedUserIdHeader: string | null | undefined
): (input: AddressApiAuthenticatedUserInput) => string | null {
  return (input) => {
    if (!trustedUserIdHeader) {
      return null;
    }
    return parseTrustedUserIdHeader(readHeader(input.headers, trustedUserIdHeader));
  };
}

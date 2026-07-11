// Import this module first (before anything that touches the API client):
// Hermes has no Web Crypto, and @voteapp/api-client's useAddressSuggestions
// generates Google Places session tokens with crypto.randomUUID.
import * as Crypto from "expo-crypto";

const cryptoLike = (globalThis.crypto ?? {}) as { randomUUID?: () => string };
if (typeof cryptoLike.randomUUID !== "function") {
  cryptoLike.randomUUID = Crypto.randomUUID;
  (globalThis as { crypto?: unknown }).crypto = cryptoLike;
}

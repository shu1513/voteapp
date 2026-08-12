import { describe, expect, it } from "vitest";

import { readChatbotEmbeddingsFromEnv } from "../../src/chatbot/chatbotConfig.js";

describe("readChatbotEmbeddingsFromEnv", () => {
  it("prepends http:// to a scheme-less hostport (Render fromService injects host:port)", () => {
    expect(readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_URL: "voteapp-embeddings-2j3e:8080" }).url).toBe(
      "http://voteapp-embeddings-2j3e:8080"
    );
  });

  it("keeps explicit schemes and strips trailing slashes", () => {
    expect(readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_URL: "http://localhost:8080/" }).url).toBe(
      "http://localhost:8080"
    );
    expect(readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_URL: "https://tei.example.test" }).url).toBe(
      "https://tei.example.test"
    );
  });

  it("composes http://<host>:<port> from the split Render vars (URL unset)", () => {
    expect(readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_HOST: "voteapp-embeddings-2j3e" }).url).toBe(
      "http://voteapp-embeddings-2j3e:8080"
    );
    expect(
      readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_HOST: "tei-host", CHATBOT_EMBEDDINGS_PORT: "9090" }).url
    ).toBe("http://tei-host:9090");
  });

  it("prefers an explicit URL over the split host/port vars", () => {
    expect(
      readChatbotEmbeddingsFromEnv({
        CHATBOT_EMBEDDINGS_URL: "http://localhost:8080",
        CHATBOT_EMBEDDINGS_HOST: "ignored-host",
      }).url
    ).toBe("http://localhost:8080");
  });

  it("rejects a malformed port instead of silently truncating it", () => {
    expect(() =>
      readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_HOST: "tei-host", CHATBOT_EMBEDDINGS_PORT: "80x" })
    ).toThrow(/CHATBOT_EMBEDDINGS_PORT/);
  });

  it("returns null when unset (keyword-only degraded mode)", () => {
    expect(readChatbotEmbeddingsFromEnv({}).url).toBeNull();
  });

  it("rejects a malformed timeout instead of silently truncating it", () => {
    expect(() =>
      readChatbotEmbeddingsFromEnv({ CHATBOT_EMBEDDINGS_URL: "http://localhost:8080", CHATBOT_EMBEDDINGS_TIMEOUT_MS: "250ms" })
    ).toThrow(/CHATBOT_EMBEDDINGS_TIMEOUT_MS/);
  });
});

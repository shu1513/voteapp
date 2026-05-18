import { getPipelineEnv } from "./src/config/env.js";

const OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  if (!env.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY missing");
  }

  const prompt = [
    "You are extracting upcoming election entries for exactly one district scope.",
    "District context:",
    "- district_name: \"Vermont\"",
    "- district_type: \"statewide\"",
    "- state: \"VT\"",
    "",
    "Use this official source context as your primary reference: https://sos.vermont.gov/elections",
    "",
    "Return strict JSON with keys district_id,district_name,district_type,state,entries,review_decision,review_reason.",
    "entries rows must have official_ballot_title,election_date(YYYY-MM-DD),description,race_type,sources.",
    "Focus on upcoming elections only.",
    "If uncertain, omit that entry.",
  ].join("\n");

  const response = await fetch(OPENAI_CHAT_COMPLETIONS_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-5.4-mini",
      messages: [
        { role: "system", content: "Return strict JSON only." },
        { role: "user", content: prompt },
      ],
      response_format: { type: "json_object" },
      temperature: 0,
    }),
  });

  const text = await response.text();
  console.log(JSON.stringify({ status: response.status, body: text }, null, 2));
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});

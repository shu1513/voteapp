const key = process.env.OPENAI_API_KEY;
if (!key) {
  throw new Error("OPENAI_API_KEY missing");
}

const controller = new AbortController();
const timeout = setTimeout(() => controller.abort(), 30000);

try {
  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-5.5",
      input: [
        {
          role: "user",
          content: [{ type: "input_text", text: "Return exactly this JSON: {\"ok\":true}" }],
        },
      ],
    }),
    signal: controller.signal,
  });

  const text = await response.text();
  console.log(JSON.stringify({ status: response.status, ok: response.ok, body: text.slice(0, 500) }, null, 2));
} finally {
  clearTimeout(timeout);
}

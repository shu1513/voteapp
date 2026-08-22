// Renders the labeler prompt over benchmark.json so a reviewer can hand it to a
// model session and score the answer by hand. Makes no provider calls.
//   npx tsx tests/fixtures/candidateRecordAreaLabel/render.ts > /path/to/prompt.txt
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { buildCandidateRecordAreaLabelPrompt } from "../../../src/ai/providers/candidateRecordAreaLabelPrompt.js";

const here = dirname(fileURLToPath(import.meta.url));
const areas = readFileSync(join(here, "areas.jsonl"), "utf8")
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line) as { slug: string; description: string });
const bench = JSON.parse(readFileSync(join(here, "benchmark.json"), "utf8")) as Array<{ description: string }>;

process.stdout.write(
  buildCandidateRecordAreaLabelPrompt({
    candidateDisplayName: "Jordan Reeves",
    districtName: "State House District 12",
    districtType: "state_house",
    state: "OH",
    electionDate: "2026-11-03",
    officialBallotTitle: "State Representative, District 12",
    allowedResearchAreaSlugs: areas.map((a) => a.slug),
    allowedResearchAreaGoals: areas.map((a) => ({ slug: a.slug, description: a.description })),
    records: bench.map((b) => ({ description: b.description, sourceUrl: "https://example.gov/record", eventDate: "2026-01-15" })),
    reviewFeedbackLines: [],
  })
);

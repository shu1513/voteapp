import { enrichElections, buildEnrichElectionsConfigFromEnv } from "./src/ai/enrichElections.ts";

(async () => {
  const config = buildEnrichElectionsConfigFromEnv();
  const result = await enrichElections(
    {
      ingestKey: "live:la-county:prompt-check",
      draft: {
        district_id: "73189dab-bc6f-45fb-ad68-5002db657225",
        district_name: "Los Angeles County, California",
        district_type: "county",
        state: "CA",
      },
      promptVersion: "elections_v2",
      softRetryCount: 0,
      reviewFeedback: [],
    },
    config
  );

  console.log(JSON.stringify(result, null, 2));
})();

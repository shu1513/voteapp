import { runDistrictsLoader, type DistrictLoadType } from "../pipeline/loaders/districtsLoader.js";

function readTypeArg(argv: string[]): DistrictLoadType {
  const prefix = "--type=";
  const typeArg = argv.find((arg) => arg.startsWith(prefix));
  const value = typeArg ? typeArg.slice(prefix.length).trim() : "state";

  if (value === "state") {
    return value;
  }

  throw new Error(`Invalid --type value: ${value}. Supported: state`);
}

const type = readTypeArg(process.argv.slice(2));
const dryRun = process.argv.includes("--dry-run");

runDistrictsLoader({ type, dryRun })
  .then((summary) => {
    console.log(
      `districts loader completed type=${summary.type} dryRun=${summary.dryRun} total=${summary.totalCandidates} inserted=${summary.inserted} updated=${summary.updated} skipped=${summary.skipped}`
    );
    if (summary.dryRun) {
      console.log(`districts dry-run source=${summary.sourceUrl}`);
    }
  })
  .catch((error) => {
    console.error("districts loader failed:", error);
    process.exit(1);
  });

import { statSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  readIdahoRegistrationArtifact,
  storeIdahoRegistrationArtifact,
  type IdahoRegistrationArtifact,
} from "../../../src/pipeline/idahoFinance/idahoRegistrationArtifactCache.js";
import { contribution, GUID_A, GUID_B, independentExpenditure, registration } from "./idahoTestFixtures.js";

const SOURCE_URL = "https://api-sunshine.voteidaho.gov/api/PublicFilerDetails/GetCandidateDetails";

function artifact(overrides: Partial<IdahoRegistrationArtifact> = {}): IdahoRegistrationArtifact {
  return {
    version: 1,
    registration: registration({ registrationGuid: GUID_A }),
    contributions: [contribution(), contribution({ guid: "33333333-3333-4333-8333-333333333302", transactionId: 313560 })],
    independentExpenditures: [independentExpenditure()],
    ...overrides,
  };
}

let cacheDir: string;

beforeEach(async () => {
  cacheDir = await mkdtemp(join(tmpdir(), "id-registration-cache-"));
});

afterEach(async () => {
  await rm(cacheDir, { recursive: true, force: true });
});

describe("idahoRegistrationArtifactCache", () => {
  it("round-trips an artifact with a verified manifest and restricted modes", async () => {
    const manifest = await storeIdahoRegistrationArtifact({
      cacheDir,
      registrationGuid: GUID_A.toUpperCase(),
      artifact: artifact(),
      sourceUrl: SOURCE_URL,
      retrievedAt: new Date("2026-09-01T12:00:00Z"),
    });
    expect(manifest).toMatchObject({
      version: 1,
      registrationGuid: GUID_A,
      sourceUrl: SOURCE_URL,
      retrievedAt: "2026-09-01T12:00:00.000Z",
      contributionCount: 2,
      independentExpenditureCount: 1,
    });
    expect(manifest.sha256).toMatch(/^[0-9a-f]{64}$/);

    const read = await readIdahoRegistrationArtifact({ cacheDir, registrationGuid: GUID_A });
    expect(read.artifact).toEqual(artifact());
    expect(read.manifest).toEqual(manifest);
    expect(JSON.parse(await readFile(resolve(cacheDir, `${GUID_A}.json.manifest.json`), "utf8"))).toEqual(manifest);
    expect(statSync(resolve(cacheDir, `${GUID_A}.json`)).mode & 0o077).toBe(0);
    expect(statSync(cacheDir).mode & 0o077).toBe(0);
  });

  it("refuses rows that belong to another registration", async () => {
    const store = (value: IdahoRegistrationArtifact) =>
      storeIdahoRegistrationArtifact({ cacheDir, registrationGuid: GUID_A, artifact: value, sourceUrl: SOURCE_URL });

    await expect(store(artifact({ registration: registration({ registrationGuid: GUID_B }) }))).rejects.toThrow(
      `carries grid row ${GUID_B}`
    );
    await expect(
      store(artifact({ contributions: [contribution({ filerRegistrationGuid: GUID_B, transactionId: 999 })] }))
    ).rejects.toThrow("carries contribution 999");
    await expect(
      store(artifact({ independentExpenditures: [independentExpenditure({ candidateMeasureFilerRegistrationGuid: null })] }))
    ).rejects.toThrow("targeting a name-only candidate");
    await expect(
      store(artifact({ independentExpenditures: [independentExpenditure({ candidateMeasureFilerRegistrationGuid: GUID_B })] }))
    ).rejects.toThrow(`targeting ${GUID_B}`);
  });

  it("fails closed on tampered bytes and reports missing artifacts", async () => {
    await storeIdahoRegistrationArtifact({ cacheDir, registrationGuid: GUID_A, artifact: artifact(), sourceUrl: SOURCE_URL });
    await writeFile(resolve(cacheDir, `${GUID_A}.json`), `${JSON.stringify(artifact({ contributions: [] }))}\n`);
    await expect(readIdahoRegistrationArtifact({ cacheDir, registrationGuid: GUID_A })).rejects.toThrow(
      `Corrupt Idaho registration artifact: ${GUID_A}`
    );
    await expect(readIdahoRegistrationArtifact({ cacheDir, registrationGuid: GUID_B })).rejects.toThrow(
      `Missing Idaho registration artifact: ${GUID_B}`
    );
    await expect(readIdahoRegistrationArtifact({ cacheDir, registrationGuid: "../escape" })).rejects.toThrow(
      "Invalid Idaho registration guid"
    );
  });
});

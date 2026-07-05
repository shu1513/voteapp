import { expect, test } from "@playwright/test";
import { findDistrictWithElections } from "./helpers";

// Anonymous core loop: shareable ballot URL → election detail → candidate
// detail. Runs against whatever the local database has researched.

test("address-less ballot URL through election to candidate", async ({ page }) => {
  const districtId = await findDistrictWithElections();
  test.skip(districtId === null, "local database has no upcoming elections");

  await page.goto(`/ballot?d=${districtId}`);
  await expect(page.getByRole("heading", { name: "Your ballot" })).toBeVisible();
  // Every AI-content view must carry the research banner.
  await expect(page.getByText(/AI/).first()).toBeVisible();

  const cards = page.locator('a[href^="/elections/"]');
  await expect(cards.first()).toBeVisible();

  await cards.first().click();
  await expect(page).toHaveURL(/\/elections\//);
  await expect(page.locator("h1")).toBeVisible();

  // Office races link their candidates; measure-only elections may not.
  const candidateLink = page.locator('a[href^="/candidates/"]').first();
  if (await candidateLink.isVisible().catch(() => false)) {
    await candidateLink.click();
    await expect(page).toHaveURL(/\/candidates\//);
    await expect(page.locator("h1")).toBeVisible();
  }
});

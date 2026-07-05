import { expect, test } from "@playwright/test";
import { deleteAccount, findDistrictWithElections, registerVerifiedUser } from "./helpers";

// Personalization loop (research areas A–E): save issues in settings, get
// the my_areas default ballot order, reorder ranks, see the badge copy.

test("saved research areas drive the ballot default sort and the rank editor", async ({ page }) => {
  const districtId = await findDistrictWithElections();
  test.skip(districtId === null, "local database has no upcoming elections");

  await registerVerifiedUser(page.request);
  try {
    const initialize = await page.request.post("/api/me/districts/initialize", {
      data: { district_ids: [districtId] },
    });
    expect(initialize.ok()).toBeTruthy();

    // Pick the first two areas from the catalog in the settings UI.
    await page.goto("/me/settings");
    await expect(page.getByText(/Nothing selected yet/)).toBeVisible();

    const catalog = await (await page.request.get("/api/research-areas")).json();
    const [firstArea, secondArea] = catalog.research_areas;
    await page.getByRole("button", { name: firstArea.name, exact: true }).click();
    await expect(page.getByLabel(`${firstArea.name}, rank 1. Drag to reorder.`)).toBeVisible();
    await page.getByRole("button", { name: secondArea.name, exact: true }).click();
    await expect(page.getByLabel(`${secondArea.name}, rank 2. Drag to reorder.`)).toBeVisible();

    // Keyboard reorder (dnd-kit KeyboardSensor): pick up row 1, move down, drop.
    // Reorder by mouse drag (the primary drag surface). MouseSensor arms
    // after 4px of movement, so move in steps from row 1's center to below
    // row 2's center before releasing.
    const firstRow = page.getByLabel(`${firstArea.name}, rank 1. Drag to reorder.`);
    const secondRow = page.getByLabel(`${secondArea.name}, rank 2. Drag to reorder.`);
    const from = (await firstRow.boundingBox())!;
    const to = (await secondRow.boundingBox())!;
    await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2);
    await page.mouse.down();
    await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2 + 8, { steps: 3 });
    await page.mouse.move(to.x + to.width / 2, to.y + to.height * 0.9, { steps: 12 });
    await page.mouse.up();
    await expect(page.getByLabel(`${firstArea.name}, rank 2. Drag to reorder.`)).toBeVisible();
    await expect(page.getByLabel(`${secondArea.name}, rank 1. Drag to reorder.`)).toBeVisible();

    // With saved areas and no explicit sort choice, the saved ballot defaults
    // to my_areas — the subtitle carries its description.
    await page.goto("/me/ballot");
    await expect(
      page.getByText("ordered by how much each race affects the issues you care about", { exact: false })
    ).toBeVisible();
  } finally {
    await deleteAccount(page.request);
  }
});

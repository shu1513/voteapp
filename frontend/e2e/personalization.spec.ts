import { expect, test } from "@playwright/test";
import { deleteAccount, findDistrictWithElections, registerVerifiedUser } from "./helpers";

// Personalization loop (research areas A–E): save issues in settings, get
// the my_areas default ballot order, reorder ranks, see the badge copy.

test("saved research areas drive the ballot default sort and the rank editor", async ({ page }) => {
  const districtId = await findDistrictWithElections();
  test.skip(districtId === null, "local database has no upcoming elections");

  // Inside the try: registerVerifiedUser can fail after creating the account
  // (verify/login step), and cleanup should still be attempted. Best-effort —
  // without a session deleteAccount cannot succeed and must not mask the
  // original failure.
  try {
    await registerVerifiedUser(page.request);
    const initialize = await page.request.post("/api/me/districts/initialize", {
      data: { district_ids: [districtId] },
    });
    expect(initialize.ok()).toBeTruthy();

    // Pick the first two areas from the catalog in the settings UI.
    await page.goto("/me/settings");
    await expect(page.getByText("Tap an issue to add it here.")).toBeVisible();

    const catalog = await (await page.request.get("/api/research-areas")).json();
    const [firstArea, secondArea] = catalog.research_areas;
    // Settings saves every edit, and the picker is disabled (drags ignored)
    // while that PUT is in flight, so each step waits for its save to land.
    const savedPreferences = () =>
      page.waitForResponse(
        (response) =>
          response.url().includes("/api/me/research-area-preferences") && response.request().method() === "PUT"
      );
    await Promise.all([savedPreferences(), page.getByRole("button", { name: firstArea.name, exact: true }).click()]);
    await expect(page.getByLabel(`${firstArea.name}, rank 1. Drag to reorder.`)).toBeVisible();
    await Promise.all([savedPreferences(), page.getByRole("button", { name: secondArea.name, exact: true }).click()]);
    await expect(page.getByLabel(`${secondArea.name}, rank 2. Drag to reorder.`)).toBeVisible();

    // Reorder with the keyboard: focus row 1's ⠿ handle, Space picks it up,
    // ArrowDown moves it below row 2, Space drops it. This is the documented
    // accessible path (dnd-kit KeyboardSensor + sortableKeyboardCoordinates)
    // and is deterministic. A mouse drag (mousedown on the row, move past the
    // 4px activation distance, mouseup) never activated the MouseSensor under
    // headless Chromium here — the live region stayed empty — so it is not
    // used as the reorder oracle; check mouse reordering by hand when the
    // picker changes.
    // Each key waits for dnd-kit's live-region announcement before the next,
    // so the drop cannot outrun the pickup. The region is aria-atomic, and
    // "Picked up …" is overwritten at once by "… was moved over" the row
    // itself, so both waits key on the "moved over" line and its target id.
    const firstHandle = page.getByLabel(`${firstArea.name}, rank 1. Drag to reorder.`);
    const announcements = page.locator('[id^="DndLiveRegion"]');
    // savedPreferences() resolves on response headers; the picker stays
    // disabled (aria-disabled on the handle) until the body is parsed and
    // the mutation settles. focus() + press() do no enabled check, unlike
    // click(), so wait for it here or Space lands on a dead handle.
    await expect(firstHandle).toBeEnabled();
    await firstHandle.focus();
    await page.keyboard.press("Space");
    await expect(announcements).toContainText(`was moved over droppable area ${firstArea.id}`);
    await page.keyboard.press("ArrowDown");
    await expect(announcements).toContainText(`was moved over droppable area ${secondArea.id}`);
    await Promise.all([savedPreferences(), page.keyboard.press("Space")]);
    await expect(page.getByLabel(`${firstArea.name}, rank 2. Drag to reorder.`)).toBeVisible();
    await expect(page.getByLabel(`${secondArea.name}, rank 1. Drag to reorder.`)).toBeVisible();

    // With saved areas and no explicit sort choice, the saved ballot defaults
    // to my_areas — the sort control shows the order the list is in.
    await page.goto("/me/ballot");
    await expect(page.getByLabel("Sort by")).toHaveValue("my_areas");
  } finally {
    await deleteAccount(page.request).catch((error) => {
      console.warn("account cleanup skipped:", error);
    });
  }
});

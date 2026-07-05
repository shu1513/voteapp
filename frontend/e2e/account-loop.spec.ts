import { expect, test } from "@playwright/test";
import { deleteAccount, E2E_PASSWORD, readVerificationToken, uniqueEmail } from "./helpers";

// Register → verify (console-mailer link) → login → saved ballot, all
// through the UI. The account is deleted at the end, pass or fail.

test("register, verify by emailed link, log in, land on the saved ballot", async ({ page }) => {
  const email = uniqueEmail();

  await page.goto("/register");
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(E2E_PASSWORD);
  await page.getByRole("checkbox").check();
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page.getByRole("heading", { name: "Check your email" })).toBeVisible();

  // The verification link lands on the SPA's /verify-email route.
  const token = await readVerificationToken(email);
  await page.goto(`/verify-email?token=${encodeURIComponent(token)}`);
  await expect(page.getByText("Your email is verified", { exact: false })).toBeVisible();

  try {
    await page.getByRole("link", { name: "Log in" }).first().click();
    await page.getByLabel("Email").fill(email);
    await page.getByLabel("Password").fill(E2E_PASSWORD);
    await page.getByRole("button", { name: "Log in" }).click();
    await expect(page.getByText("Hi Smoke").or(page.getByText(/^Hi /))).toBeVisible();

    // Fresh account, no saved districts: the saved ballot asks for an address.
    await page.goto("/me/ballot");
    await expect(page.getByRole("heading", { name: "Set your address" })).toBeVisible();
  } finally {
    await deleteAccount(page.request);
  }
});

// Full-page navigation to another origin (Stripe Checkout / customer
// portal). A module seam rather than an inline window.location.assign so
// tests can vi.mock it — jsdom's Location is unforgeable, so the property
// itself cannot be stubbed.
export function navigateExternal(url: string): void {
  window.location.assign(url);
}

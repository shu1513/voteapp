import { Platform, Pressable, Share, Text } from "react-native";

// Opens the OS share sheet for a page on the public website. Port of the web
// ShareButton's native branch — on native there is no fallback menu to port,
// the sheet always exists.
//
// The shared link always points at the WEBSITE, not a deep link: recipients
// mostly don't have the app, and the web page carries the og:* tags that make
// the link render as a rich card in messengers (see frontend pageMeta.ts).
// SITE_ORIGIN matches SITE_ORIGIN in frontend/src/lib/pageMeta.ts and
// render.yaml. Exported for screens that display the public URL itself
// (the pick-card share link) next to this button.
export const SITE_ORIGIN = "https://electionssimplified.com";

type ShareButtonProps = {
  /** Path with a leading slash on the public site, e.g. "/candidates/abc". */
  path: string;
  /** One line of context sent alongside the link. */
  shareText: string;
};

export function ShareButton({ path, shareText }: ShareButtonProps) {
  const url = `${SITE_ORIGIN}${path}`;

  async function onShare() {
    try {
      // iOS takes the URL as its own field (targets like Messages render it
      // as a link preview); Android's sheet only forwards `message`, so the
      // URL rides in the text there.
      await Share.share(
        Platform.OS === "ios" ? { message: shareText, url } : { message: `${shareText}\n${url}` },
        Platform.OS === "ios" ? { subject: shareText } : { dialogTitle: "Share" }
      );
    } catch {
      // Dismissing the sheet resolves; a rejection means no share target
      // was available — nothing useful to surface either way.
    }
  }

  return (
    <Pressable
      onPress={onShare}
      accessibilityRole="button"
      accessibilityLabel={`Share ${shareText}`}
      className="rounded-lg border border-line bg-white px-3 py-2 active:border-rausch"
    >
      <Text className="text-sm font-semibold text-ink">Share</Text>
    </Pressable>
  );
}

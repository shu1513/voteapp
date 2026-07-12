import Ionicons from "@expo/vector-icons/Ionicons";
import type { Me } from "@voteapp/api-client";
import { useRouter, type Href } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { useLogout } from "../../lib/auth";

// Settings hub. The web's single 690-line settings page becomes a row list
// pushing focused subscreens (the plan's split): Profile, Security, Email
// notifications, Issues you care about. Sections mirror the backend's
// gating: profile and security work for unverified users too; the two
// preference screens are verified-only and their rows hide until then.

function SettingsRow({
  icon,
  label,
  detail,
  onPress,
}: {
  icon: keyof typeof Ionicons.glyphMap;
  label: string;
  detail?: string;
  onPress: () => void;
}) {
  return (
    <Pressable
      onPress={onPress}
      accessibilityRole="link"
      accessibilityLabel={label}
      className="flex-row items-center gap-3 rounded-xl border border-line bg-white px-4 py-3 active:border-rausch"
    >
      <Ionicons name={icon} size={20} color="#717171" />
      <View className="flex-1">
        <Text className="font-medium text-ink">{label}</Text>
        {detail ? <Text className="text-xs text-ink-soft">{detail}</Text> : null}
      </View>
      <Ionicons name="chevron-forward" size={16} color="#717171" />
    </Pressable>
  );
}

function SettingsBody({ me }: { me: Me }) {
  const router = useRouter();
  const logout = useLogout();
  const push = (href: Href) => () => router.push(href);

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-sm text-ink-soft">
        Signed in as {me.email}
        {me.email_verified ? "" : " (unverified)"}
      </Text>

      <View className="mt-4 gap-2">
        <SettingsRow icon="person-outline" label="Profile" detail="Name and email address" onPress={push("/settings/profile")} />
        <SettingsRow
          icon="lock-closed-outline"
          label="Security"
          detail="Password, sessions, delete account"
          onPress={push("/settings/security")}
        />
        {me.email_verified ? (
          <>
            <SettingsRow
              icon="mail-outline"
              label="Email notifications"
              detail="Digest, alerts and reminders"
              onPress={push("/settings/email-preferences")}
            />
            <SettingsRow
              icon="star-outline"
              label="Issues you care about"
              detail="Rank the issues that order your ballot"
              onPress={push("/settings/research-areas")}
            />
          </>
        ) : (
          <Text className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
            Verify your email to manage notifications and issue preferences.
          </Text>
        )}
      </View>

      <Text className="mt-8 text-xs font-semibold uppercase text-ink-soft">Legal</Text>
      <View className="mt-2 gap-2">
        <SettingsRow icon="document-text-outline" label="Terms of Use" onPress={push("/legal/terms")} />
        <SettingsRow icon="shield-outline" label="Privacy Policy" onPress={push("/legal/privacy")} />
        <SettingsRow icon="information-circle-outline" label="Disclaimer" onPress={push("/legal/disclaimer")} />
      </View>

      <Pressable
        disabled={logout.isPending}
        onPress={() => logout.mutate()}
        accessibilityRole="button"
        className="mt-8 rounded-xl border border-line bg-white px-4 py-3 active:border-rausch"
      >
        <Text className="text-center font-semibold text-ink">
          {logout.isPending ? "Logging out…" : "Log out"}
        </Text>
      </Pressable>
    </ScrollView>
  );
}

export default function SettingsScreen() {
  return (
    <AccountGate signedOutText="Log in to manage your account." allowUnverified>
      {(me) => <SettingsBody me={me} />}
    </AccountGate>
  );
}

import Ionicons from "@expo/vector-icons/Ionicons";
import { Tabs } from "expo-router";

/**
 * Bottom tabs (the plan's mobile IA): Home search, saved ballot, follows,
 * settings. Detail, auth, settings-detail and legal screens stay in the
 * root stack so they push over the tab bar's content.
 */
export default function TabsLayout() {
  return (
    <Tabs
      screenOptions={{
        tabBarActiveTintColor: "#ff385c",
        tabBarInactiveTintColor: "#717171",
      }}
    >
      <Tabs.Screen
        name="index"
        options={{
          title: "VoteApp",
          tabBarLabel: "Home",
          tabBarIcon: ({ color, size }) => <Ionicons name="search" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="my-ballot"
        options={{
          title: "Your saved ballot",
          tabBarLabel: "My ballot",
          tabBarIcon: ({ color, size }) => <Ionicons name="reader-outline" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="follows"
        options={{
          title: "Candidates you follow",
          tabBarLabel: "Follows",
          tabBarIcon: ({ color, size }) => <Ionicons name="people-outline" color={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="settings"
        options={{
          title: "Settings",
          tabBarLabel: "Settings",
          tabBarIcon: ({ color, size }) => <Ionicons name="settings-outline" color={color} size={size} />,
        }}
      />
    </Tabs>
  );
}

const { getDefaultConfig } = require("expo/metro-config");
const { withNativeWind } = require("nativewind/metro");

// expo/metro-config detects the npm-workspaces monorepo automatically
// (watchFolders + node_modules resolution); only NativeWind needs wiring.
const config = getDefaultConfig(__dirname);

module.exports = withNativeWind(config, { input: "./src/global.css" });

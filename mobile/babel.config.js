module.exports = function (api) {
  api.cache(true);
  return {
    presets: [["babel-preset-expo", { jsxImportSource: "nativewind" }], "nativewind/babel"],
    plugins: [
      // Inlines docs/legal/*.md as strings at build time (Metro has no
      // Vite-style ?raw): the repo's versioned legal texts stay the single
      // source of truth, same as the web's raw imports.
      ["inline-import", { extensions: [".md"] }],
    ],
  };
};

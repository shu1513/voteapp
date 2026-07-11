/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  presets: [require("nativewind/preset")],
  theme: {
    extend: {
      // Mirror of the web theme tokens in frontend/src/index.css @theme.
      // Keep the two files in sync until a shared token source exists.
      colors: {
        rausch: "#ff385c",
        "rausch-dark": "#e31c5f",
        ink: "#222222",
        "ink-soft": "#717171",
        line: "#dddddd",
        surface: "#f7f7f7",
      },
    },
  },
  plugins: [],
};

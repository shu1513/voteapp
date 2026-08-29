/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  presets: [require("nativewind/preset")],
  theme: {
    extend: {
      // Mirror of the web theme tokens in frontend/src/index.css @theme.
      // Keep the two files in sync until a shared token source exists.
      // Same 14px -> 15px raise as web's --text-sm (index.css): the whole
      // chrome tier (card meta, form labels, summaries) sits on text-sm and
      // 14px read too small on phones. Line height is absolute because RN
      // has no unitless line-height; 22px ~= web's 1.45.
      fontSize: {
        sm: ["15px", { lineHeight: "22px" }],
      },
      colors: {
        rausch: "#ff385c",
        "rausch-dark": "#e31c5f",
        ink: "#222222",
        "ink-soft": "#717171",
        line: "#dddddd",
        surface: "#f7f7f7",
        "dem-blue": "#0015bc",
        "gop-red": "#cc0000",
        // Reserved pick yellow (web --color-pick): only ever marks an UNDONE
        // pick decision — never reuse for other CTAs.
        pick: "#ffd814",
        "pick-hover": "#f7ca00",
        // Address-nudge greens (web --color-nudge*): the pick gate's
        // "enter your address" line.
        nudge: "#e8f5e9",
        "nudge-line": "#a5d6a7",
        "nudge-deep": "#166534",
      },
    },
  },
  plugins: [],
};

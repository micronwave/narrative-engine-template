/**
 * Shared color constants for use in components where CSS var() cannot be used
 * (SVG stroke/fill attributes, inline styles tested by JSDOM assertions).
 *
 * These values MUST match the corresponding CSS variables in globals.css.
 * When updating colors, update BOTH this file and globals.css.
 */
export const COLORS = {
  bullish: "#32A467",
  bearish: "#E76A6E",
  alert: "#EC9A3C",
  danger: "#E76A6E",
  accent: "#2D72D2",
  purple: "#A854F7",
  muted: "#738091",
} as const;

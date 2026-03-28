import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
        base: "var(--bg-base)",
        surface: "var(--bg-surface)",
        elevated: "var(--bg-elevated)",
        overlay: "var(--bg-overlay)",
        inset: "var(--bg-inset)",
        "accent-primary": "var(--accent-primary)",
        "accent-hover": "var(--accent-primary-hover)",
        "accent-muted": "var(--accent-primary-muted)",
        "accent-text": "var(--accent-primary-text)",
        bullish: "var(--bullish)",
        "bullish-bg": "var(--bullish-bg)",
        bearish: "var(--bearish)",
        "bearish-bg": "var(--bearish-bg)",
        alert: "var(--alert)",
        "alert-bg": "var(--alert-bg)",
        critical: "var(--critical)",
        "critical-bg": "var(--critical-bg)",
        purple: "var(--purple)",
        "purple-muted": "var(--purple-muted)",
        "surface-hover": "var(--bg-surface-hover)",
        "text-primary": "var(--text-primary)",
        "text-secondary": "var(--text-secondary)",
        "text-tertiary": "var(--text-tertiary)",
        "text-disabled": "var(--text-disabled)",
        "border-default": "var(--border-default)",
        "border-subtle": "var(--border-subtle)",
        "border-strong": "var(--border-strong)",
      },
      borderRadius: {
        sm: "var(--radius-sm)",
        md: "var(--radius-md)",
        lg: "var(--radius-lg)",
        xl: "var(--radius-xl)",
        "2xl": "var(--radius-2xl)",
      },
      boxShadow: {
        card: "var(--shadow-card)",
        "card-hover": "var(--shadow-card-hover)",
        sm: "var(--shadow-sm)",
        md: "var(--shadow-md)",
        lg: "var(--shadow-lg)",
        xl: "var(--shadow-xl)",
        glow: "var(--shadow-glow)",
      },
      fontFamily: {
        display: ["var(--font-display)", "sans-serif"],
      },
      transitionTimingFunction: {
        "ease-out-spring": "var(--ease-out)",
        "ease-in-out-smooth": "var(--ease-in-out)",
      },
      transitionDuration: {
        fast: "var(--duration-fast)",
        normal: "var(--duration-normal)",
        slow: "var(--duration-slow)",
        enter: "var(--duration-enter)",
      },
    },
  },
  plugins: [],
};
export default config;

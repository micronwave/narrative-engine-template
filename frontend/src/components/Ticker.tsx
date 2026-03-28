"use client";

import { useRouter } from "next/navigation";
import type { TickerItem } from "@/lib/api";

type Props = {
  items: TickerItem[];
};

export default function Ticker({ items }: Props) {
  const router = useRouter();

  if (!items.length) return null;

  // Show up to 5 emerging trends (sorted by velocity, take top items with IDs)
  const trends = items
    .filter((item) => item.id)
    .slice(0, 5);

  return (
    <div
      className="w-full flex items-center gap-4 px-4 lg:px-6 overflow-hidden"
      style={{
        height: 36,
        background: "var(--bg-hero)",
        borderBottom: "1px solid var(--bg-border)",
      }}
      aria-label="Today's emerging trends"
    >
      <span
        className="shrink-0"
        style={{
          fontSize: "var(--text-micro)",
          fontWeight: 500,
          color: "var(--text-muted)",
          textTransform: "uppercase",
          letterSpacing: "0.5px",
        }}
      >
        Emerging Trends
      </span>

      <div className="flex items-center gap-2 overflow-hidden">
        {trends.map((item, i) => (
          <button
            key={i}
            onClick={() => {
              if (item.id) router.push(`/narrative/${item.id}`);
            }}
            className="shrink-0 truncate max-w-[200px] transition-all"
            style={{
              fontSize: "var(--text-small)",
              color: "var(--text-muted)",
              padding: "var(--space-1) var(--space-3)",
              borderRadius: "var(--radius-sm)",
              background: "var(--bg-surface-hover)",
              border: "none",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.color = "var(--intent-primary)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.color = "var(--text-muted)";
            }}
          >
            {item.name}
          </button>
        ))}
      </div>
    </div>
  );
}

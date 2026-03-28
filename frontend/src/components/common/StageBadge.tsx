"use client";

type Props = {
  stage: string;
  className?: string;
};

function stageBadgeClass(stage: string): string {
  switch (stage) {
    case "Emerging":
      return "bg-accent-muted text-accent-text";
    case "Growing":
      return "bg-bullish-bg text-bullish";
    case "Mature":
      return "bg-alert-bg text-alert";
    case "Declining":
      return "bg-bearish-bg text-bearish";
    case "Dormant":
      return "bg-surface-hover text-text-tertiary";
    default:
      return "bg-surface-hover text-text-tertiary";
  }
}

export default function StageBadge({ stage, className = "" }: Props) {
  return (
    <span
      data-testid="stage-badge"
      className={`inline-block font-mono text-[11px] font-medium uppercase tracking-[0.02em] px-2 py-[2px] rounded-sm ${stageBadgeClass(stage)} ${className}`}
    >
      {stage}
    </span>
  );
}

"use client";

type Props = {
  value: number;
  className?: string;
};

export default function MomentumArrow({ value, className = "" }: Props) {
  const absVal = Math.abs(value);
  const size = Math.max(12, Math.min(24, 12 + absVal * 2));

  let rotation: number;
  let colorVar: string;

  if (value >= 5) {
    rotation = -45;
    colorVar = "var(--bullish)";
  } else if (value <= -0.5) {
    rotation = 45;
    colorVar = "var(--bearish)";
  } else {
    rotation = 0;
    colorVar = "var(--text-muted)";
  }

  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke={colorVar}
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      style={{ transform: `rotate(${rotation}deg)`, transition: "transform 0.12s ease" }}
    >
      <line x1="5" y1="12" x2="19" y2="12" />
      <polyline points="12 5 19 12 12 19" />
    </svg>
  );
}

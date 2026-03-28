"use client";

type Props = {
  symbol: string;
  change: string;
  className?: string;
};

export default function TickerPill({ symbol, change, className = "" }: Props) {
  const isPositive = change.startsWith("+") || (!change.startsWith("-") && parseFloat(change) > 0);
  const colorClass = isPositive ? "text-bullish bg-bullish-bg" : "text-bearish bg-bearish-bg";

  return (
    <span
      className={`inline-block font-mono text-[11px] font-normal px-1.5 py-[2px] rounded-sm mr-1 ${colorClass} ${className}`}
    >
      {symbol} {change}
    </span>
  );
}

"use client";

type Timeframe = {
  label: string;
  days: number;
};

const TIMEFRAMES: Timeframe[] = [
  { label: "1D", days: 1 },
  { label: "5D", days: 5 },
  { label: "1M", days: 30 },
  { label: "3M", days: 90 },
  { label: "6M", days: 180 },
  { label: "YTD", days: ytdDays() },
  { label: "1Y", days: 365 },
];

function ytdDays(): number {
  const now = new Date();
  const jan1 = new Date(now.getFullYear(), 0, 1);
  return Math.max(1, Math.floor((now.getTime() - jan1.getTime()) / 86400000));
}

type Props = {
  selected: number;
  onChange: (days: number) => void;
  small?: boolean;
};

export default function TimeframeSelector({ selected, onChange, small = false }: Props) {
  return (
    <div
      data-testid="timeframe-selector"
      className="flex items-center gap-1"
      role="group"
      aria-label="Select timeframe"
    >
      {TIMEFRAMES.map((tf) => (
        <button
          key={tf.label}
          data-testid={`timeframe-${tf.label}`}
          onClick={() => onChange(tf.days)}
          className={`font-mono py-0.5 rounded-sm transition-colors ${small ? "text-[9px] px-1.5" : "text-[10px] px-2"} ${
            selected === tf.days
              ? "bg-accent-primary text-base font-semibold"
              : "text-text-muted hover:text-text-secondary hover:bg-surface-hover"
          }`}
          aria-pressed={selected === tf.days}
        >
          {tf.label}
        </button>
      ))}
    </div>
  );
}

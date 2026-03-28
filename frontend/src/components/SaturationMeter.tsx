import { COLORS } from "@/lib/colors";

type Props = {
  saturation: number; // 0–1
  label?: boolean;
};

/**
 * Horizontal saturation bar with a blue→amber→red color ramp.
 * < 0.33 → blue (accent)
 * < 0.66 → amber (alert)
 * ≥ 0.66 → red (bearish)
 */
export default function SaturationMeter({ saturation, label }: Props) {
  const clamped = Math.min(Math.max(saturation, 0), 1);
  const pct = clamped * 100;
  const color =
    clamped < 0.33 ? COLORS.accent : clamped < 0.66 ? COLORS.alert : COLORS.bearish;

  return (
    <div data-testid="saturation-meter" className="flex items-center gap-2 w-full">
      <div className="flex-1 h-1.5 bg-inset rounded-sm overflow-hidden">
        <div
          data-testid="saturation-fill"
          style={{ width: `${pct}%`, backgroundColor: color }}
          className="h-full rounded-sm transition-all"
        />
      </div>
      {label && (
        <span className="text-xs font-mono-data text-text-secondary w-8 text-right">
          {Math.round(pct)}%
        </span>
      )}
    </div>
  );
}

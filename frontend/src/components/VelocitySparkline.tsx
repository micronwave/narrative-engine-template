import type { TimeseriesPoint } from "@/lib/api";
import { COLORS } from "@/lib/colors";

type Props = {
  timeseries: TimeseriesPoint[];
  width?: number;
  height?: number;
  className?: string;
  showEndValue?: boolean;
};

/**
 * Compute a smooth cubic bezier SVG path through the given points.
 * Uses Catmull-Rom → cubic bezier conversion for natural curves.
 */
function smoothPath(coords: { x: number; y: number }[]): string {
  if (coords.length < 2) return "";
  if (coords.length === 2) {
    return `M${coords[0].x},${coords[0].y} L${coords[1].x},${coords[1].y}`;
  }

  let d = `M${coords[0].x},${coords[0].y}`;

  for (let i = 0; i < coords.length - 1; i++) {
    const p0 = coords[Math.max(i - 1, 0)];
    const p1 = coords[i];
    const p2 = coords[i + 1];
    const p3 = coords[Math.min(i + 2, coords.length - 1)];

    // Catmull-Rom to cubic bezier control points (tension = 0.3)
    const tension = 0.3;
    const cp1x = p1.x + (p2.x - p0.x) * tension;
    const cp1y = p1.y + (p2.y - p0.y) * tension;
    const cp2x = p2.x - (p3.x - p1.x) * tension;
    const cp2y = p2.y - (p3.y - p1.y) * tension;

    d += ` C${cp1x.toFixed(1)},${cp1y.toFixed(1)} ${cp2x.toFixed(1)},${cp2y.toFixed(1)} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
  }

  return d;
}

/**
 * Pure SVG velocity sparkline with smooth bezier curves and area fill.
 *
 * Color: green (#32A467) when last value >= first (upward trend),
 *        red  (#E76A6E) when last value < first (downward trend).
 */
export default function VelocitySparkline({
  timeseries,
  width = 120,
  height = 30,
  className,
  showEndValue = false,
}: Props) {
  if (!timeseries || timeseries.length < 2) {
    return (
      <div
        data-testid="velocity-sparkline"
        data-timeseries={JSON.stringify(timeseries ?? [])}
        className={className}
        style={{ width, height }}
        aria-label="Velocity trend sparkline"
      />
    );
  }

  // Use a wider internal viewBox so curves have more resolution
  const internalWidth = 300;
  const endLabelWidth = showEndValue ? 40 : 0;
  const chartW = internalWidth - endLabelWidth;
  const pad = 3; // vertical padding so line doesn't clip edges

  const values = timeseries.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const coords = timeseries.map((p, i) => ({
    x: (i / (timeseries.length - 1)) * chartW,
    y: pad + (height - 2 * pad) - ((p.value - min) / range) * (height - 2 * pad),
  }));

  const linePath = smoothPath(coords);

  // Area: same curve but close back along the bottom
  const areaPath = `${linePath} L${chartW},${height} L0,${height} Z`;

  const lastVal = values[values.length - 1];
  const firstVal = values[0];
  const isUp = lastVal >= firstVal;
  const stroke = isUp ? COLORS.bullish : COLORS.bearish;
  const uid = `spark-${Math.random().toString(36).slice(2, 8)}`;

  const lastCoord = coords[coords.length - 1];

  return (
    <div
      data-testid="velocity-sparkline"
      data-timeseries={JSON.stringify(timeseries)}
      className={className}
      aria-label="Velocity trend sparkline"
      style={{ width: "100%" }}
    >
      <svg
        width="100%"
        height={height}
        viewBox={`0 0 ${internalWidth} ${height}`}
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-hidden="true"
        overflow="visible"
      >
        <defs>
          <linearGradient id={uid} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={stroke} stopOpacity={0.2} />
            <stop offset="100%" stopColor={stroke} stopOpacity={0.01} />
          </linearGradient>
        </defs>

        {/* Area fill */}
        <path d={areaPath} fill={`url(#${uid})`} />

        {/* Smooth line */}
        <path
          d={linePath}
          fill="none"
          stroke={stroke}
          strokeWidth={1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* End dot */}
        <circle cx={lastCoord.x} cy={lastCoord.y} r={3} fill={stroke} />

        {/* End value (proportional to viewBox) */}
        {showEndValue && (
          <text
            x={lastCoord.x + 8}
            y={lastCoord.y + 4}
            fill="var(--text-disabled)"
            fontSize={10}
            fontFamily="var(--font-mono)"
          >
            {lastVal.toFixed(2)}
          </text>
        )}
      </svg>
    </div>
  );
}

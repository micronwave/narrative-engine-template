"use client";

/**
 * HistoryChart — pure SVG line chart for time series data.
 * Used for velocity history and price history on narrative/brief pages.
 * Matches VelocitySparkline pattern but larger with axis labels.
 */

type DataPoint = {
  date: string;
  value: number;
};

type Props = {
  data: DataPoint[];
  color?: string;
  label?: string;
  width?: number;
  height?: number;
};

export default function HistoryChart({
  data,
  color = "#32A467",
  label,
  width = 600,
  height = 120,
}: Props) {
  if (!data || data.length < 2) {
    return (
      <div
        className="flex items-center justify-center text-text-tertiary text-xs bg-inset rounded-sm"
        style={{ width, height }}
        data-testid="history-chart-empty"
      >
        Collecting data&hellip;
      </div>
    );
  }

  const padding = { top: 12, right: 12, bottom: 24, left: 40 };
  const chartW = width - padding.left - padding.right;
  const chartH = height - padding.top - padding.bottom;

  const values = data.map((d) => d.value);
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const range = maxVal - minVal || 1;

  const points = data.map((d, i) => {
    const x = padding.left + (i / (data.length - 1)) * chartW;
    const y = padding.top + chartH - ((d.value - minVal) / range) * chartH;
    return `${x},${y}`;
  });

  // Y-axis labels (min, mid, max)
  const midVal = (minVal + maxVal) / 2;
  const yLabels = [
    { val: maxVal, y: padding.top },
    { val: midVal, y: padding.top + chartH / 2 },
    { val: minVal, y: padding.top + chartH },
  ];

  // X-axis labels (first, middle, last date)
  const xLabels = [
    { date: data[0].date, x: padding.left },
    { date: data[Math.floor(data.length / 2)].date, x: padding.left + chartW / 2 },
    { date: data[data.length - 1].date, x: padding.left + chartW },
  ];

  return (
    <div data-testid="history-chart">
      {label && (
        <p className="text-text-tertiary text-[10px] uppercase tracking-widest mb-1 font-display">
          {label}
        </p>
      )}
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        className="bg-inset rounded-sm"
      >
        {/* Grid lines */}
        {yLabels.map((yl, i) => (
          <line
            key={i}
            x1={padding.left}
            y1={yl.y}
            x2={padding.left + chartW}
            y2={yl.y}
            stroke="var(--border-subtle)"
            strokeWidth={0.5}
          />
        ))}

        {/* Y-axis labels */}
        {yLabels.map((yl, i) => (
          <text
            key={i}
            x={padding.left - 4}
            y={yl.y + 3}
            textAnchor="end"
            fill="var(--text-disabled)"
            fontSize={9}
            fontFamily="var(--font-mono)"
          >
            {yl.val.toFixed(2)}
          </text>
        ))}

        {/* X-axis labels */}
        {xLabels.map((xl, i) => (
          <text
            key={i}
            x={xl.x}
            y={height - 4}
            textAnchor={i === 0 ? "start" : i === 2 ? "end" : "middle"}
            fill="var(--text-disabled)"
            fontSize={8}
            fontFamily="var(--font-mono)"
          >
            {xl.date.slice(5)}
          </text>
        ))}

        {/* Area fill */}
        <polygon
          points={`${padding.left},${padding.top + chartH} ${points.join(" ")} ${padding.left + chartW},${padding.top + chartH}`}
          fill={color}
          fillOpacity={0.08}
        />

        {/* Line */}
        <polyline
          points={points.join(" ")}
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </svg>
    </div>
  );
}

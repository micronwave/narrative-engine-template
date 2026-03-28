"use client";

type Props = {
  velocity: number;
  size?: "sm" | "md" | "lg";
  className?: string;
};

/**
 * Horizontal momentum bar — semantic velocity colors.
 * Accelerating (>5%): warm amber. Decelerating (<-0.5%): cool teal. Stable: neutral steel.
 */
export default function MomentumBar({ velocity, size = "md", className = "" }: Props) {
  const absVel = Math.abs(velocity);
  const fillPct = Math.min((absVel / 20) * 100, 100);

  const heightMap = { sm: 3, md: 5, lg: 7 };
  const h = heightMap[size];

  const color =
    velocity > 5
      ? "var(--vel-accelerating)"
      : velocity < -0.5
      ? "var(--vel-decelerating)"
      : "var(--vel-stable)";

  return (
    <div
      className={className}
      style={{
        height: h,
        background: "var(--bg-border)",
        overflow: "hidden",
        width: "100%",
      }}
    >
      <div
        style={{
          height: "100%",
          width: `${fillPct}%`,
          background: color,
          transition: "width 300ms ease",
        }}
      />
    </div>
  );
}

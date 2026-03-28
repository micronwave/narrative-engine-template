"use client";

type Props = {
  width?: string | number;
  height?: string | number;
  className?: string;
};

export default function Skeleton({ width, height, className = "" }: Props) {
  return (
    <div
      className={`skeleton-shimmer rounded-sm ${className}`}
      style={{
        width: typeof width === "number" ? `${width}px` : width,
        height: typeof height === "number" ? `${height}px` : height,
      }}
    />
  );
}

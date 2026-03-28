"use client";

import { useState, useRef, useEffect, useId } from "react";
import { METRIC_GLOSSARY } from "@/lib/metrics";

type Props = {
  metricKey: string;
  children?: React.ReactNode;
  className?: string;
};

export default function MetricTooltip({ metricKey, children, className = "" }: Props) {
  const [show, setShow] = useState(false);
  const [position, setPosition] = useState<"above" | "below">("above");
  const triggerRef = useRef<HTMLSpanElement>(null);
  const tooltipId = useId();
  const metric = METRIC_GLOSSARY[metricKey];

  useEffect(() => {
    if (show && triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPosition(rect.top < 120 ? "below" : "above");
    }
  }, [show]);

  if (!metric) return <>{children}</>;

  const open = () => setShow(true);
  const close = () => setShow(false);
  const toggle = () => setShow((prev) => !prev);

  return (
    <span
      ref={triggerRef}
      className={`relative inline-flex items-center gap-1 ${className}`}
      onMouseEnter={open}
      onMouseLeave={close}
    >
      {children}
      <button
        type="button"
        className="inline-flex items-center justify-center bg-transparent border-none p-0 cursor-help flex-shrink-0 text-text-tertiary hover:text-text-secondary focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--accent-primary)] focus-visible:outline-offset-2"
        style={{ width: 14, height: 14 }}
        onFocus={open}
        onBlur={close}
        onClick={toggle}
        aria-describedby={show ? tooltipId : undefined}
        aria-label={`Info: ${metric.label}`}
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="1.2" />
          <text
            x="8"
            y="11.5"
            textAnchor="middle"
            fill="currentColor"
            fontSize="10"
            fontFamily="var(--font-mono)"
            fontWeight="500"
          >
            i
          </text>
        </svg>
      </button>
      {show && (
        <div
          id={tooltipId}
          role="tooltip"
          className={`absolute z-[100] whitespace-normal max-w-[300px] px-3 py-2 border border-border-default bg-surface text-[11px] font-mono ${
            position === "above" ? "bottom-full mb-2" : "top-full mt-2"
          } left-1/2 -translate-x-1/2`}
          style={{ boxShadow: "0 4px 12px rgba(0, 0, 0, 0.5)" }}
        >
          <div className="text-text-primary font-medium mb-1">{metric.label}</div>
          <div className="text-text-secondary mb-1">{metric.computation}</div>
          <div className="text-text-secondary">{metric.interpretation}</div>
        </div>
      )}
    </span>
  );
}

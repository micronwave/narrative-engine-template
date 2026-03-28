"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import type { ConstellationData, ConstellationNode } from "@/lib/api";
import { COLORS } from "@/lib/colors";

type Props = {
  data: ConstellationData;
  width?: number;
  height?: number;
};

type NodePosition = {
  id: string;
  x: number;
  y: number;
  size: number;
  node: ConstellationNode;
};

type Tooltip = {
  text: string;
  description: string | null;
  impact_score: number | null;
  x: number;
  y: number;
};

/**
 * Interactive SVG constellation map — no D3.
 *
 * Layout algorithm (deterministic):
 * - Narrative nodes: evenly spaced on a circle of radius 38% of min(w,h).
 * - Catalyst nodes: positioned between their triggered narrative and the centre.
 *
 * Interaction:
 * - Click narrative node → navigate to /narrative/{id}.
 * - Hover catalyst node → show tooltip with description + impact score.
 * - Hover narrative node → highlight connected edges.
 */
export default function ConstellationMap({
  data,
  width = 800,
  height = 560,
}: Props) {
  const router = useRouter();
  const svgRef = useRef<SVGSVGElement>(null);
  const [tooltip, setTooltip] = useState<Tooltip | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [focusedId, setFocusedId] = useState<string | null>(null);

  const { nodes, edges } = data;

  // -------------------------------------------------------------------------
  // Compute positions
  // -------------------------------------------------------------------------
  const positions = useMemo<Record<string, NodePosition>>(() => {
    const cx = width / 2;
    const cy = height / 2;
    const R = Math.min(width, height) * 0.38;

    const narrativeNodes = nodes.filter((n) => n.type === "narrative");
    const catalystNodes = nodes.filter((n) => n.type === "catalyst");

    const pos: Record<string, NodePosition> = {};

    // Narrative nodes on a circle
    narrativeNodes.forEach((n, i) => {
      const angle =
        (2 * Math.PI * i) / narrativeNodes.length - Math.PI / 2;
      pos[n.id] = {
        id: n.id,
        x: cx + R * Math.cos(angle),
        y: cy + R * Math.sin(angle),
        // Size: base 8px + entropy bonus (0–8px)
        size: 8 + (n.entropy ?? 0.5) * 8,
        node: n,
      };
    });

    // Catalyst nodes: between their triggered narrative and the centre
    catalystNodes.forEach((c) => {
      const triggeredEdge = edges.find(
        (e) => e.source === c.id && e.label === "triggered"
      );
      if (triggeredEdge && pos[triggeredEdge.target]) {
        const tgt = pos[triggeredEdge.target];
        const dx = cx - tgt.x;
        const dy = cy - tgt.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        // 40% of the way from the narrative toward the centre
        pos[c.id] = {
          id: c.id,
          x: tgt.x + (dx / dist) * (dist * 0.4),
          y: tgt.y + (dy / dist) * (dist * 0.4),
          size: 5,
          node: c,
        };
      } else {
        pos[c.id] = { id: c.id, x: cx, y: cy, size: 5, node: c };
      }
    });

    return pos;
  }, [nodes, edges, width, height]);

  // -------------------------------------------------------------------------
  // Hide tooltip when clicking outside
  // -------------------------------------------------------------------------
  useEffect(() => {
    function handleClick() {
      setTooltip(null);
    }
    document.addEventListener("click", handleClick);
    return () => document.removeEventListener("click", handleClick);
  }, []);

  // -------------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------------
  const isEdgeHighlighted = (e: (typeof edges)[0]) =>
    hoveredId === null ||
    e.source === hoveredId ||
    e.target === hoveredId;

  return (
    <div className="relative w-full" style={{ height }}>
      <svg
        ref={svgRef}
        width="100%"
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="Constellation map of narrative relationships"
        data-testid="constellation-svg"
        style={{ display: "block" }}
      >
        {/* Edges */}
        <g data-testid="constellation-edges">
          {edges.map((edge, i) => {
            const src = positions[edge.source];
            const tgt = positions[edge.target];
            if (!src || !tgt) return null;
            const highlighted = isEdgeHighlighted(edge);
            const isTriggered = edge.label === "triggered";
            return (
              <line
                key={`edge-${i}`}
                data-testid={`edge-${edge.source}-${edge.target}`}
                data-source={edge.source}
                data-target={edge.target}
                data-label={edge.label}
                x1={src.x}
                y1={src.y}
                x2={tgt.x}
                y2={tgt.y}
                stroke={isTriggered ? "#EC9A3C" : "#383E47"}
                strokeWidth={Math.max(edge.weight * 3, 0.5)}
                strokeOpacity={highlighted ? 0.7 : 0.2}
                strokeDasharray={isTriggered ? "4 2" : undefined}
              />
            );
          })}
        </g>

        {/* Edge weight labels on hover */}
        {hoveredId &&
          edges
            .filter((e) => e.source === hoveredId || e.target === hoveredId)
            .map((edge, i) => {
              const src = positions[edge.source];
              const tgt = positions[edge.target];
              if (!src || !tgt) return null;
              const mx = (src.x + tgt.x) / 2;
              const my = (src.y + tgt.y) / 2;
              return (
                <text
                  key={`edge-label-${i}`}
                  x={mx}
                  y={my - 4}
                  textAnchor="middle"
                  fill="#9ca3af"
                  fontSize={9}
                  style={{ pointerEvents: "none", userSelect: "none" }}
                >
                  {edge.label}
                </text>
              );
            })}

        {/* Nodes */}
        <g data-testid="constellation-nodes">
          {nodes.map((node) => {
            const pos = positions[node.id];
            if (!pos) return null;
            const isNarrative = node.type === "narrative";
            const isCatalyst = node.type === "catalyst";
            const isHovered = hoveredId === node.id;

            return (
              <g
                key={node.id}
                data-testid={`node-${node.id}`}
                data-id={node.id}
                data-type={node.type}
                transform={`translate(${pos.x},${pos.y})`}
                style={{ cursor: isNarrative ? "pointer" : "default" }}
                // Keyboard accessibility: narrative nodes are focusable and navigable
                tabIndex={isNarrative ? 0 : undefined}
                role={isNarrative ? "button" : undefined}
                onClick={(e) => {
                  e.stopPropagation();
                  if (isNarrative) {
                    router.push(`/narrative/${encodeURIComponent(node.id)}`);
                  }
                }}
                onKeyDown={(e) => {
                  if (isNarrative && (e.key === "Enter" || e.key === " ")) {
                    e.preventDefault();
                    router.push(`/narrative/${encodeURIComponent(node.id)}`);
                  }
                }}
                onFocus={() => isNarrative && setFocusedId(node.id)}
                onBlur={() => setFocusedId(null)}
                onMouseEnter={() => {
                  setHoveredId(node.id);
                  if (isCatalyst) {
                    setTooltip({
                      text: node.name || "Catalyst",
                      description: node.description || null,
                      impact_score: node.impact_score ?? null,
                      x: pos.x,
                      y: pos.y - pos.size - 8,
                    });
                  }
                }}
                onMouseLeave={() => {
                  setHoveredId(null);
                  setTooltip(null);
                }}
                aria-label={
                  isNarrative
                    ? `Narrative: ${node.name}`
                    : `Catalyst: ${node.name}`
                }
              >
                {/* Outer glow ring on hover or keyboard focus */}
                {isHovered && (
                  <circle
                    r={pos.size + 4}
                    fill="none"
                    stroke={isNarrative ? COLORS.accent : COLORS.alert}
                    strokeWidth={1}
                    strokeOpacity={0.5}
                  />
                )}
                {/* Focus ring for keyboard navigation */}
                {isNarrative && focusedId === node.id && (
                  <circle
                    r={pos.size + 6}
                    fill="none"
                    stroke={COLORS.accent}
                    strokeWidth={2}
                    strokeOpacity={0.9}
                  />
                )}

                {/* Node circle */}
                <circle
                  r={pos.size}
                  fill={isNarrative ? COLORS.accent : COLORS.alert}
                  fillOpacity={isNarrative ? (isHovered ? 1 : 0.85) : 0.9}
                />

                {/* Label */}
                <text
                  y={pos.size + 11}
                  textAnchor="middle"
                  fill={isNarrative ? "var(--text-primary)" : "var(--text-muted)"}
                  fontSize={isNarrative ? 9 : 7}
                  fontWeight={isNarrative ? "500" : "400"}
                  style={{ pointerEvents: "none", userSelect: "none" }}
                >
                  {(node.name || node.id).slice(0, 20)}
                </text>
              </g>
            );
          })}
        </g>

        {/* Catalyst tooltip (SVG foreignObject for HTML layout) */}
        {tooltip && (
          <foreignObject
            x={Math.max(0, Math.min(tooltip.x - 100, width - 200))}
            y={Math.max(0, tooltip.y - 56)}
            width={200}
            height={80}
            data-testid="constellation-tooltip"
            style={{ pointerEvents: "none", overflow: "visible" }}
          >
            <div
              className="bg-overlay border border-border-default text-text-primary text-xs rounded-sm px-2.5 py-1.5 shadow-lg"
              style={{ maxWidth: 200 }}
            >
              <div className="font-semibold text-amber-300 mb-0.5 font-display" data-testid="tooltip-name">
                {tooltip.text}
              </div>
              {tooltip.description && (
                <div className="text-text-secondary leading-snug mb-0.5" data-testid="tooltip-description">
                  {tooltip.description}
                </div>
              )}
              {tooltip.impact_score !== null && (
                <div className="text-text-secondary font-mono-data" data-testid="tooltip-impact">
                  Impact: {(tooltip.impact_score * 100).toFixed(0)}%
                </div>
              )}
            </div>
          </foreignObject>
        )}
      </svg>
    </div>
  );
}

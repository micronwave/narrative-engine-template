"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchLifecycleFunnel,
  type AnalyticsFunnelResponse,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import {
  sankey as d3Sankey,
  sankeyLinkHorizontal,
} from "d3-sankey";

type Props = { timeRange: string };

const STAGES = ["Emerging", "Growing", "Mature", "Declining", "Dormant"];
const STAGE_COLORS: Record<string, string> = {
  Emerging: "#4C90F0",
  Growing: "#32A467",
  Mature: "#EC9A3C",
  Declining: "#E76A6E",
  Dormant: "#738091",
};

const SVG_W = 540;
const SVG_H = 280;
const MARGIN = { top: 16, right: 100, bottom: 16, left: 16 };

interface NodeExtra { name: string; count: number }
interface LinkExtra { avgDays?: number; isRevival?: boolean }

type SNode = NodeExtra & {
  x0?: number; x1?: number; y0?: number; y1?: number;
  sourceLinks?: SLink[]; targetLinks?: SLink[];
};
type SLink = LinkExtra & {
  source: SNode | number; target: SNode | number;
  value: number; width?: number;
  y0?: number; y1?: number;
};

function SectionHeader() {
  return (
    <div className="flex items-baseline gap-3 mb-4">
      <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
        Lifecycle Funnel
      </h2>
      <span className="font-mono text-[11px] text-text-tertiary">
        stage transition flow
      </span>
    </div>
  );
}

export default function LifecycleFunnel({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsFunnelResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hoveredLink, setHoveredLink] = useState<number | null>(null);

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchLifecycleFunnel(days)
      .then(setData)
      .catch(() => setError("Failed to load lifecycle funnel data"));
  }, [days]);

  const sankeyLayout = useMemo(() => {
    if (!data) return null;

    const stageIndex: Record<string, number> = {};
    STAGES.forEach((s, i) => {
      stageIndex[s] = i;
    });

    const nodes = STAGES.map((s) => ({
      name: s,
      count: data.stage_counts[s] ?? 0,
    }));

    const links: Array<{
      source: number;
      target: number;
      value: number;
      avgDays?: number;
      isRevival?: boolean;
    }> = [];

    for (const t of data.transitions) {
      const si = stageIndex[t.from];
      const ti = stageIndex[t.to];
      if (si === undefined || ti === undefined) continue;
      if (t.count <= 0) continue;
      links.push({
        source: si,
        target: ti,
        value: t.count,
        avgDays: t.avg_days,
        isRevival: t.label === "Revival",
      });
    }

    if (links.length === 0) return null;

    try {
      const layout = d3Sankey<NodeExtra, LinkExtra>()
        .nodeWidth(20)
        .nodePadding(16)
        .extent([
          [MARGIN.left, MARGIN.top],
          [SVG_W - MARGIN.right, SVG_H - MARGIN.bottom],
        ]);

      const graph = layout({
        nodes: nodes.map((n) => ({ ...n })),
        links: links.map((l) => ({ ...l })),
      });

      return graph;
    } catch {
      return null;
    }
  }, [data]);

  const totalTracked = useMemo(() => {
    if (!data?.stage_counts) return 0;
    return Object.values(data.stage_counts).reduce((a, b) => a + b, 0);
  }, [data]);

  if (error) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  if (!data) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-text-tertiary">
          No lifecycle data available
        </p>
      </div>
    );
  }

  // Fallback: horizontal bar chart if no transitions
  if (!sankeyLayout) {
    return (
      <div>
        <SectionHeader />
        <div className="flex flex-col gap-2 mb-4">
          {STAGES.map((stage) => {
            const count = data.stage_counts[stage] ?? 0;
            const maxCount = Math.max(
              ...Object.values(data.stage_counts),
              1
            );
            const pct = (count / maxCount) * 100;
            return (
              <div key={stage} className="flex items-center gap-3">
                <span
                  className="font-mono text-[11px] w-20 text-right"
                  style={{ color: STAGE_COLORS[stage] }}
                >
                  {stage}
                </span>
                <div
                  style={{
                    height: 16,
                    flex: 1,
                    background: "var(--bg-surface-hover)",
                    borderRadius: 1,
                    overflow: "hidden",
                  }}
                >
                  <div
                    style={{
                      width: `${pct}%`,
                      height: "100%",
                      background: STAGE_COLORS[stage],
                      opacity: 0.7,
                      transition: "width 0.3s ease",
                    }}
                  />
                </div>
                <span className="font-mono text-[11px] text-text-secondary w-8">
                  {count}
                </span>
              </div>
            );
          })}
        </div>
        <StatsRow data={data} totalTracked={totalTracked} />
      </div>
    );
  }

  const linkPathGen = sankeyLinkHorizontal();

  return (
    <div>
      <SectionHeader />

      <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} className="w-full" style={{ maxHeight: 280 }}>
        {/* Links */}
        {(sankeyLayout.links as SLink[]).map((link, i) => {
          const d = linkPathGen(link as Parameters<typeof linkPathGen>[0]);
          if (!d) return null;
          const sourceNode = link.source as SNode;
          const color = STAGE_COLORS[sourceNode.name] ?? "#738091";
          const isHovered = hoveredLink === i;
          const isRevival = (link as SLink).isRevival;

          return (
            <g key={i}>
              <path
                d={d}
                fill="none"
                stroke={isRevival ? "#EC9A3C" : color}
                strokeWidth={Math.max((link.width ?? 2), 2)}
                strokeOpacity={isHovered ? 0.6 : 0.25}
                strokeDasharray={isRevival ? "6 3" : undefined}
                style={{ cursor: "pointer", transition: "stroke-opacity 0.15s ease" }}
                onMouseEnter={() => setHoveredLink(i)}
                onMouseLeave={() => setHoveredLink(null)}
              />
              {/* Annotation */}
              {(link as SLink).avgDays != null && (
                <text
                  x={
                    ((sourceNode.x1 ?? 0) +
                      ((link.target as SNode).x0 ?? 0)) /
                    2
                  }
                  y={
                    ((link.y0 ?? 0) + (link.y1 ?? 0)) / 2 - 4
                  }
                  textAnchor="middle"
                  className="font-mono"
                  style={{
                    fontSize: 9,
                    fill: "var(--text-tertiary)",
                    pointerEvents: "none",
                    opacity: isHovered ? 1 : 0.6,
                  }}
                >
                  avg {((link as SLink).avgDays ?? 0).toFixed(0)}d
                </text>
              )}
            </g>
          );
        })}

        {/* Nodes */}
        {(sankeyLayout.nodes as SNode[]).map((node) => {
          const color = STAGE_COLORS[node.name] ?? "#738091";
          const x0 = node.x0 ?? 0;
          const y0 = node.y0 ?? 0;
          const x1 = node.x1 ?? 0;
          const y1 = node.y1 ?? 0;
          const height = y1 - y0;
          if (height <= 0) return null;

          return (
            <g key={node.name}>
              <rect
                x={x0}
                y={y0}
                width={x1 - x0}
                height={height}
                fill={color}
                opacity={0.85}
                rx={1}
              />
              <text
                x={x1 + 6}
                y={y0 + height / 2}
                dominantBaseline="central"
                className="font-mono"
                style={{ fontSize: 10, fill: "var(--text-secondary)" }}
              >
                {node.name} ({node.count})
              </text>
            </g>
          );
        })}

        {/* Hover tooltip */}
        {hoveredLink !== null && (() => {
          const link = sankeyLayout.links[hoveredLink] as SLink;
          if (!link) return null;
          const src = (link.source as SNode).name;
          const tgt = (link.target as SNode).name;
          const tx =
            (((link.source as SNode).x1 ?? 0) +
              ((link.target as SNode).x0 ?? 0)) /
            2;
          const ty = ((link.y0 ?? 0) + (link.y1 ?? 0)) / 2 - 18;

          return (
            <g>
              <rect
                x={tx - 60}
                y={ty - 12}
                width={120}
                height={24}
                rx={3}
                fill="var(--bg-surface)"
                stroke="var(--bg-border)"
              />
              <text
                x={tx}
                y={ty}
                textAnchor="middle"
                dominantBaseline="central"
                className="font-mono"
                style={{ fontSize: 9, fill: "var(--text-primary)" }}
              >
                {src} → {tgt}: {link.value}
                {link.avgDays != null && ` (${link.avgDays.toFixed(0)}d avg)`}
              </text>
            </g>
          );
        })()}
      </svg>

      <StatsRow data={data} totalTracked={totalTracked} />
    </div>
  );
}

function StatsRow({
  data,
  totalTracked,
}: {
  data: AnalyticsFunnelResponse;
  totalTracked: number;
}) {
  return (
    <div className="grid grid-cols-3 gap-4 mt-2">
      <div className="text-center">
        <div className="font-mono text-[22px] font-semibold text-text-primary">
          {data.avg_lifespan_days.toFixed(0)}
        </div>
        <div className="font-mono text-[10px] text-text-tertiary">
          Avg Lifespan (days)
        </div>
      </div>
      <div className="text-center">
        <div className="font-mono text-[22px] font-semibold text-text-primary">
          {(data.revival_rate * 100).toFixed(0)}%
        </div>
        <div className="font-mono text-[10px] text-text-tertiary">
          Revival Rate
        </div>
      </div>
      <div className="text-center">
        <div className="font-mono text-[22px] font-semibold text-text-primary">
          {totalTracked}
        </div>
        <div className="font-mono text-[10px] text-text-tertiary">
          Total Tracked
        </div>
      </div>
    </div>
  );
}

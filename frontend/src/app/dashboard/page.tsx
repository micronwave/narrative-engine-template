"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { LayoutDashboard, Plus } from "lucide-react";
import DashboardGrid, { type DashboardWidget, type GridLayout } from "@/components/dashboard/DashboardGrid";
import WidgetCatalog, { type WidgetType, WIDGET_DEFINITIONS } from "@/components/dashboard/WidgetCatalog";
import { fetchDashboardLayout, saveDashboardLayout } from "@/lib/api";

const DEFAULT_WIDGETS: DashboardWidget[] = [
  { id: "narrative_radar", type: "narrative_radar", title: "Narrative Radar" },
  { id: "signal_leaderboard", type: "signal_leaderboard", title: "Signal Leaderboard" },
  { id: "top_movers", type: "top_movers", title: "Top Movers" },
  { id: "sentiment_meter", type: "sentiment_meter", title: "Sentiment Meter" },
  { id: "alert_feed", type: "alert_feed", title: "Alert Feed" },
];

const DEFAULT_GRID: GridLayout = {
  lg: [
    { i: "narrative_radar", x: 0, y: 0, w: 8, h: 4 },
    { i: "signal_leaderboard", x: 8, y: 0, w: 4, h: 4 },
    { i: "top_movers", x: 0, y: 4, w: 4, h: 3 },
    { i: "sentiment_meter", x: 4, y: 4, w: 4, h: 3 },
    { i: "alert_feed", x: 8, y: 4, w: 4, h: 3 },
  ],
};

export default function DashboardPage() {
  const [widgets, setWidgets] = useState<DashboardWidget[]>(DEFAULT_WIDGETS);
  const [gridLayout, setGridLayout] = useState<GridLayout>(DEFAULT_GRID);
  const [isEditing, setIsEditing] = useState(false);
  const [showCatalog, setShowCatalog] = useState(false);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Load saved layout on mount
  useEffect(() => {
    fetchDashboardLayout()
      .then((data) => {
        if (data?.widgets && Array.isArray(data.widgets)) {
          const allowedTypes = new Set(WIDGET_DEFINITIONS.map((d) => d.type));
          const normalizedWidgets: DashboardWidget[] = data.widgets
            .filter((w) => allowedTypes.has(w.type as WidgetType))
            .map((w) => ({ ...w, type: w.type as WidgetType }));
          if (data.widgets.length === 0) {
            setWidgets([]);
          } else if (normalizedWidgets.length > 0) {
            setWidgets(normalizedWidgets);
          }
        }
        if (data?.grid) {
          setGridLayout(data.grid);
        }
      })
      .catch(() => {});
  }, []);

  const saveLayout = useCallback(
    (updatedWidgets: DashboardWidget[], updatedGrid: GridLayout) => {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
      saveTimerRef.current = setTimeout(() => {
        saveDashboardLayout({ widgets: updatedWidgets, grid: updatedGrid }).catch(() => {});
      }, 2000);
    },
    []
  );

  const handleLayoutChange = useCallback(
    (newGrid: GridLayout) => {
      setGridLayout(newGrid);
      saveLayout(widgets, newGrid);
    },
    [widgets, saveLayout]
  );

  const handleRemoveWidget = useCallback(
    (id: string) => {
      const updated = widgets.filter((w) => w.id !== id);
      setWidgets(updated);
      saveLayout(updated, gridLayout);
    },
    [widgets, gridLayout, saveLayout]
  );

  const handleAddWidget = useCallback(
    (type: WidgetType) => {
      const def = WIDGET_DEFINITIONS.find((d) => d.type === type);
      if (!def) return;
      const newWidget: DashboardWidget = { id: `${type}_${Date.now()}`, type, title: def.title };
      const updated = [...widgets, newWidget];
      const newGrid = { ...gridLayout };
      const lgItems = newGrid.lg || [];
      newGrid.lg = [
        ...lgItems,
        { i: newWidget.id, x: (lgItems.length * 4) % 12, y: Infinity, w: 4, h: 3 },
      ];
      setWidgets(updated);
      setGridLayout(newGrid);
      saveLayout(updated, newGrid);
      setShowCatalog(false);
    },
    [widgets, gridLayout, saveLayout]
  );

  const activeTypes = widgets.map((w) => w.type);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="px-4 lg:px-8 pt-6 pb-16">
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <LayoutDashboard size={16} className="text-text-muted" />
            <h1 className="text-[16px] font-semibold text-text-primary">Dashboard</h1>
          </div>
          <div className="flex items-center gap-2">
            {isEditing && (
              <button
                onClick={() => setShowCatalog(true)}
                className="flex items-center gap-1.5 font-mono text-[11px] text-accent-text border border-[var(--bg-border)] px-3 py-1.5 rounded-sm hover:opacity-80 transition-opacity"
                data-testid="add-widget-btn"
              >
                <Plus size={12} />
                Add Widget
              </button>
            )}
            <button
              onClick={() => setIsEditing((v) => !v)}
              className={`font-mono text-[11px] px-3 py-1.5 rounded-sm border transition-colors ${
                isEditing
                  ? "bg-accent-primary border-accent-primary text-text-primary"
                  : "border-[var(--bg-border)] text-text-muted hover:text-text-primary"
              }`}
              data-testid="edit-dashboard-btn"
            >
              {isEditing ? "Done" : "Edit Dashboard"}
            </button>
          </div>
        </div>

        {/* Dashboard grid */}
        <DashboardGrid
          widgets={widgets}
          gridLayout={gridLayout}
          isEditing={isEditing}
          onLayoutChange={handleLayoutChange}
          onRemoveWidget={handleRemoveWidget}
        />
      </div>

      {showCatalog && (
        <WidgetCatalog
          activeTypes={activeTypes}
          onAdd={handleAddWidget}
          onClose={() => setShowCatalog(false)}
        />
      )}
    </main>
  );
}

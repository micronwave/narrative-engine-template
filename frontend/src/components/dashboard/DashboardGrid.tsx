"use client";

// react-grid-layout uses CommonJS exports; cast to access named members
// eslint-disable-next-line @typescript-eslint/no-require-imports
const _RGL = require("react-grid-layout") as {
  ResponsiveGridLayout: React.ComponentType<Record<string, unknown>>;
};
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";
import React from "react";
import WidgetRenderer from "./WidgetRenderer";
import type { WidgetType } from "./WidgetCatalog";
import type { Layout } from "react-grid-layout";

const { ResponsiveGridLayout } = _RGL;

export interface DashboardWidget {
  id: string;
  type: WidgetType;
  title: string;
}

export interface GridLayout {
  [breakpoint: string]: Array<{ i: string; x: number; y: number; w: number; h: number }>;
}

interface DashboardGridProps {
  widgets: DashboardWidget[];
  gridLayout: GridLayout;
  isEditing: boolean;
  onLayoutChange: (layout: GridLayout) => void;
  onRemoveWidget: (id: string) => void;
}

export default function DashboardGrid({
  widgets,
  gridLayout,
  isEditing,
  onLayoutChange,
  onRemoveWidget,
}: DashboardGridProps) {
  const breakpoints = { lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 };
  const cols = { lg: 12, md: 10, sm: 6, xs: 4, xxs: 2 };

  return (
    <div data-testid="dashboard-grid">
      <ResponsiveGridLayout
        className="layout"
        layouts={gridLayout}
        breakpoints={breakpoints}
        cols={cols}
        rowHeight={80}
        isDraggable={isEditing}
        isResizable={isEditing}
        onLayoutChange={(_currentLayout: Layout[], allLayouts: Record<string, Layout[]>) => onLayoutChange(allLayouts as unknown as GridLayout)}
        margin={[8, 8]}
      >
        {widgets.map((widget) => (
          <div key={widget.id}>
            <WidgetRenderer
              id={widget.id}
              type={widget.type}
              title={widget.title}
              isEditing={isEditing}
              onRemove={onRemoveWidget}
            />
          </div>
        ))}
      </ResponsiveGridLayout>
    </div>
  );
}

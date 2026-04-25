// Jest mock for react-grid-layout — avoids DOM measurement issues in jsdom
import React from "react";

const WidthProvider = (Component: React.ComponentType<Record<string, unknown>>) => Component;

const Responsive = ({
  children,
  className,
  "data-testid": testId,
}: {
  children?: React.ReactNode;
  className?: string;
  "data-testid"?: string;
}) =>
  React.createElement(
    "div",
    { className, "data-testid": testId ?? "responsive-grid" },
    children
  );

const ResponsiveGridLayout = Responsive;

export { Responsive, ResponsiveGridLayout, WidthProvider };
export default { Responsive, ResponsiveGridLayout, WidthProvider };

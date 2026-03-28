import React from "react";
const Link = ({ children, href, ...props }: { children: React.ReactNode; href: string; [key: string]: unknown }) =>
  React.createElement("a", { href, ...props }, children);
export default Link;

"use client";

import { useEffect, useRef, useCallback, useState } from "react";
import { X } from "lucide-react";

type Props = {
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
  title?: string;
};

export default function Drawer({ open, onClose, children, title }: Props) {
  const drawerRef = useRef<HTMLDivElement>(null);
  const previousFocus = useRef<HTMLElement | null>(null);
  const [visible, setVisible] = useState(false);
  const [animating, setAnimating] = useState(false);

  const titleId = title ? "drawer-title" : undefined;

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
        return;
      }
      if (e.key === "Tab" && drawerRef.current) {
        const focusable = drawerRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        if (focusable.length === 0) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    },
    [onClose]
  );

  // Open: mount DOM then animate in
  useEffect(() => {
    if (open) {
      previousFocus.current = document.activeElement as HTMLElement;
      setVisible(true);
      document.body.style.overflow = "hidden";
      document.addEventListener("keydown", handleKeyDown);
      // Trigger slide-in on next frame
      requestAnimationFrame(() => {
        requestAnimationFrame(() => setAnimating(true));
      });
      // Focus first focusable element
      setTimeout(() => {
        drawerRef.current?.querySelector<HTMLElement>("button")?.focus();
      }, 50);
    } else {
      setAnimating(false);
      document.body.style.overflow = "";
      document.removeEventListener("keydown", handleKeyDown);
      // Wait for slide-out animation then unmount
      const timer = setTimeout(() => {
        setVisible(false);
        previousFocus.current?.focus();
      }, 200);
      return () => clearTimeout(timer);
    }
    return () => {
      document.body.style.overflow = "";
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open, handleKeyDown]);

  if (!visible && !open) return null;

  return (
    <>
      {/* Overlay */}
      <div
        className="fixed inset-0 z-40"
        style={{
          backgroundColor: "rgba(0, 0, 0, 0.4)",
          opacity: animating ? 1 : 0,
          transition: "opacity 0.2s ease",
        }}
        onClick={onClose}
      />
      {/* Drawer panel */}
      <div
        ref={drawerRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="fixed top-0 right-0 z-50 h-full w-full md:w-[480px] bg-surface border-l border-border-default overflow-y-auto"
        style={{
          padding: 24,
          transform: animating ? "translateX(0)" : "translateX(100%)",
          transition: "transform 0.2s ease",
        }}
      >
        <div className="flex items-center justify-between mb-6">
          {title && (
            <h2
              id={titleId}
              className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary"
            >
              {title}
            </h2>
          )}
          <button
            onClick={onClose}
            className="ml-auto text-text-tertiary hover:text-text-primary cursor-pointer bg-transparent border-none"
            aria-label="Close drawer"
          >
            <X size={20} />
          </button>
        </div>
        {children}
      </div>
    </>
  );
}

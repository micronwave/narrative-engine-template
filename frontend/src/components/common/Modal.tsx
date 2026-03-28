"use client";

import { useEffect, useRef, useCallback } from "react";
import { X } from "lucide-react";

type Props = {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: string;
  description?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  children?: React.ReactNode;
};

export default function Modal({
  open,
  onClose,
  onConfirm,
  title,
  description,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  destructive = false,
  children,
}: Props) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const previousFocus = useRef<HTMLElement | null>(null);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
        return;
      }
      if (e.key === "Tab" && dialogRef.current) {
        const focusable = dialogRef.current.querySelectorAll<HTMLElement>(
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

  useEffect(() => {
    if (open) {
      previousFocus.current = document.activeElement as HTMLElement;
      document.body.style.overflow = "hidden";
      document.addEventListener("keydown", handleKeyDown);
      requestAnimationFrame(() => {
        dialogRef.current?.querySelector<HTMLElement>("button")?.focus();
      });
    } else {
      document.body.style.overflow = "";
      document.removeEventListener("keydown", handleKeyDown);
      previousFocus.current?.focus();
    }
    return () => {
      document.body.style.overflow = "";
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open, handleKeyDown]);

  if (!open) return null;

  const titleId = "modal-title";

  return (
    <>
      {/* Overlay */}
      <div
        className="fixed inset-0 z-[60]"
        style={{ backgroundColor: "rgba(0, 0, 0, 0.6)" }}
        onClick={onClose}
      />
      {/* Dialog */}
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="fixed inset-0 z-[70] flex items-center justify-center pointer-events-none"
      >
        <div
          className="pointer-events-auto w-full max-w-[400px] mx-4 bg-surface border border-border-default overflow-hidden"
          style={{ borderRadius: 4, padding: 24 }}
        >
          <div className="flex items-start justify-between mb-4">
            <h2
              id={titleId}
              className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-primary"
            >
              {title}
            </h2>
            <button
              onClick={onClose}
              className="text-text-muted hover:text-text-primary transition-colors cursor-pointer bg-transparent border-none"
              aria-label="Close dialog"
            >
              <X size={16} />
            </button>
          </div>

          {description && (
            <p className="text-[13px] text-text-secondary mb-6">{description}</p>
          )}

          {children}

          <div className="flex items-center justify-end gap-3 mt-6">
            <button
              onClick={onClose}
              className="font-sans text-[13px] font-medium text-text-muted bg-transparent border border-border-default rounded-sm px-4 py-2 cursor-pointer hover:text-text-secondary transition-colors"
            >
              {cancelLabel}
            </button>
            <button
              onClick={onConfirm}
              className={`font-sans text-[13px] font-medium text-text-primary border-none rounded-sm px-4 py-2 cursor-pointer transition-all hover:brightness-110 ${
                destructive ? "bg-bearish" : "bg-accent-primary"
              }`}
            >
              {confirmLabel}
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

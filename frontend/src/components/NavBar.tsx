"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Radio, Network, Inbox, BarChart2, ShieldAlert, TrendingUp, MoreHorizontal, X, LayoutDashboard } from "lucide-react";

const PRIMARY_TABS = [
  { href: "/", label: "Signals", icon: Radio },
  { href: "/stocks", label: "Stocks", icon: BarChart2 },
  { href: "/constellation", label: "Analytics", icon: Network },
];

const OVERFLOW_ITEMS = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/signals", label: "Inbox", icon: Inbox },
  { href: "/market-impact", label: "Market Impact", icon: TrendingUp },
  { href: "/manipulation", label: "Manipulation", icon: ShieldAlert },
];

const NAV_ITEMS = [...PRIMARY_TABS, ...OVERFLOW_ITEMS];

export default function NavBar() {
  const pathname = usePathname();
  const [moreOpen, setMoreOpen] = useState(false);

  return (
    <>
      {/* Desktop sidebar — 64px collapsed, 220px on hover */}
      <nav
        className="hidden md:flex fixed top-0 left-0 h-full w-16 hover:w-[220px] flex-col py-5 z-50 group transition-all duration-300 ease-out overflow-hidden"
        style={{
          background: "var(--bg-sidebar)",
          borderRight: "1px solid rgba(56, 62, 71, 0.3)",
        }}
        aria-label="Main navigation"
      >
        {/* Brand */}
        <Link
          href="/"
          className="flex items-center gap-3 px-5 mb-6 shrink-0"
          aria-label="Narrative Intelligence — home"
        >
          <span
            className="font-display shrink-0"
            style={{ color: "var(--accent-primary)", fontWeight: 700, fontSize: "var(--text-heading)" }}
          >
            NI
          </span>
          <span
            className="opacity-0 group-hover:opacity-100 transition-opacity duration-200 whitespace-nowrap font-display"
            style={{ fontSize: "var(--text-small)", fontWeight: 500, color: "var(--text-muted)" }}
          >
            Narrative Intel
          </span>
        </Link>

        {/* Nav items */}
        <div className="flex flex-col gap-1 px-2 flex-1">
          {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
            const active = pathname === href;
            return (
              <Link
                key={href}
                href={href}
                className={`flex items-center gap-3 px-3 py-2.5 transition-all duration-200 ${
                  active
                    ? "text-[var(--text-primary)] bg-[var(--accent-primary-hover)]"
                    : "text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-surface-hover)]"
                }`}
                style={{
                  borderLeft: active ? "2px solid var(--accent-primary)" : "2px solid transparent",
                  borderRadius: "var(--radius-sm)",
                }}
                aria-current={active ? "page" : undefined}
              >
                <Icon size={18} className="shrink-0" />
                <span
                  className="opacity-0 group-hover:opacity-100 transition-opacity duration-200 whitespace-nowrap"
                  style={{ fontSize: "var(--text-small)", fontWeight: 500 }}
                >
                  {label}
                </span>
              </Link>
            );
          })}
        </div>

      </nav>

      {/* Mobile bottom tab bar — 4 primary + More */}
      <nav
        className="md:hidden fixed bottom-0 left-0 right-0 z-50 pb-[env(safe-area-inset-bottom)]"
        style={{
          background: "var(--bg-sidebar)",
          borderTop: "1px solid rgba(56, 62, 71, 0.3)",
        }}
        aria-label="Main navigation"
      >
        <div className="flex items-center justify-around h-[56px]">
          {PRIMARY_TABS.map(({ href, label, icon: Icon }) => {
            const active = pathname === href;
            return (
              <Link
                key={href}
                href={href}
                className={`flex flex-col items-center gap-1 px-2 py-1.5 transition-colors duration-200 ${
                  active ? "text-[var(--text-primary)]" : "text-[var(--text-muted)]"
                }`}
                aria-current={active ? "page" : undefined}
              >
                <Icon size={20} />
                <span style={{ fontSize: 10, fontWeight: 500 }}>{label}</span>
                {active && (
                  <span className="w-1 h-1 rounded-full" style={{ background: "var(--accent-primary)" }} />
                )}
              </Link>
            );
          })}
          {/* More button */}
          <button
            onClick={() => setMoreOpen(true)}
            className="flex flex-col items-center gap-1 px-2 py-1.5 transition-colors duration-200 text-[var(--text-muted)] bg-transparent border-none cursor-pointer"
            aria-label="More navigation options"
          >
            <MoreHorizontal size={20} />
            <span style={{ fontSize: 10, fontWeight: 500 }}>More</span>
          </button>
        </div>
      </nav>

      {/* More bottom sheet */}
      {moreOpen && (
        <>
          <div
            className="md:hidden fixed inset-0 z-40"
            style={{ background: "rgba(0, 0, 0, 0.4)" }}
            onClick={() => setMoreOpen(false)}
          />
          <div
            className="md:hidden fixed bottom-0 left-0 right-0 z-50 pb-[env(safe-area-inset-bottom)]"
            style={{
              background: "var(--bg-surface)",
              borderTop: "1px solid rgba(56, 62, 71, 0.3)",
              borderRadius: "2px 2px 0 0",
            }}
          >
            <div className="flex items-center justify-between px-4 py-3">
              <span className="font-display text-[13px] font-semibold text-text-primary">More</span>
              <button
                onClick={() => setMoreOpen(false)}
                className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer p-1 transition-colors"
                aria-label="Close"
              >
                <X size={18} />
              </button>
            </div>
            <div className="flex flex-col px-2 pb-4">
              {OVERFLOW_ITEMS.map(({ href, label, icon: Icon }) => {
                const active = pathname === href;
                return (
                  <Link
                    key={href}
                    href={href}
                    onClick={() => setMoreOpen(false)}
                    className={`flex items-center gap-3 px-3 py-3 transition-colors duration-200 ${
                      active
                        ? "text-[var(--accent-primary)]"
                        : "text-[var(--text-muted)] hover:text-[var(--text-primary)]"
                    }`}
                  >
                    <Icon size={18} />
                    <span style={{ fontSize: 13, fontWeight: 500 }}>{label}</span>
                  </Link>
                );
              })}
            </div>
          </div>
        </>
      )}
    </>
  );
}

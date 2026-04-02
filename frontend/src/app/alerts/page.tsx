"use client";

import { useEffect, useState, useCallback } from "react";
import { Bell, Trash2, CheckCheck, Plus, X, ToggleLeft, ToggleRight } from "lucide-react";
import {
  fetchAlerts,
  fetchAlertRules,
  fetchAlertTypes,
  createAlertRule,
  deleteAlertRule,
  toggleAlertRule,
  markAlertRead,
  markAllAlertsRead,
  type AlertNotification,
  type AlertRule,
} from "@/lib/api";
import { useAlerts } from "@/contexts/AlertContext";

type Tab = "notifications" | "rules";

const TARGET_TYPE_FOR_RULE: Record<string, string> = {
  ns_above: "narrative",
  ns_below: "narrative",
  mutation: "narrative",
  stage_change: "narrative",
  catalyst: "narrative",
  new_narrative: "ticker",
  price_above: "ticker",
  price_below: "ticker",
  pct_change: "ticker",
  rsi_overbought: "ticker",
  rsi_oversold: "ticker",
  macd_crossover: "ticker",
  sentiment_spike: "ticker",
  portfolio_drawdown: "portfolio",
};

const THRESHOLD_LABEL: Record<string, string> = {
  ns_above: "Ns threshold",
  ns_below: "Ns threshold",
  price_above: "Price ($)",
  price_below: "Price ($)",
  pct_change: "Change (%)",
  rsi_overbought: "RSI level (default 70)",
  rsi_oversold: "RSI level (default 30)",
  macd_crossover: "Threshold (unused)",
  sentiment_spike: "Std deviations",
  portfolio_drawdown: "Drawdown (%)",
};

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function NotificationRow({
  n,
  onMarkRead,
}: {
  n: AlertNotification;
  onMarkRead: (id: string) => void;
}) {
  return (
    <div
      className={`flex items-start gap-3 py-3 px-4 transition-colors ${
        n.is_read ? "opacity-60" : "bg-inset"
      }`}
      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
    >
      <Bell size={14} className="mt-0.5 shrink-0 text-accent-text" />
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between gap-2">
          <span className="text-[13px] font-semibold text-text-primary truncate">{n.title}</span>
          <span className="font-mono text-[10px] text-text-muted shrink-0">{timeAgo(n.created_at)}</span>
        </div>
        <p className="font-mono text-[11px] text-text-secondary mt-0.5">{n.message}</p>
        {n.link && (
          <a
            href={n.link}
            className="font-mono text-[10px] text-accent-text hover:text-text-primary transition-colors mt-1 inline-block"
          >
            View →
          </a>
        )}
      </div>
      {!n.is_read && (
        <button
          onClick={() => onMarkRead(n.id)}
          className="shrink-0 text-[10px] font-mono text-text-muted hover:text-text-primary transition-colors px-2 py-1 border border-[var(--bg-border)] rounded-sm"
        >
          Mark read
        </button>
      )}
    </div>
  );
}

function CreateRuleModal({
  ruleTypes,
  onClose,
  onCreated,
}: {
  ruleTypes: Record<string, string>;
  onClose: () => void;
  onCreated: () => void;
}) {
  const typeKeys = Object.keys(ruleTypes);
  const [ruleType, setRuleType] = useState(typeKeys[0] || "ns_above");
  const [targetId, setTargetId] = useState("");
  const [threshold, setThreshold] = useState("0");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const targetType = TARGET_TYPE_FOR_RULE[ruleType] || "ticker";
  const needsThreshold = ruleType in THRESHOLD_LABEL;
  const needsTarget = targetType !== "portfolio";

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      await createAlertRule(ruleType, targetType, targetId, parseFloat(threshold) || 0);
      onCreated();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create rule");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div
        className="bg-surface border border-[var(--bg-border)] rounded-sm w-full max-w-sm mx-4 p-5"
        style={{ boxShadow: "0 4px 32px rgba(0,0,0,0.5)" }}
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-[14px] font-semibold text-text-primary">Create Alert Rule</h3>
          <button onClick={onClose} className="text-text-muted hover:text-text-primary transition-colors">
            <X size={16} />
          </button>
        </div>

        <div className="flex flex-col gap-3">
          <div>
            <label className="label-micro mb-1 block">Rule Type</label>
            <select
              value={ruleType}
              onChange={(e) => setRuleType(e.target.value)}
              className="w-full bg-inset border border-[var(--bg-border)] rounded-sm font-mono text-[12px] text-text-primary px-2 py-1.5"
            >
              {typeKeys.map((k) => (
                <option key={k} value={k}>
                  {ruleTypes[k] || k}
                </option>
              ))}
            </select>
          </div>

          {needsTarget && (
            <div>
              <label className="label-micro mb-1 block">
                {targetType === "narrative" ? "Narrative ID" : "Ticker"}
              </label>
              <input
                value={targetId}
                onChange={(e) => setTargetId(e.target.value.toUpperCase())}
                placeholder={targetType === "narrative" ? "e.g. nar-001" : "e.g. AAPL"}
                className="w-full bg-inset border border-[var(--bg-border)] rounded-sm font-mono text-[12px] text-text-primary px-2 py-1.5 placeholder:text-text-muted"
              />
            </div>
          )}

          {needsThreshold && (
            <div>
              <label className="label-micro mb-1 block">{THRESHOLD_LABEL[ruleType]}</label>
              <input
                type="number"
                value={threshold}
                onChange={(e) => setThreshold(e.target.value)}
                className="w-full bg-inset border border-[var(--bg-border)] rounded-sm font-mono text-[12px] text-text-primary px-2 py-1.5"
              />
            </div>
          )}

          {error && (
            <p className="font-mono text-[11px] text-bearish">{error}</p>
          )}

          <div className="flex gap-2 mt-1">
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex-1 bg-accent-muted text-accent-text font-mono text-[12px] py-1.5 rounded-sm hover:opacity-80 transition-opacity disabled:opacity-50"
            >
              {saving ? "Saving…" : "Create Rule"}
            </button>
            <button
              onClick={onClose}
              className="px-3 border border-[var(--bg-border)] text-text-muted font-mono text-[12px] py-1.5 rounded-sm hover:text-text-primary transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function RuleRow({
  rule,
  onDelete,
  onToggle,
}: {
  rule: AlertRule;
  onDelete: (id: string) => void;
  onToggle: (id: string) => void;
}) {
  const enabled = Boolean(rule.enabled);
  return (
    <div
      className="flex items-center gap-3 py-3 px-4"
      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-mono text-[11px] text-accent-text bg-accent-muted px-1.5 py-0.5 rounded-sm">
            {rule.rule_type}
          </span>
          {rule.target_id && (
            <span className="font-mono text-[11px] text-text-secondary">
              {rule.target_id}
            </span>
          )}
          {rule.threshold !== null && rule.threshold !== 0 && (
            <span className="font-mono text-[11px] text-text-muted">
              @ {rule.threshold}
            </span>
          )}
        </div>
        <p className="font-mono text-[10px] text-text-muted mt-0.5 capitalize">{rule.target_type}</p>
      </div>
      <button
        onClick={() => onToggle(rule.id)}
        className="shrink-0 text-text-muted hover:text-text-primary transition-colors"
        title={enabled ? "Disable rule" : "Enable rule"}
      >
        {enabled ? <ToggleRight size={18} className="text-bullish" /> : <ToggleLeft size={18} />}
      </button>
      <button
        onClick={() => onDelete(rule.id)}
        className="shrink-0 text-text-muted hover:text-bearish transition-colors"
        title="Delete rule"
      >
        <Trash2 size={14} />
      </button>
    </div>
  );
}

export default function AlertsPage() {
  const [tab, setTab] = useState<Tab>("notifications");
  const [notifications, setNotifications] = useState<AlertNotification[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [ruleTypes, setRuleTypes] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const { refresh: refreshCount } = useAlerts();

  const loadNotifications = useCallback(() => {
    setLoading(true);
    fetchAlerts().then(setNotifications).catch(() => {}).finally(() => setLoading(false));
  }, []);

  const loadRules = useCallback(() => {
    fetchAlertRules().then(setRules).catch(() => {});
  }, []);

  useEffect(() => {
    fetchAlertTypes().then(setRuleTypes).catch(() => {});
    loadNotifications();
    loadRules();
  }, [loadNotifications, loadRules]);

  const handleMarkRead = useCallback((id: string) => {
    markAlertRead(id)
      .then(() => {
        setNotifications((prev) => prev.map((n) => n.id === id ? { ...n, is_read: 1 } : n));
        refreshCount();
      })
      .catch(() => {});
  }, [refreshCount]);

  const handleMarkAllRead = useCallback(() => {
    markAllAlertsRead()
      .then(() => {
        setNotifications((prev) => prev.map((n) => ({ ...n, is_read: 1 })));
        refreshCount();
      })
      .catch(() => {});
  }, [refreshCount]);

  const handleDeleteRule = useCallback((id: string) => {
    deleteAlertRule(id).then(() => setRules((prev) => prev.filter((r) => r.id !== id))).catch(() => {});
  }, []);

  const handleToggleRule = useCallback((id: string) => {
    toggleAlertRule(id)
      .then((res) => {
        setRules((prev) => prev.map((r) => r.id === id ? { ...r, enabled: res.enabled ? 1 : 0 } : r));
      })
      .catch(() => {});
  }, []);

  const unread = notifications.filter((n) => !n.is_read).length;

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1
              className="text-[22px] font-semibold text-text-primary font-display"
              style={{ letterSpacing: "-0.01em" }}
            >
              Alerts
            </h1>
            <p className="font-mono text-[11px] text-text-muted mt-1">
              Notifications and alert rule management
            </p>
          </div>
        </div>

        {/* Tab selector */}
        <div className="flex gap-1 mb-4 p-1 bg-inset border border-[var(--bg-border)] rounded-sm w-fit" data-testid="alert-tabs">
          {(["notifications", "rules"] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`font-mono text-[11px] uppercase tracking-[0.05em] px-4 py-1.5 rounded-sm transition-colors ${
                tab === t
                  ? "bg-surface text-text-primary"
                  : "text-text-muted hover:text-text-primary"
              }`}
            >
              {t === "notifications" ? `Notifications${unread > 0 ? ` (${unread})` : ""}` : "Rules"}
            </button>
          ))}
        </div>

        {/* Notifications tab */}
        {tab === "notifications" && (
          <div className="bg-surface border border-[var(--bg-border)] rounded-sm" data-testid="notification-list">
            <div className="flex items-center justify-between px-4 py-3" style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}>
              <span className="font-mono text-[11px] uppercase tracking-[0.05em] text-text-secondary">
                {notifications.length} notification{notifications.length !== 1 ? "s" : ""}
              </span>
              {unread > 0 && (
                <button
                  onClick={handleMarkAllRead}
                  className="flex items-center gap-1.5 font-mono text-[11px] text-text-muted hover:text-text-primary transition-colors"
                >
                  <CheckCheck size={13} />
                  Mark all read
                </button>
              )}
            </div>

            {loading ? (
              <div className="py-8 text-center font-mono text-[12px] text-text-muted">Loading…</div>
            ) : notifications.length === 0 ? (
              <div className="py-8 text-center">
                <Bell size={24} className="mx-auto mb-2 text-text-tertiary" />
                <p className="font-mono text-[12px] text-text-muted">No notifications yet.</p>
                <p className="font-mono text-[11px] text-text-muted mt-1">Create alert rules to receive notifications.</p>
              </div>
            ) : (
              notifications.map((n) => (
                <NotificationRow key={n.id} n={n} onMarkRead={handleMarkRead} />
              ))
            )}
          </div>
        )}

        {/* Rules tab */}
        {tab === "rules" && (
          <div className="bg-surface border border-[var(--bg-border)] rounded-sm" data-testid="rules-management">
            <div className="flex items-center justify-between px-4 py-3" style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}>
              <span className="font-mono text-[11px] uppercase tracking-[0.05em] text-text-secondary">
                {rules.length} rule{rules.length !== 1 ? "s" : ""}
              </span>
              <button
                onClick={() => setShowCreateModal(true)}
                className="flex items-center gap-1.5 font-mono text-[11px] text-accent-text hover:opacity-80 transition-opacity border border-[var(--bg-border)] px-2 py-1 rounded-sm"
              >
                <Plus size={12} />
                Create Rule
              </button>
            </div>

            {rules.length === 0 ? (
              <div className="py-8 text-center">
                <p className="font-mono text-[12px] text-text-muted">No alert rules configured.</p>
                <button
                  onClick={() => setShowCreateModal(true)}
                  className="mt-3 font-mono text-[11px] text-accent-text hover:opacity-80 transition-opacity"
                >
                  + Create your first rule
                </button>
              </div>
            ) : (
              rules.map((r) => (
                <RuleRow
                  key={r.id}
                  rule={r}
                  onDelete={handleDeleteRule}
                  onToggle={handleToggleRule}
                />
              ))
            )}
          </div>
        )}
      </div>

      {showCreateModal && (
        <CreateRuleModal
          ruleTypes={ruleTypes}
          onClose={() => setShowCreateModal(false)}
          onCreated={() => {
            loadRules();
            loadNotifications();
          }}
        />
      )}
    </main>
  );
}

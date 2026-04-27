// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type TimeseriesPoint = {
  date: string;
  value: number;
};

export type VisibleNarrative = {
  id: string;
  name: string;
  descriptor: string;
  velocity_summary: string;
  entropy: number | null;
  saturation: number;
  velocity_timeseries: TimeseriesPoint[];
  signals: string[];
  catalysts: string[];
  mutations: string[];
  stage?: string;
  burst_velocity?: { rate: number; baseline: number; ratio: number; is_burst: boolean } | null;
  topic_tags?: string[];
  entity_tags?: string[];
  source_stats?: { total: number; news: number; research: number; filings: number; other: number };
  last_evidence_at?: string;
  signal_direction?: "bullish" | "bearish" | "neutral" | null;
  signal_confidence?: number | null;
  signal_certainty?: "speculative" | "rumored" | "expected" | "confirmed" | null;
  signal_catalyst_type?: string | null;
  human_review_required?: boolean;
  blurred: false;
};

export type Narrative = VisibleNarrative;

export type TickerItem = {
  id?: string | null; // optional — added in C4 for click navigation
  name: string;
  velocity_summary: string;
};

export type SubscriptionStatus = {
  user_id: string;
  subscribed: boolean;
};

export type Source = {
  id: string;
  name: string;
  type: string;
  url: string;
  credibility_score: number;
};

export type Signal = {
  id: string;
  narrative_id: string;
  headline: string;
  source: Source;
  timestamp: string;
  sentiment: number;
  coordination_flag: boolean;
};

export type Catalyst = {
  id: string;
  narrative_id: string;
  description: string;
  timestamp: string;
  impact_score: number;
};

export type Mutation = {
  id: string;
  narrative_id: string;
  from_state: string;
  to_state: string;
  timestamp: string;
  trigger: string;
  description: string;
  mutation_type?: string;
  magnitude?: number;
};

export type EntropyScore = {
  narrative_id: string;
  score: number | null;
  components: {
    source_diversity: number;
    temporal_spread: number;
    sentiment_variance: number;
  };
};

// ---------------------------------------------------------------------------
// D1 — Asset Class Association Model types
// ---------------------------------------------------------------------------

export type AssetClass = {
  id: string;
  name: string;
  type: "sector" | "commodity" | "currency" | "index" | "crypto";
  description: string;
};

export type TrackedSecurity = {
  id: string;
  symbol: string;
  name: string;
  asset_class_id: string;
  exchange: string;
  current_price: number | null;
  price_change_24h: number | null;
  narrative_impact_score: number;
};

export type NarrativeAsset = {
  id: string;
  narrative_id: string;
  asset_class_id: string;
  exposure_score: number;
  direction: "bullish" | "bearish" | "mixed" | "uncertain";
  rationale: string;
  asset_class: AssetClass;
  securities: TrackedSecurity[];
};

export type SentimentData = {
  mean: number;
  min: number;
  max: number;
  std: number;
  count: number;
  polarization_label: string;
};

export type NarrativeSignal = {
  direction: "bullish" | "bearish" | "neutral";
  confidence: number;
  timeframe: "immediate" | "near_term" | "long_term" | "unknown";
  magnitude: "incremental" | "significant" | "transformative";
  certainty: "speculative" | "rumored" | "expected" | "confirmed";
  key_actors: string[];
  affected_sectors: string[];
  catalyst_type: "earnings" | "regulatory" | "geopolitical" | "macro" | "corporate" | "unknown";
};

export type CoordinationEvent = {
  id?: string;
  narrative_id?: string;
  event_type?: string;
  detected_at?: string;
  source_domains?: string;
  similarity_score?: number;
  description?: string;
};

export type CoordinationData = {
  flags: number;
  is_coordinated: boolean;
  events: CoordinationEvent[];
};

export type CorrelationPair = {
  narrative_id: string;
  narrative_name: string;
  ticker: string;
  correlation: number;
  p_value: number;
  n_observations: number;
  is_significant: boolean;
  interpretation: string;
  lead_days: number;
};

export type SourceBreakdown = {
  domain: string;
  count: number;
  percentage: number;
  category: string;
  latest_at: string;
};

export type BufferStatus = {
  pending: number;
  clustered: number;
  total: number;
};

export type NarrativeDetail = {
  id: string;
  name: string;
  descriptor: string;
  velocity_summary: string;
  entropy: number | null;
  saturation: number;
  velocity_timeseries: TimeseriesPoint[];
  signals: Signal[];
  catalysts: Catalyst[];
  mutations: Mutation[];
  entropy_detail: EntropyScore;
  blurred: false;
  assets?: NarrativeAsset[];
  stage?: string;
  entity_tags?: string[];
  source_stats?: { total: number; news: number; research: number; filings: number; other: number };
  last_evidence_at?: string;
  sonnet_analysis?: string | null;
  sentiment?: SentimentData | null;
  signal?: NarrativeSignal | null;
  coordination?: CoordinationData | null;
  ns_score?: number;
  document_count?: number;
  cross_source_score?: number;
  polarization?: number;
  topic_tags?: string[];
  burst_velocity?: { ratio: number; is_burst: boolean; label: string } | null;
};

export type InvestigationCredit = {
  user_id: string;
  balance: number;
  total_purchased: number;
  total_used: number;
};

export type ConstellationNode = {
  id: string;
  name: string;
  type: "narrative" | "catalyst";
  entropy?: number | null;
  /** Catalyst nodes only — short description of the catalyst event */
  description?: string | null;
  /** Catalyst nodes only — impact score 0–1 */
  impact_score?: number | null;
};

export type ConstellationEdge = {
  source: string;
  target: string;
  weight: number;
  label: string;
};

export type ConstellationData = {
  nodes: ConstellationNode[];
  edges: ConstellationEdge[];
};

// ---------------------------------------------------------------------------
// Auth token management — module-level token for automatic injection
// ---------------------------------------------------------------------------

let _authToken: string | null = null;

/** Called by AuthContext whenever the token changes. */
export function setApiToken(token: string | null): void {
  _authToken = token;
}

/** Returns the current module-level auth token. */
export function getApiToken(): string | null {
  return _authToken;
}

/** Builds headers with auth token injected. Extra headers override defaults. */
function authHeaders(extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = {};
  if (_authToken) {
    headers["x-auth-token"] = _authToken;
  }
  if (extra) {
    Object.assign(headers, extra);
  }
  return headers;
}

// ---------------------------------------------------------------------------
// Fetch helpers — always call relative /api/* paths (proxied in dev)
// ---------------------------------------------------------------------------

export async function fetchNarratives(): Promise<Narrative[]> {
  const res = await fetch("/api/narratives", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narratives fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchTicker(): Promise<TickerItem[]> {
  const res = await fetch("/api/ticker", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`ticker fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeDetail(id: string): Promise<NarrativeDetail> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative detail fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchConstellation(): Promise<ConstellationData> {
  const res = await fetch("/api/constellation", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`constellation fetch failed: ${res.status}`);
  return res.json();
}

export type DashboardLayoutWidget = {
  id: string;
  type: string;
  title: string;
};

export type DashboardLayout = {
  widgets: DashboardLayoutWidget[];
  grid: Record<string, Array<{ i: string; x: number; y: number; w: number; h: number }>>;
};

export async function fetchDashboardLayout(): Promise<DashboardLayout> {
  const res = await fetch("/api/dashboard/layout", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`dashboard layout fetch failed: ${res.status}`);
  return res.json();
}

export async function saveDashboardLayout(layout: DashboardLayout): Promise<{ status: string }> {
  const res = await fetch("/api/dashboard/layout", {
    method: "PUT",
    headers: authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(layout),
    credentials: "include",
  });
  if (!res.ok) throw new Error(`dashboard layout save failed: ${res.status}`);
  return res.json();
}

export async function exportNarrative(id: string, token?: string | null): Promise<Blob> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}/export`, {
    method: "POST",
    headers: token ? authHeaders({ "x-auth-token": token }) : authHeaders(),
    credentials: "include",
  });
  if (!res.ok) throw new Error(`export failed: ${res.status}`);
  return res.blob();
}

export async function fetchSignals(): Promise<Signal[]> {
  const res = await fetch("/api/signals", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`signals fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// D1 API helpers
// ---------------------------------------------------------------------------

export async function fetchNarrativeAssets(id: string): Promise<NarrativeAsset[]> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}/assets`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative assets fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchAssetClasses(): Promise<AssetClass[]> {
  const res = await fetch("/api/asset-classes", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`asset classes fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchSecurities(): Promise<TrackedSecurity[]> {
  const res = await fetch("/api/securities", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`securities fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// D3 — Stocks with Narrative Impact Scores
// ---------------------------------------------------------------------------

export type StockDetail = TrackedSecurity & {
  narratives: {
    narrative_id: string;
    narrative_name: string;
    exposure_score: number;
    direction: "bullish" | "bearish" | "mixed" | "uncertain";
  }[];
};

export async function fetchStocks(params?: {
  sort_by?: string;
  sort_order?: string;
  asset_class?: string;
  min_impact?: number;
}): Promise<TrackedSecurity[]> {
  const query = new URLSearchParams();
  if (params?.sort_by) query.set("sort_by", params.sort_by);
  if (params?.sort_order) query.set("sort_order", params.sort_order);
  if (params?.asset_class) query.set("asset_class", params.asset_class);
  if (params?.min_impact !== undefined) query.set("min_impact", String(params.min_impact));
  const qs = query.toString();
  const res = await fetch(`/api/stocks${qs ? `?${qs}` : ""}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`stocks fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchStockDetail(symbol: string): Promise<StockDetail> {
  const res = await fetch(`/api/stocks/${encodeURIComponent(symbol)}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`stock detail fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// D4 — Manipulation/Coordination Detection
// ---------------------------------------------------------------------------

export type ManipulationIndicator = {
  id: string;
  narrative_id: string;
  indicator_type:
    | "coordinated_amplification"
    | "astroturfing"
    | "bot_network"
    | "sockpuppet_cluster"
    | "temporal_spike"
    | "source_concentration";
  confidence: number;
  detected_at: string;
  evidence_summary: string;
  flagged_signals: string[];
  status: "active" | "dismissed" | "confirmed" | "under_review";
};

export type ManipulationNarrative = {
  id: string;
  name: string;
  descriptor: string;
  entropy: number | null;
  velocity_summary: string;
  manipulation_indicators: ManipulationIndicator[];
};

export async function fetchManipulation(params?: {
  indicator_type?: string;
  min_confidence?: number;
  status?: string;
}): Promise<ManipulationNarrative[]> {
  const query = new URLSearchParams();
  if (params?.indicator_type) query.set("indicator_type", params.indicator_type);
  if (params?.min_confidence !== undefined)
    query.set("min_confidence", String(params.min_confidence));
  if (params?.status) query.set("status", params.status);
  const qs = query.toString();
  const res = await fetch(`/api/manipulation${qs ? `?${qs}` : ""}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`manipulation fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeManipulation(
  id: string
): Promise<ManipulationIndicator[]> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}/manipulation`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative manipulation fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// F3 — Pre-Earnings Intelligence Brief
// ---------------------------------------------------------------------------

export type BriefNarrative = {
  id: string;
  name: string;
  stage: string;
  velocity_windowed: number;
  entropy: number | null;
  entropy_interpretation: string;
  burst_velocity: { ratio: number; is_burst: boolean } | null;
  coordination_flags: number;
  exposure_score: number;
  direction: string;
  days_active: number;
  signal_count: number;
  top_signals: { headline: string; source: string; timestamp: string }[];
};

export type RiskSummary = {
  coordination_detected: boolean;
  highest_burst_ratio: number;
  dominant_direction: string;
  narrative_count: number;
  avg_entropy: number;
  entropy_assessment: string;
};

export type TickerBrief = {
  ticker: string;
  security: TrackedSecurity | null;
  narratives: BriefNarrative[];
  risk_summary: RiskSummary;
  generated_at: string;
};

export async function fetchBrief(ticker: string): Promise<TickerBrief> {
  const res = await fetch(`/api/brief/${encodeURIComponent(ticker)}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`brief fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// F5 — Historical Snapshot API + Price Data
// ---------------------------------------------------------------------------

export type NarrativeSnapshot = {
  date: string;
  velocity: number | null;
  entropy: number | null;
  ns_score: number | null;
  document_count: number | null;
  lifecycle_stage: string | null;
  linked_assets: string[];
  burst_ratio: number | null;
};

export type OHLCVBar = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  change_pct: number;
};

export type PriceHistoryResponse = {
  symbol: string;
  data: OHLCVBar[];
  available: boolean;
};

export async function fetchNarrativeHistory(
  id: string,
  days: number = 30
): Promise<NarrativeSnapshot[]> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}/history?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative history fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchPriceHistory(
  symbol: string,
  days: number = 30,
  interval: string = "1d"
): Promise<PriceHistoryResponse> {
  const params = new URLSearchParams({ days: String(days), interval });
  const res = await fetch(`/api/ticker/${encodeURIComponent(symbol)}/price-history?${params}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`price history fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Part B — Signal endpoints
// ---------------------------------------------------------------------------

export async function fetchNarrativeSignal(id: string): Promise<{ narrative_id: string; signal: NarrativeSignal | null }> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(id)}/signal`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative signal fetch failed: ${res.status}`);
  return res.json();
}

export type SignalLeaderboardEntry = {
  narrative_id: string;
  name: string;
  stage: string | null;
  direction: "bullish" | "bearish" | "neutral";
  confidence: number;
  magnitude: string | null;
  certainty: string | null;
  catalyst_type: string | null;
  signal_strength: number;
};

export async function fetchSignalLeaderboard(limit: number = 50): Promise<SignalLeaderboardEntry[]> {
  const res = await fetch(`/api/signals/leaderboard?limit=${limit}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`signal leaderboard fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// F6 — Velocity-Price Correlation
// ---------------------------------------------------------------------------

export type CorrelationResult = {
  correlation: number;
  p_value: number;
  n_observations: number;
  is_significant: boolean;
  lead_days: number;
  interpretation: string;
  narrative_id: string;
  ticker: string;
};

export async function fetchCorrelation(
  narrativeId: string,
  ticker: string,
  leadDays: number = 1
): Promise<CorrelationResult> {
  const res = await fetch(
    `/api/correlations/${encodeURIComponent(narrativeId)}/${encodeURIComponent(ticker)}?lead_days=${leadDays}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!res.ok) throw new Error(`correlation fetch failed: ${res.status}`);
  return res.json();
}

// --- Activity Feed ---

export type ActivityItem = {
  type: "mutation" | "alert" | "system";
  subtype: string;
  timestamp: string;
  title: string;
  message: string;
  link: string;
  metadata: Record<string, unknown>;
};

export async function fetchActivity(limit = 100): Promise<ActivityItem[]> {
  const res = await fetch(`/api/activity?limit=${limit}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`activity fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// V3 Phase 1 — New fetch functions
// ---------------------------------------------------------------------------

export async function fetchNarrativeCoordination(narrativeId: string): Promise<CoordinationData & { narrative_id: string; flag_count: number }> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/coordination`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`coordination fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCoordinationSummary(): Promise<{ total_events: number; events_by_type: Record<string, number>; most_recent: CoordinationEvent | null }> {
  const res = await fetch("/api/coordination/summary", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`coordination summary failed: ${res.status}`);
  return res.json();
}

export async function fetchCorrelationMatrix(leadDays = 1, limit = 20): Promise<{ pairs: CorrelationPair[]; generated_at: number; cached: boolean }> {
  const res = await fetch(`/api/correlations/top?limit=${limit}&lead_days=${leadDays}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`correlation matrix failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeCorrelations(narrativeId: string, leadDays = 1): Promise<CorrelationResult[]> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/correlations?lead_days=${leadDays}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative correlations failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeSources(narrativeId: string): Promise<SourceBreakdown[]> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/sources`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`sources fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchBufferStatus(): Promise<BufferStatus> {
  const res = await fetch("/api/pipeline/buffer", { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`buffer status failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeDocuments(narrativeId: string, limit = 10, offset = 0): Promise<{ items: Record<string, unknown>[]; total: number; limit: number; offset: number }> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/documents?limit=${limit}&offset=${offset}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`documents fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// V3 Phase 2 — Timeline
// ---------------------------------------------------------------------------

export type TimelineEntry = {
  date: string;
  ns_score: number | null;
  velocity: number | null;
  document_count: number | null;
  stage: string | null;
  mutations: { type: string; description: string }[];
};

export async function fetchNarrativeTimeline(narrativeId: string, days = 30): Promise<{ narrative_id: string; timeline: TimelineEntry[] }> {
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/timeline?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`timeline fetch failed: ${res.status}`);
  return res.json();
}

export async function compareNarrativeSnapshots(narrativeId: string, date1: string, date2: string): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({ date1, date2 });
  const res = await fetch(`/api/narratives/${encodeURIComponent(narrativeId)}/compare?${params}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`compare failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// V3 Phase 3 — Earnings
// ---------------------------------------------------------------------------

export type EarningsEntry = {
  ticker: string;
  earnings_date: string;
  days_until: number | null;
};

export async function fetchUpcomingEarnings(days = 14): Promise<EarningsEntry[]> {
  const res = await fetch(`/api/earnings/upcoming?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) return [];
  return res.json();
}

// ---------------------------------------------------------------------------
// Analytics Dashboard
// ---------------------------------------------------------------------------

export type AnalyticsMomentumEntry = {
  narrative_id: string;
  name: string;
  stage: string;
  current_velocity: number;
  momentum_score: number;
  slope: number;
  slope_direction: "accelerating" | "decelerating" | "steady";
  linked_assets: string[];
  burst_active: boolean;
  data_quality: { snapshots_available: number };
};

export type AnalyticsHistorySnapshot = {
  date: string;
  ns_score: number | null;
  velocity: number | null;
  entropy: number | null;
  cohesion: number | null;
  polarization: number | null;
  doc_count: number | null;
  burst_ratio: number | null;
  gap_filled: boolean;
};

export type AnalyticsNarrativeHistory = {
  name: string;
  stage: string;
  history: AnalyticsHistorySnapshot[];
};

export type AnalyticsHistoriesResponse = {
  days: number;
  generated_at: string;
  narratives: Record<string, AnalyticsNarrativeHistory>;
};

export type AnalyticsOverlapNarrative = {
  id: string;
  name: string;
  stage: string;
  ns_score: number;
};

export type AnalyticsOverlapResponse = {
  generated_at: string;
  cached: boolean;
  narratives: AnalyticsOverlapNarrative[];
  matrix: number[][];
};

export type AnalyticsContributingNarrative = {
  narrative_id: string;
  name: string;
  ns_score: number;
  stage: string;
};

export type AnalyticsSectorEntry = {
  name: string;
  narrative_count: number;
  weighted_pressure: number;
  contributing_narratives: AnalyticsContributingNarrative[];
  top_assets: { ticker: string; similarity_score: number }[];
};

export type AnalyticsSectorResponse = {
  generated_at: string;
  sectors: AnalyticsSectorEntry[];
};

export type AnalyticsFunnelTransition = {
  from: string;
  to: string;
  count: number;
  avg_days?: number;
  label?: string;
};

export type AnalyticsFunnelResponse = {
  generated_at: string;
  days: number;
  stage_counts: Record<string, number>;
  transitions: AnalyticsFunnelTransition[];
  avg_lifespan_days: number;
  revival_rate: number;
};

export type AnalyticsLeadTimeDataPoint = {
  narrative_id: string;
  ticker: string;
  lead_days: number | null;
  price_change_pct: number | null;
};

export type AnalyticsLeadTimeBucket = {
  range: string;
  count: number;
};

export type AnalyticsLeadTimeResponse = {
  generated_at: string;
  cached: boolean;
  data_points: AnalyticsLeadTimeDataPoint[];
  histogram_buckets: AnalyticsLeadTimeBucket[];
  median_lead_days: number;
  mean_lead_days: number;
  hit_rate: number;
};

export type AnalyticsCoordinationEvent = {
  detected_at: string | null;
  source_domains: string[];
  similarity_score: number;
};

export type AnalyticsContrarianAsset = {
  ticker: string;
  price_at_detection: number | null;
  price_now: number | null;
  price_change_pct: number | null;
  similarity_score: number;
};

export type AnalyticsContrarianSignal = {
  narrative_id: string;
  name: string;
  ns_score: number;
  stage: string;
  coordination_events: AnalyticsCoordinationEvent[];
  linked_assets: AnalyticsContrarianAsset[];
  velocity_at_detection: number;
  velocity_now: number;
  velocity_sustained: boolean;
};

export type AnalyticsContrarianResponse = {
  generated_at: string;
  cached: boolean;
  signals: AnalyticsContrarianSignal[];
};

export async function fetchMomentumLeaderboard(days = 7): Promise<{ generated_at: string; leaderboard: AnalyticsMomentumEntry[] }> {
  const res = await fetch(`/api/analytics/momentum-leaderboard?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`momentum-leaderboard failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeHistories(days = 30): Promise<AnalyticsHistoriesResponse> {
  const res = await fetch(`/api/analytics/narrative-histories?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative-histories failed: ${res.status}`);
  return res.json();
}

export async function fetchNarrativeOverlap(days = 30): Promise<AnalyticsOverlapResponse> {
  const res = await fetch(`/api/analytics/narrative-overlap?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`narrative-overlap failed: ${res.status}`);
  return res.json();
}

export async function fetchSectorConvergence(days = 30): Promise<AnalyticsSectorResponse> {
  const res = await fetch(`/api/analytics/sector-convergence?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`sector-convergence failed: ${res.status}`);
  return res.json();
}

export async function fetchLifecycleFunnel(days = 30): Promise<AnalyticsFunnelResponse> {
  const res = await fetch(`/api/analytics/lifecycle-funnel?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`lifecycle-funnel failed: ${res.status}`);
  return res.json();
}

export async function fetchLeadTimeDistribution(days = 90, threshold = 2.0): Promise<AnalyticsLeadTimeResponse> {
  const res = await fetch(`/api/analytics/lead-time-distribution?days=${days}&threshold=${threshold}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`lead-time-distribution failed: ${res.status}`);
  return res.json();
}

export async function fetchContrarianSignals(days = 30): Promise<AnalyticsContrarianResponse> {
  const res = await fetch(`/api/analytics/contrarian-signals?days=${days}`, { headers: authHeaders(), credentials: "include" });
  if (!res.ok) throw new Error(`contrarian-signals failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Phase 3 Batch 3 — AI Narrative Deep Analysis
// ---------------------------------------------------------------------------

export type DeepAnalysis = {
  thesis: string;
  key_drivers: string[];
  asset_impact: { asset: string; impact: string }[];
  risk_factors: string[];
  historical_comparison: string | null;
  analyzed_at: string;
  cached: boolean;
};

export async function analyzeNarrative(
  narrativeId: string,
  force = false
): Promise<DeepAnalysis> {
  const res = await fetch(
    `/api/narratives/${encodeURIComponent(narrativeId)}/analyze${force ? "?force=true" : ""}`,
    { method: "POST", headers: authHeaders(), credentials: "include" }
  );
  if (!res.ok) throw new Error(`analyze failed: ${res.status}`);
  return res.json();
}


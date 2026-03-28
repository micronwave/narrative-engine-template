export interface MetricDefinition {
  label: string;
  computation: string;
  interpretation: string;
}

export const METRIC_GLOSSARY: Record<string, MetricDefinition> = {
  ns_score: {
    label: "Narrative Strength",
    computation:
      "Composite of velocity, document count, cohesion, and source diversity. Range 0.0\u20131.5.",
    interpretation:
      "Above 0.8 = strong signal. Below 0.3 = weak/noisy.",
  },
  velocity: {
    label: "Velocity",
    computation:
      "Rate of new document ingestion over a 7-day rolling window.",
    interpretation:
      "Higher = more media/social coverage accumulating.",
  },
  entropy: {
    label: "Source Diversity",
    computation:
      "Shannon entropy over source domains contributing to this narrative.",
    interpretation:
      "High entropy = broad coverage (many sources). Low = concentrated (few sources, potentially coordinated).",
  },
  cohesion: {
    label: "Cohesion",
    computation:
      "Average cosine similarity between document embeddings within the cluster.",
    interpretation:
      "High cohesion = tight, focused narrative. Low = fragmented or merging topics.",
  },
  burst_ratio: {
    label: "Burst Ratio",
    computation:
      "Current document ingestion rate / 7-day average rate.",
    interpretation:
      "3.0+ = SURGE (narrative is exploding). Below 1.0 = slowing.",
  },
  polarization: {
    label: "Polarization",
    computation:
      "Variance of sentiment scores across contributing documents.",
    interpretation:
      "High = contentious (strong bulls and bears). Low = consensus.",
  },
  similarity_score: {
    label: "Asset Similarity",
    computation:
      "Cosine similarity between the asset\u2019s 10-K embedding and the narrative centroid.",
    interpretation:
      "Above 0.7 = strong link. Below 0.4 = weak/tangential.",
  },
  correlation: {
    label: "Correlation (Pearson)",
    computation:
      "Pearson coefficient over daily snapshots with configurable lead time.",
    interpretation:
      "+1.0 = velocity perfectly predicts price movement. \u22121.0 = inverse relationship. 0 = no relationship.",
  },
} as const;

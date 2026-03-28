/**
 * F2 Frontend Tests — Burst Velocity Indicator
 *
 * F2-U1: Burst indicator renders when burst_velocity.is_burst is true
 * F2-U2: Burst indicator not rendered when is_burst is false
 * F2-U3: Burst indicator shows "SURGE" text
 */

import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import NarrativeCard from "../components/NarrativeCard";
import { AuthContext } from "../contexts/AuthContext";
import type { VisibleNarrative } from "../lib/api";

const guestAuth = {
  isSignedIn: false,
  token: null as string | null,
  signIn: jest.fn(),
  signOut: jest.fn(),
};

function makeNarrative(burstVelocity: { rate: number; baseline: number; ratio: number; is_burst: boolean } | null): VisibleNarrative {
  return {
    id: "nar-001",
    name: "Test Narrative",
    descriptor: "A test narrative.",
    velocity_summary: "+5.0% signal velocity over 7d",
    entropy: 0.72,
    saturation: 0.45,
    velocity_timeseries: [],
    signals: [],
    catalysts: [],
    mutations: [],
    stage: "Growing",
    burst_velocity: burstVelocity,
    blurred: false,
  };
}

function renderCard(burstVelocity: { rate: number; baseline: number; ratio: number; is_burst: boolean } | null) {
  return render(
    <AuthContext.Provider value={guestAuth}>
      <NarrativeCard narrative={makeNarrative(burstVelocity)} />
    </AuthContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// F2-U1: Burst indicator renders when is_burst is true
// ---------------------------------------------------------------------------

describe("F2-U1: Burst indicator renders when is_burst is true", () => {
  it("renders burst-indicator when is_burst=true", () => {
    renderCard({ rate: 30, baseline: 10, ratio: 3.0, is_burst: true });
    expect(screen.getByTestId("burst-indicator")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// F2-U2: Burst indicator not rendered when is_burst is false
// ---------------------------------------------------------------------------

describe("F2-U2: Burst indicator not rendered when is_burst is false", () => {
  it("does not render burst-indicator when is_burst=false", () => {
    renderCard({ rate: 10, baseline: 10, ratio: 1.0, is_burst: false });
    expect(screen.queryByTestId("burst-indicator")).not.toBeInTheDocument();
  });

  it("does not render burst-indicator when burst_velocity is null", () => {
    renderCard(null);
    expect(screen.queryByTestId("burst-indicator")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// F2-U3: Burst indicator shows "SURGE" text
// ---------------------------------------------------------------------------

describe("F2-U3: Burst indicator shows SURGE text", () => {
  it("shows SURGE text", () => {
    renderCard({ rate: 50, baseline: 10, ratio: 5.0, is_burst: true });
    expect(screen.getByTestId("burst-indicator")).toHaveTextContent("SURGE");
  });
});

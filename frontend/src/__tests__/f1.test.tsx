/**
 * F1 Frontend Tests — Lifecycle Stage Badges
 *
 * F1-U1: NarrativeCard renders stage badge with data-testid="stage-badge"
 * F1-U2: Stage "Emerging" renders with accent styling
 * F1-U3: Stage "Growing" renders with bullish styling
 * F1-U4: Stage "Mature" renders with alert styling
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

function makeNarrative(stage: string): VisibleNarrative {
  return {
    id: "nar-001",
    name: "Test Narrative",
    descriptor: "A test narrative for stage badge testing.",
    velocity_summary: "+5.0% signal velocity over 7d",
    entropy: 0.72,
    saturation: 0.45,
    velocity_timeseries: [],
    signals: [],
    catalysts: [],
    mutations: [],
    stage,
    blurred: false,
  };
}

function renderCard(stage: string) {
  return render(
    <AuthContext.Provider value={guestAuth}>
      <NarrativeCard narrative={makeNarrative(stage)} />
    </AuthContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// F1-U1: Stage badge renders
// ---------------------------------------------------------------------------

describe("F1-U1: NarrativeCard renders stage badge", () => {
  it("renders a stage badge with data-testid='stage-badge'", () => {
    renderCard("Emerging");
    expect(screen.getByTestId("stage-badge")).toBeInTheDocument();
  });

  it("shows the stage name in lowercase", () => {
    renderCard("Growing");
    expect(screen.getByTestId("stage-badge")).toHaveTextContent("growing");
  });
});

// ---------------------------------------------------------------------------
// F1-U2: Emerging renders with accent styling
// ---------------------------------------------------------------------------

describe("F1-U2: Emerging stage badge styling", () => {
  it("has accent-muted class for Emerging", () => {
    renderCard("Emerging");
    const badge = screen.getByTestId("stage-badge");
    expect(badge.className).toMatch(/accent-muted/);
    expect(badge.className).toMatch(/accent-text/);
  });
});

// ---------------------------------------------------------------------------
// F1-U3: Growing renders with bullish styling
// ---------------------------------------------------------------------------

describe("F1-U3: Growing stage badge styling", () => {
  it("has bullish class for Growing", () => {
    renderCard("Growing");
    const badge = screen.getByTestId("stage-badge");
    expect(badge.className).toMatch(/bullish/);
  });
});

// ---------------------------------------------------------------------------
// F1-U4: Mature renders with alert styling
// ---------------------------------------------------------------------------

describe("F1-U4: Mature stage badge styling", () => {
  it("has alert class for Mature", () => {
    renderCard("Mature");
    const badge = screen.getByTestId("stage-badge");
    expect(badge.className).toMatch(/alert/);
  });
});

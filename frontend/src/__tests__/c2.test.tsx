/**
 * C2 Frontend Tests (updated for D4 — monetization removed)
 * C2-I1: NarrativeCard visible state receives velocity_timeseries and renders sparkline
 * C2-A1: Guest clicks Investigate → onInvestigateClick called (no sign-up gate)
 * C2-A2: Signed-in user clicks Investigate → onInvestigateClick called (opens drawer)
 */

import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";

import { mockPush } from "../__mocks__/next-navigation";

import NarrativeCard from "../components/NarrativeCard";
import { AuthContext } from "../contexts/AuthContext";
import type { VisibleNarrative } from "../lib/api";

const MOCK_TIMESERIES = [
  { date: "2026-03-10", value: 0.58 },
  { date: "2026-03-11", value: 0.62 },
  { date: "2026-03-12", value: 0.67 },
  { date: "2026-03-13", value: 0.71 },
  { date: "2026-03-14", value: 0.74 },
  { date: "2026-03-15", value: 0.80 },
  { date: "2026-03-16", value: 0.86 },
];

const MOCK_VISIBLE: VisibleNarrative = {
  id: "nar-001",
  name: "Semiconductor Reshoring Acceleration",
  descriptor: "US chip manufacturing policy is catalyzing a supply-chain realignment.",
  velocity_summary: "+14.0% signal velocity over 7d",
  entropy: 0.72,
  saturation: 0.45,
  velocity_timeseries: MOCK_TIMESERIES,
  signals: ["sig-001", "sig-002"],
  catalysts: ["cat-001"],
  mutations: ["mut-001"],
  blurred: false,
};

const guestAuth = {
  isSignedIn: false,
  token: null,
  signIn: jest.fn(),
  signOut: jest.fn(),
};

const signedInAuth = {
  isSignedIn: true,
  token: "stub-auth-token",
  signIn: jest.fn(),
  signOut: jest.fn(),
};

function renderWithAuth(
  ui: React.ReactElement,
  auth: typeof guestAuth | typeof signedInAuth
) {
  return render(
    <AuthContext.Provider value={auth}>{ui}</AuthContext.Provider>
  );
}

// C2-I1
describe("C2-I1: NarrativeCard velocity sparkline", () => {
  it("renders data-testid=velocity-sparkline with timeseries data", () => {
    renderWithAuth(<NarrativeCard narrative={MOCK_VISIBLE} />, guestAuth);

    const sparkline = screen.getByTestId("velocity-sparkline");
    expect(sparkline).toBeInTheDocument();

    const tsData = sparkline.getAttribute("data-timeseries");
    expect(tsData).toBeTruthy();
    const parsed = JSON.parse(tsData!);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed).toHaveLength(7);
    expect(parsed[0]).toHaveProperty("date");
    expect(parsed[0]).toHaveProperty("value");
  });
});

// C2-A1
describe("C2-A1: Guest Investigate flow", () => {
  it("calls onInvestigateClick when guest clicks Investigate", () => {
    const mockInvestigate = jest.fn();
    renderWithAuth(
      <NarrativeCard narrative={MOCK_VISIBLE} onInvestigateClick={mockInvestigate} />,
      guestAuth
    );

    const investigateBtn = screen.getByRole("button", { name: /investigate/i });
    expect(investigateBtn).toBeInTheDocument();

    fireEvent.click(investigateBtn);

    expect(mockInvestigate).toHaveBeenCalledWith("nar-001");
    expect(mockPush).not.toHaveBeenCalled();
  });
});

// C2-A2
describe("C2-A2: Signed-in Investigate flow", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("calls onInvestigateClick with narrative id when signed-in user investigates", () => {
    const mockInvestigate = jest.fn();
    renderWithAuth(
      <NarrativeCard
        narrative={MOCK_VISIBLE}
        onInvestigateClick={mockInvestigate}
      />,
      signedInAuth
    );

    const investigateBtn = screen.getByRole("button", { name: /investigate/i });
    expect(investigateBtn).toBeInTheDocument();
    expect(investigateBtn).not.toBeDisabled();

    fireEvent.click(investigateBtn);

    expect(mockInvestigate).toHaveBeenCalledWith("nar-001");
    expect(mockPush).not.toHaveBeenCalled();
  });
});

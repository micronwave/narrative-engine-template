import type { Config } from "jest";

const config: Config = {
  testEnvironment: "jsdom",
  transform: {
    "^.+\\.(ts|tsx)$": [
      "ts-jest",
      {
        tsconfig: {
          jsx: "react-jsx",
        },
      },
    ],
  },
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/src/$1",
    "^next/navigation$": "<rootDir>/src/__mocks__/next-navigation.ts",
    "^next/font/google$": "<rootDir>/src/__mocks__/next-font.ts",
    "^next/link$": "<rootDir>/src/__mocks__/next-link.ts",
    "^lightweight-charts$": "<rootDir>/src/__mocks__/lightweight-charts.ts",
    "^react-grid-layout$": "<rootDir>/src/__mocks__/react-grid-layout.ts",
    "\\.(css|less|sass|scss)$": "<rootDir>/src/__mocks__/fileMock.ts",
  },
  testMatch: ["**/__tests__/**/*.test.(ts|tsx)"],
};

export default config;

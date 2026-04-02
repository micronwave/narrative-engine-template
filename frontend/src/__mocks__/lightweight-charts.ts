// Manual mock for lightweight-charts (ESM-only, incompatible with Jest/jsdom)

export const createChart = jest.fn(() => ({
  addSeries: jest.fn(() => ({ setData: jest.fn() })),
  priceScale: jest.fn(() => ({ applyOptions: jest.fn() })),
  timeScale: jest.fn(() => ({ fitContent: jest.fn() })),
  applyOptions: jest.fn(),
  remove: jest.fn(),
}));

export const CandlestickSeries = {};
export const HistogramSeries = {};
export const LineSeries = {};

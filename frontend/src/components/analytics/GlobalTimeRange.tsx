"use client";

import SegmentedControl from "@/components/common/SegmentedControl";

const TIME_RANGES = ["7d", "14d", "30d", "60d", "90d"];

type Props = {
  value: string;
  onChange: (value: string) => void;
};

export default function GlobalTimeRange({ value, onChange }: Props) {
  return <SegmentedControl options={TIME_RANGES} activeOption={value} onChange={onChange} />;
}

export function parseDays(range: string): number {
  return parseInt(range.replace("d", ""), 10) || 30;
}

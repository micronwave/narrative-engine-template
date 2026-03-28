"use client";

type Props = {
  options: string[];
  activeOption: string;
  onChange: (option: string) => void;
  className?: string;
};

export default function SegmentedControl({ options, activeOption, onChange, className = "" }: Props) {
  return (
    <div className={`inline-flex gap-[2px] ${className}`}>
      {options.map((option) => {
        const isActive = option === activeOption;
        return (
          <button
            key={option}
            onClick={() => onChange(option)}
            className={`font-mono text-[12px] px-3 py-1 rounded-sm transition-all duration-[120ms] ease-linear cursor-pointer focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--accent-primary)] focus-visible:outline-offset-2 ${
              isActive
                ? "bg-accent-primary text-text-primary"
                : "bg-transparent text-text-tertiary hover:text-text-secondary"
            }`}
          >
            {option}
          </button>
        );
      })}
    </div>
  );
}

"use client";

import React, { forwardRef } from "react";

/* ─────────────────────────────────────────────
   Baidu Maps Design System
   Color tokens, components & layout primitives
   inspired by Baidu Maps (百度地图) UI language.
   ───────────────────────────────────────────── */

export const tokens = {
  blue50: "#E8F1FF",
  blue100: "#C4DAFF",
  blue200: "#91B9FF",
  blue400: "#5B9BFF",
  blue500: "#3385FF",
  blue600: "#1A6FE8",
  blue700: "#0D5BD4",
  blue800: "#0848AB",

  red400: "#FF5C5C",
  red500: "#FF3D3D",

  green400: "#34D058",
  green500: "#00C853",

  orange400: "#FF9500",
  orange500: "#FF7A00",

  gray50: "#F7F8FA",
  gray100: "#F0F1F5",
  gray200: "#E4E6EB",
  gray300: "#D1D4DB",
  gray400: "#A8ABB3",
  gray500: "#72757E",
  gray600: "#555860",
  gray700: "#3D3F45",
  gray800: "#1F2024",
  gray900: "#111214",

  white: "#FFFFFF",

  shadow1: "0 2px 8px rgba(0,0,0,0.08)",
  shadow2: "0 4px 16px rgba(0,0,0,0.10)",
  shadow3: "0 8px 32px rgba(0,0,0,0.12)",
  shadowBlue: "0 4px 16px rgba(51,133,255,0.24)",

  radius1: "8px",
  radius2: "12px",
  radius3: "16px",
  radiusFull: "9999px",
} as const;

/* ── Shell ─────────────────────────────────── */

export function AppShell({ children }: { children: React.ReactNode }) {
  return <div className="bd-shell">{children}</div>;
}

/* ── TopBar ────────────────────────────────── */

type TopBarProps = {
  title?: string;
  subtitle?: string;
  left?: React.ReactNode;
  right?: React.ReactNode;
};

export function TopBar({ title, subtitle, left, right }: TopBarProps) {
  return (
    <header className="bd-topbar">
      {left && <div className="bd-topbar-left">{left}</div>}
      <div className="bd-topbar-center">
        {title && <span className="bd-topbar-title">{title}</span>}
        {subtitle && <span className="bd-topbar-subtitle">{subtitle}</span>}
      </div>
      {right && <div className="bd-topbar-right">{right}</div>}
    </header>
  );
}

/* ── SearchBar ─────────────────────────────── */

type SearchBarProps = {
  value?: string;
  placeholder?: string;
  onChange?: (val: string) => void;
  icon?: React.ReactNode;
};

export function SearchBar({ value, placeholder = "搜索", onChange, icon }: SearchBarProps) {
  return (
    <div className="bd-search">
      <span className="bd-search-icon">
        {icon || (
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        )}
      </span>
      <input
        className="bd-search-input"
        type="text"
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange?.(e.target.value)}
      />
    </div>
  );
}

/* ── FloatingPanel ─────────────────────────── */

type FloatingPanelProps = {
  children: React.ReactNode;
  className?: string;
  position?: "top-left" | "top-right" | "bottom-left" | "bottom-right" | "bottom-center";
  glass?: boolean;
  style?: React.CSSProperties;
};

export function FloatingPanel({ children, className = "", position, glass, style }: FloatingPanelProps) {
  const posClass = position ? `bd-float-${position}` : "";
  const glassClass = glass ? "bd-glass" : "";
  return (
    <div className={`bd-float-panel ${posClass} ${glassClass} ${className}`.trim()} style={style}>
      {children}
    </div>
  );
}

/* ── Card ──────────────────────────────────── */

type CardProps = {
  children: React.ReactNode;
  className?: string;
  variant?: "default" | "elevated" | "outlined" | "blue";
  style?: React.CSSProperties;
  onClick?: () => void;
};

export function Card({ children, className = "", variant = "default", style, onClick }: CardProps) {
  return (
    <div className={`bd-card bd-card-${variant} ${className}`.trim()} style={style} onClick={onClick}>
      {children}
    </div>
  );
}

/* ── Button ────────────────────────────────── */

type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost" | "danger" | "outline";
  size?: "sm" | "md" | "lg";
  icon?: React.ReactNode;
  fullWidth?: boolean;
};

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = "primary", size = "md", icon, fullWidth, className = "", children, ...rest }, ref) => {
    return (
      <button
        ref={ref}
        className={`bd-btn bd-btn-${variant} bd-btn-${size} ${fullWidth ? "bd-btn-full" : ""} ${className}`.trim()}
        {...rest}
      >
        {icon && <span className="bd-btn-icon">{icon}</span>}
        {children}
      </button>
    );
  }
);
Button.displayName = "Button";

/* ── FAB (Floating Action Button) ──────────── */

type FABProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "white";
  size?: "sm" | "md";
};

export function FAB({ variant = "white", size = "md", className = "", children, ...rest }: FABProps) {
  return (
    <button className={`bd-fab bd-fab-${variant} bd-fab-${size} ${className}`.trim()} {...rest}>
      {children}
    </button>
  );
}

/* ── Chip / Tag ────────────────────────────── */

type ChipProps = {
  children: React.ReactNode;
  selected?: boolean;
  onClick?: () => void;
  icon?: React.ReactNode;
};

export function Chip({ children, selected, onClick, icon }: ChipProps) {
  return (
    <button className={`bd-chip ${selected ? "bd-chip-selected" : ""}`} onClick={onClick} type="button">
      {icon && <span className="bd-chip-icon">{icon}</span>}
      {children}
    </button>
  );
}

/* ── Badge ─────────────────────────────────── */

type BadgeProps = {
  children: React.ReactNode;
  variant?: "blue" | "green" | "orange" | "red" | "gray";
};

export function Badge({ children, variant = "blue" }: BadgeProps) {
  return <span className={`bd-badge bd-badge-${variant}`}>{children}</span>;
}

/* ── Stat ──────────────────────────────────── */

type StatProps = {
  label: string;
  value: string | number;
  icon?: React.ReactNode;
};

export function Stat({ label, value, icon }: StatProps) {
  return (
    <div className="bd-stat">
      {icon && <div className="bd-stat-icon">{icon}</div>}
      <div className="bd-stat-value">{value}</div>
      <div className="bd-stat-label">{label}</div>
    </div>
  );
}

/* ── ProgressBar ───────────────────────────── */

type ProgressBarProps = {
  current: number;
  total: number;
  variant?: "blue" | "green";
};

export function ProgressBar({ current, total, variant = "blue" }: ProgressBarProps) {
  const pct = total > 0 ? Math.min((current / total) * 100, 100) : 0;
  return (
    <div className="bd-progress">
      <div className={`bd-progress-fill bd-progress-${variant}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

/* ── BottomSheet ───────────────────────────── */

type BottomSheetProps = {
  children: React.ReactNode;
  open?: boolean;
  className?: string;
};

export function BottomSheet({ children, open = true, className = "" }: BottomSheetProps) {
  return (
    <div className={`bd-bottomsheet ${open ? "bd-bottomsheet-open" : ""} ${className}`.trim()}>
      <div className="bd-bottomsheet-handle" />
      {children}
    </div>
  );
}

/* ── Divider ───────────────────────────────── */

export function Divider({ spacing = 16 }: { spacing?: number }) {
  return <hr className="bd-divider" style={{ margin: `${spacing}px 0` }} />;
}

/* ── AudioCard (styled audio player) ───────── */

type AudioCardProps = {
  src: string;
  transcript?: string;
  city?: string;
  description?: string;
  showDetails?: boolean;
  result?: { distanceKm: number; roundScore: number } | null;
};

export function AudioCard({ src, transcript, city, description, showDetails, result }: AudioCardProps) {
  return (
    <Card variant="default" className="bd-audio-card">
      <div className="bd-audio-header">
        <svg width="20" height="20" viewBox="0 0 24 24" fill={tokens.blue500} stroke="none">
          <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3Z" />
          <path d="M19 10v2a7 7 0 0 1-14 0v-2" fill="none" stroke={tokens.blue500} strokeWidth="2" />
          <line x1="12" y1="19" x2="12" y2="23" stroke={tokens.blue500} strokeWidth="2" />
          <line x1="8" y1="23" x2="16" y2="23" stroke={tokens.blue500} strokeWidth="2" />
        </svg>
        <span className="bd-audio-label">方言音频 Dialect Audio</span>
      </div>
      <audio controls preload="metadata" src={src} className="bd-audio-player" />
      {transcript && (
        <div className="bd-audio-transcript">
          <span className="bd-audio-hint-label">提示 Hint</span>
          <span>{transcript}</span>
        </div>
      )}
      {showDetails && (
        <div className="bd-audio-details">
          {city && (
            <div className="bd-audio-detail-row">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={tokens.green500} strokeWidth="2">
                <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z" />
                <circle cx="12" cy="10" r="3" />
              </svg>
              <span>{city}</span>
            </div>
          )}
          {description && (
            <div className="bd-audio-detail-row">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={tokens.gray500} strokeWidth="2">
                <circle cx="12" cy="12" r="10" />
                <line x1="12" y1="16" x2="12" y2="12" />
                <line x1="12" y1="8" x2="12.01" y2="8" />
              </svg>
              <span>{description}</span>
            </div>
          )}
          {result && (
            <div className="bd-audio-result">
              <Badge variant={result.roundScore >= 3000 ? "green" : result.roundScore >= 1000 ? "orange" : "red"}>
                {result.roundScore} 分
              </Badge>
              <span className="bd-audio-distance">{result.distanceKm.toFixed(1)} km</span>
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

/* ── MapContainer ──────────────────────────── */

type MapContainerProps = {
  mapRef: React.Ref<HTMLDivElement>;
  ready?: boolean;
  error?: string;
  notice?: string;
};

export function MapContainer({ mapRef, ready, error, notice }: MapContainerProps) {
  return (
    <div className="bd-map-wrapper">
      <div ref={mapRef as React.LegacyRef<HTMLDivElement>} className="bd-map" />
      {!ready && !error && (
        <div className="bd-map-loading">
          <div className="bd-spinner" />
          <span>地图加载中...</span>
        </div>
      )}
      {error && <div className="bd-map-error">{error}</div>}
      {notice && (
        <div className="bd-map-notice">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
            <line x1="12" y1="9" x2="12" y2="13" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
          {notice}
        </div>
      )}
    </div>
  );
}

/* ── ScoreDisplay (final screen) ───────────── */

type ScoreDisplayProps = {
  score: number;
  maxScore: number;
};

export function ScoreDisplay({ score, maxScore }: ScoreDisplayProps) {
  const pct = maxScore > 0 ? (score / maxScore) * 100 : 0;
  const emoji = pct >= 80 ? "🏆" : pct >= 50 ? "⭐" : "💪";
  return (
    <div className="bd-score-display">
      <div className="bd-score-ring">
        <svg viewBox="0 0 120 120" className="bd-score-svg">
          <circle cx="60" cy="60" r="52" fill="none" stroke={tokens.gray200} strokeWidth="8" />
          <circle
            cx="60"
            cy="60"
            r="52"
            fill="none"
            stroke={tokens.blue500}
            strokeWidth="8"
            strokeLinecap="round"
            strokeDasharray={`${(pct / 100) * 327} 327`}
            transform="rotate(-90 60 60)"
            className="bd-score-arc"
          />
        </svg>
        <div className="bd-score-inner">
          <span className="bd-score-emoji">{emoji}</span>
          <span className="bd-score-number">{score}</span>
        </div>
      </div>
      <div className="bd-score-label">总分 / {maxScore}</div>
    </div>
  );
}

/* ── CitySelector ──────────────────────────── */

type CitySelectorProps = {
  cities: string[];
  selected: string[];
  onToggle: (city: string) => void;
  onSelectAll?: () => void;
  onClearAll?: () => void;
};

export function CitySelector({ cities, selected, onToggle, onSelectAll, onClearAll }: CitySelectorProps) {
  return (
    <div className="bd-city-selector">
      <div className="bd-city-actions">
        {onSelectAll && (
          <Button variant="ghost" size="sm" onClick={onSelectAll}>
            全选 Select All
          </Button>
        )}
        {onClearAll && (
          <Button variant="ghost" size="sm" onClick={onClearAll}>
            清空 Clear
          </Button>
        )}
        <Badge variant="blue">{selected.length} / {cities.length}</Badge>
      </div>
      <div className="bd-city-grid">
        {cities.map((city) => (
          <Chip key={city} selected={selected.includes(city)} onClick={() => onToggle(city)}>
            {city}
          </Chip>
        ))}
      </div>
    </div>
  );
}

/* ── Icons (SVG helpers) ───────────────────── */

export function IconLocation({ size = 20, color = tokens.blue500 }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="2">
      <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z" />
      <circle cx="12" cy="10" r="3" />
    </svg>
  );
}

export function IconPlay({ size = 20, color = tokens.blue500 }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill={color} stroke="none">
      <polygon points="5,3 19,12 5,21" />
    </svg>
  );
}

export function IconTarget({ size = 20, color = tokens.blue500 }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="2">
      <circle cx="12" cy="12" r="10" />
      <circle cx="12" cy="12" r="6" />
      <circle cx="12" cy="12" r="2" />
    </svg>
  );
}

export function IconRefresh({ size = 20, color = tokens.blue500 }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="2">
      <polyline points="23,4 23,10 17,10" />
      <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
    </svg>
  );
}

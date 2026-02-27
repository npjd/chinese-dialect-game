"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import { point } from "@turf/helpers";
import type { Feature, MultiPolygon, Polygon } from "geojson";
import type { CircleMarker, LatLngBounds, LeafletMouseEvent, Layer, Map, Polyline } from "leaflet";
import { feature } from "topojson-client";
import { getPublicStorageUrl, hasSupabaseConfig, indexObjectPath } from "@/lib/supabase";
import {
  AppShell,
  TopBar,
  Card,
  Button,
  Badge,
  ProgressBar,
  ScoreDisplay,
  CitySelector,
  Divider,
  FloatingPanel,
  IconLocation,
  IconTarget,
  IconRefresh,
  tokens,
} from "./components/baidu-design";

type Clip = {
  id: string;
  storage_path: string;
  clip_path?: string;
  target_location?: string;
  city?: string;
  transcript?: string;
  description?: string;
  latitude?: number;
  longitude?: number;
};

type IndexResponse = {
  items?: Clip[];
};

function shuffle<T>(arr: T[]): T[] {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function haversineKm(a: { lat: number; lng: number }, b: { lat: number; lng: number }) {
  const R = 6371;
  const toRad = (v: number) => (v * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

function scoreFromDistance(distanceKm: number) {
  const maxPoints = 5000;
  const scoreDecayKm = 1200;
  return Math.round(maxPoints * Math.exp(-distanceKm / scoreDecayKm));
}

export default function HomePage() {
  const roundsPerGame = 5;
  const maxPossibleScore = roundsPerGame * 5000;
  const indexUrl = useMemo(() => getPublicStorageUrl(indexObjectPath), []);

  const [clips, setClips] = useState<Clip[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [gameState, setGameState] = useState<"setup" | "playing" | "finished">("setup");
  const [selectedCities, setSelectedCities] = useState<string[]>([]);
  const [rounds, setRounds] = useState<Clip[]>([]);
  const [currentRoundIndex, setCurrentRoundIndex] = useState(-1);
  const [totalScore, setTotalScore] = useState(0);
  const [guessLatLng, setGuessLatLng] = useState<{ lat: number; lng: number } | null>(null);
  const [roundResult, setRoundResult] = useState<{ distanceKm: number; roundScore: number } | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const [mapInitError, setMapInitError] = useState("");
  const [clickNotice, setClickNotice] = useState("");
  const [audioCollapsed, setAudioCollapsed] = useState(false);

  const playableClips = useMemo(
    () => clips.filter((clip) => typeof clip.latitude === "number" && typeof clip.longitude === "number"),
    [clips]
  );

  const allCities = useMemo(() => {
    const names = playableClips
      .map((clip) => (clip.city || clip.target_location || "").trim())
      .filter(Boolean);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b, "zh-Hans-CN"));
  }, [playableClips]);

  const filteredPlayableClips = useMemo(() => {
    if (!selectedCities.length) return [];
    const selected = new Set(selectedCities);
    return playableClips.filter((clip) => selected.has((clip.city || clip.target_location || "").trim()));
  }, [playableClips, selectedCities]);

  const currentRound = useMemo(() => {
    if (currentRoundIndex < 0 || currentRoundIndex >= rounds.length) return null;
    return rounds[currentRoundIndex];
  }, [currentRoundIndex, rounds]);

  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const leafletRef = useRef<typeof import("leaflet") | null>(null);
  const mapBoundsRef = useRef<LatLngBounds | null>(null);
  const guessMarkerRef = useRef<CircleMarker | null>(null);
  const answerMarkerRef = useRef<CircleMarker | null>(null);
  const answerLineRef = useRef<Polyline | null>(null);
  const chinaBoundaryRef = useRef<Feature<Polygon | MultiPolygon> | null>(null);
  const currentRoundRef = useRef<Clip | null>(null);
  const hasSubmittedRef = useRef(false);

  currentRoundRef.current = currentRound;
  hasSubmittedRef.current = Boolean(roundResult);

  const clearRoundMapOverlays = useCallback(() => {
    const map = mapRef.current;
    if (!map) return;
    if (guessMarkerRef.current) {
      map.removeLayer(guessMarkerRef.current);
      guessMarkerRef.current = null;
    }
    if (answerMarkerRef.current) {
      map.removeLayer(answerMarkerRef.current);
      answerMarkerRef.current = null;
    }
    if (answerLineRef.current) {
      map.removeLayer(answerLineRef.current);
      answerLineRef.current = null;
    }
  }, []);

  const startRound = useCallback(
    (nextRounds: Clip[], roundIndex: number) => {
      setCurrentRoundIndex(roundIndex);
      setGuessLatLng(null);
      setRoundResult(null);
      setClickNotice("");
      clearRoundMapOverlays();
      if (mapRef.current && mapBoundsRef.current) {
        mapRef.current.fitBounds(mapBoundsRef.current);
      }
      const roundItem = nextRounds[roundIndex];
      if (!roundItem) setGameState("finished");
    },
    [clearRoundMapOverlays]
  );

  const startGame = useCallback(() => {
    const shuffled = shuffle(filteredPlayableClips);
    const nextRounds = shuffled.slice(0, Math.min(roundsPerGame, shuffled.length));
    if (!nextRounds.length) return;
    setRounds(nextRounds);
    setTotalScore(0);
    setGameState("playing");
    startRound(nextRounds, 0);
  }, [filteredPlayableClips, startRound]);

  function nextRound() {
    const nextIndex = currentRoundIndex + 1;
    if (nextIndex >= rounds.length) {
      setGameState("finished");
      return;
    }
    startRound(rounds, nextIndex);
  }

  function playAgain() {
    setGameState("setup");
    setRounds([]);
    setCurrentRoundIndex(-1);
    setTotalScore(0);
    setGuessLatLng(null);
    setRoundResult(null);
    clearRoundMapOverlays();
    if (mapRef.current && mapBoundsRef.current) {
      mapRef.current.fitBounds(mapBoundsRef.current);
    }
  }

  function submitGuess() {
    if (!currentRound || !guessLatLng || roundResult) return;
    if (typeof currentRound.latitude !== "number" || typeof currentRound.longitude !== "number") return;

    const answer = { lat: currentRound.latitude, lng: currentRound.longitude };
    const distanceKm = haversineKm(guessLatLng, answer);
    const roundScore = scoreFromDistance(distanceKm);
    setRoundResult({ distanceKm, roundScore });
    setTotalScore((prev) => prev + roundScore);

    const L = leafletRef.current;
    const map = mapRef.current;
    if (!L || !map) return;

    const answerLatLng = L.latLng(answer.lat, answer.lng);
    answerMarkerRef.current = L.circleMarker(answerLatLng, {
      radius: 8,
      color: tokens.green500,
      fillColor: tokens.green500,
      fillOpacity: 0.9,
      weight: 2,
    }).addTo(map);

    answerLineRef.current = L.polyline([guessLatLng, answerLatLng], {
      color: tokens.gray500,
      weight: 2,
      dashArray: "8 6",
    }).addTo(map);

    const overlays: Layer[] = [guessMarkerRef.current, answerMarkerRef.current, answerLineRef.current].filter(
      (layer): layer is NonNullable<typeof layer> => Boolean(layer)
    );
    if (overlays.length) {
      const group = L.featureGroup(overlays);
      map.fitBounds(group.getBounds().pad(0.3));
    }
  }

  function toggleCity(city: string) {
    setSelectedCities((prev) =>
      prev.includes(city) ? prev.filter((x) => x !== city) : [...prev, city]
    );
  }

  useEffect(() => {
    if (!hasSupabaseConfig) {
      setError("Missing NEXT_PUBLIC_SUPABASE_URL");
      setLoading(false);
      return;
    }

    async function load() {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(indexUrl, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = (await res.json()) as IndexResponse;
        const items = Array.isArray(payload.items) ? payload.items : [];
        setClips(items.filter((x) => x.id && (x.storage_path || x.clip_path)));
      } catch (e) {
        setError(`Failed to load clips index: ${String(e)}`);
      } finally {
        setLoading(false);
      }
    }

    void load();
  }, [indexUrl]);

  useEffect(() => {
    if (gameState === "playing") return;
    if (mapRef.current) {
      mapRef.current.remove();
      mapRef.current = null;
      leafletRef.current = null;
      mapBoundsRef.current = null;
      guessMarkerRef.current = null;
      answerMarkerRef.current = null;
      answerLineRef.current = null;
    }
    setMapReady(false);
    setMapInitError("");
  }, [gameState]);

  useEffect(() => {
    let cancelled = false;
    async function initMap() {
      if (gameState !== "playing") return;
      if (!mapContainerRef.current) return;
      if (mapRef.current) {
        mapRef.current.invalidateSize();
        setMapReady(true);
        return;
      }
      try {
        const L = await import("leaflet");
        if (cancelled || !mapContainerRef.current || mapRef.current) return;
        leafletRef.current = L;
        const bounds = L.latLngBounds(L.latLng(17.5, 73.0), L.latLng(54.0, 135.5));
        mapBoundsRef.current = bounds;

        const map = L.map(mapContainerRef.current, {
          maxBounds: bounds,
          maxBoundsViscosity: 1,
          zoomSnap: 0.25,
          zoomControl: false,
        });
        L.control.zoom({ position: "bottomright" }).addTo(map);
        map.fitBounds(bounds);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          maxZoom: 18,
          attribution: "&copy; OpenStreetMap contributors",
        }).addTo(map);

        const atlasModule = await import("world-atlas/countries-110m.json");
        const atlas = atlasModule.default as {
          objects: { countries: unknown };
        };
        const countries = feature(atlas as never, atlas.objects.countries as never) as {
          features?: Array<Feature<Polygon | MultiPolygon> & { id?: string | number; properties?: Record<string, unknown> }>;
        };
        const chinaFeature =
          countries.features?.find((f) => String(f.id) === "156") ||
          countries.features?.find((f) => String(f.properties?.name || "").toLowerCase() === "china");
        if (!chinaFeature) {
          throw new Error("Could not load China boundary from GeoJSON.");
        }
        chinaBoundaryRef.current = chinaFeature;
        const chinaLayer = L.geoJSON(chinaFeature, {
          style: { color: tokens.blue700, weight: 2, fill: false, opacity: 0.6 },
        }).addTo(map);
        const chinaBounds = chinaLayer.getBounds();
        mapBoundsRef.current = chinaBounds;
        map.fitBounds(chinaBounds.pad(0.05));
        map.setMaxBounds(chinaBounds.pad(0.3));

        map.on("click", (evt: LeafletMouseEvent) => {
          if (!currentRoundRef.current || hasSubmittedRef.current) return;
          const latlng = evt.latlng;
          if (chinaBoundaryRef.current) {
            const isInsideChina = booleanPointInPolygon(point([latlng.lng, latlng.lat]), chinaBoundaryRef.current);
            if (!isInsideChina) {
              setClickNotice("请点击中国境内 Please click inside China.");
              return;
            }
          }
          setClickNotice("");
          setGuessLatLng({ lat: latlng.lat, lng: latlng.lng });
          if (guessMarkerRef.current) {
            guessMarkerRef.current.setLatLng(latlng);
          } else {
            guessMarkerRef.current = L.circleMarker(latlng, {
              radius: 8,
              color: tokens.blue500,
              fillColor: tokens.blue500,
              fillOpacity: 0.9,
              weight: 2,
            }).addTo(map);
          }
        });

        mapRef.current = map;
        setMapReady(true);
      } catch (e) {
        setMapInitError(`Failed to initialize map: ${String(e)}`);
      }
    }
    void initMap();
    return () => {
      cancelled = true;
    };
  }, [gameState]);

  useEffect(() => {
    if (mapRef.current) {
      window.setTimeout(() => mapRef.current?.invalidateSize(), 0);
    }
  }, [gameState, currentRoundIndex]);

  /* ── Render ────────────────────────────────── */

  if (gameState === "playing" && currentRound) {
    return (
      <div className="bd-game-fullscreen">
        {/* Full-screen map background */}
        <div ref={mapContainerRef} className="bd-game-map" />
        {!mapReady && !mapInitError && (
          <div className="bd-map-loading">
            <div className="bd-spinner" />
            <span>地图加载中...</span>
          </div>
        )}
        {mapInitError && <div className="bd-map-error">{mapInitError}</div>}

        {/* ── Top-left: Score ── */}
        <FloatingPanel glass className="bd-hud bd-hud-top-left">
          <div className="bd-hud-score">{totalScore}</div>
          <div className="bd-hud-score-label">总分 Score</div>
        </FloatingPanel>

        {/* ── Top-right: Round progress ── */}
        <FloatingPanel glass className="bd-hud bd-hud-top-right">
          <div className="bd-hud-round">
            {currentRoundIndex + 1}<span className="bd-hud-round-sep">/</span>{rounds.length}
          </div>
          <ProgressBar current={currentRoundIndex + 1} total={rounds.length} />
        </FloatingPanel>

        {/* ── Bottom-left: Audio + hint panel ── */}
        <FloatingPanel glass className={`bd-hud bd-hud-bottom-left ${audioCollapsed ? "bd-hud-collapsed" : ""}`}>
          <button
            className="bd-hud-collapse-btn"
            onClick={() => setAudioCollapsed((v) => !v)}
            type="button"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
              {audioCollapsed
                ? <polyline points="6,9 12,15 18,9" />
                : <polyline points="18,15 12,9 6,15" />
              }
            </svg>
          </button>
          {!audioCollapsed && (
            <>
              <div className="bd-hud-audio-header">
                <svg width="16" height="16" viewBox="0 0 24 24" fill={tokens.blue500} stroke="none">
                  <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3Z" />
                  <path d="M19 10v2a7 7 0 0 1-14 0v-2" fill="none" stroke={tokens.blue500} strokeWidth="2" />
                </svg>
                <span className="bd-hud-audio-label">方言音频</span>
              </div>
              <audio
                controls
                preload="metadata"
                src={getPublicStorageUrl(currentRound.storage_path || currentRound.clip_path || "")}
                className="bd-hud-audio-player"
              />
              {currentRound.transcript && (
                <div className="bd-hud-hint">
                  <span className="bd-hud-hint-tag">提示</span>
                  <span>{currentRound.transcript}</span>
                </div>
              )}
              {roundResult && (
                <div className="bd-hud-result">
                  <div className="bd-hud-result-row">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={tokens.green500} strokeWidth="2">
                      <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z" />
                      <circle cx="12" cy="10" r="3" />
                    </svg>
                    <span>{currentRound.city || currentRound.target_location || "Unknown"}</span>
                  </div>
                  {currentRound.description && (
                    <div className="bd-hud-result-row bd-hud-result-desc">{currentRound.description}</div>
                  )}
                  <div className="bd-hud-result-score">
                    <Badge variant={roundResult.roundScore >= 3000 ? "green" : roundResult.roundScore >= 1000 ? "orange" : "red"}>
                      +{roundResult.roundScore} 分
                    </Badge>
                    <span className="bd-hud-result-dist">{roundResult.distanceKm.toFixed(1)} km</span>
                  </div>
                </div>
              )}
            </>
          )}
        </FloatingPanel>

        {/* ── Bottom-center: Action buttons ── */}
        <div className="bd-hud bd-hud-bottom-center">
          {!roundResult ? (
            <Button
              variant="primary"
              size="lg"
              onClick={submitGuess}
              disabled={!guessLatLng}
              icon={<IconTarget size={18} color="#fff" />}
            >
              提交猜测 Submit
            </Button>
          ) : (
            <Button
              variant="primary"
              size="lg"
              onClick={nextRound}
            >
              {currentRoundIndex === rounds.length - 1 ? "查看结果 Finish" : "下一轮 Next →"}
            </Button>
          )}
        </div>

        {/* ── Click-outside-China notice ── */}
        {clickNotice && (
          <div className="bd-map-notice">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
              <line x1="12" y1="9" x2="12" y2="13" />
              <line x1="12" y1="17" x2="12.01" y2="17" />
            </svg>
            {clickNotice}
          </div>
        )}
      </div>
    );
  }

  return (
    <AppShell>
      <TopBar
        title="方言地图"
        subtitle="Dialect Map Game"
        left={<IconLocation size={22} />}
      />

      {/* ── Loading / Error ── */}
      {(loading || error) && (
        <div className="bd-page-content">
          {loading && (
            <Card variant="default" style={{ textAlign: "center", padding: "40px" }}>
              <div className="bd-spinner" style={{ margin: "0 auto 12px" }} />
              <span style={{ color: tokens.gray500 }}>加载中 Loading clips...</span>
            </Card>
          )}
          {error && (
            <Card variant="outlined" style={{ borderColor: tokens.red400, color: tokens.red500 }}>
              {error}
            </Card>
          )}
        </div>
      )}

      {/* ── Setup Screen ── */}
      {gameState === "setup" && !loading && !error && (
        <div className="bd-page-content">
          <div className="bd-setup-header">
            <div className="bd-setup-title">方言地图</div>
            <div className="bd-setup-subtitle">
              听方言，猜位置 — Listen to dialects, guess the location
            </div>
          </div>

          <Card variant="elevated">
            <div style={{ marginBottom: 12, fontWeight: 600, fontSize: 15 }}>
              选择城市 Select Cities
            </div>
            <CitySelector
              cities={allCities}
              selected={selectedCities}
              onToggle={toggleCity}
              onSelectAll={() => setSelectedCities([...allCities])}
              onClearAll={() => setSelectedCities([])}
            />
            <Divider spacing={16} />
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span style={{ fontSize: 13, color: tokens.gray500 }}>
                已选音频 {filteredPlayableClips.length} 条，需至少 {roundsPerGame} 条
              </span>
              <Button
                variant="primary"
                size="lg"
                disabled={filteredPlayableClips.length < roundsPerGame}
                onClick={startGame}
                icon={<IconTarget size={18} color="#fff" />}
              >
                开始游戏 Start
              </Button>
            </div>
          </Card>
        </div>
      )}

      {/* ── Finished Screen ── */}
      {gameState === "finished" && (
        <div className="bd-page-content">
          <Card variant="elevated" className="bd-finished">
            <div style={{ fontSize: 20, fontWeight: 700, color: tokens.gray800 }}>
              游戏结束 Game Over
            </div>
            <ScoreDisplay score={totalScore} maxScore={maxPossibleScore} />
            <Button
              variant="primary"
              size="lg"
              onClick={playAgain}
              icon={<IconRefresh size={18} color="#fff" />}
            >
              再来一局 Play Again
            </Button>
          </Card>
        </div>
      )}
    </AppShell>
  );
}

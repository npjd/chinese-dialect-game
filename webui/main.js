const MAP_BOUNDS = L.latLngBounds(
  L.latLng(17.5, 73.0), // southwest China-ish
  L.latLng(54.0, 135.5) // northeast China-ish
);

const MAX_POINTS = 5000;
const SCORE_DECAY_KM = 1200;
const DEFAULT_ROUNDS = 10;
const APP_CONFIG = window.DIALECT_GAME_CONFIG || { mode: "local" };

const state = {
  allItems: [],
  rounds: [],
  currentRoundIndex: -1,
  totalScore: 0,
  guessLatLng: null,
  guessMarker: null,
  answerMarker: null,
  answerLine: null,
  map: null,
};

const el = {
  roundLabel: document.getElementById("roundLabel"),
  totalScoreLabel: document.getElementById("totalScoreLabel"),
  distanceLabel: document.getElementById("distanceLabel"),
  roundScoreLabel: document.getElementById("roundScoreLabel"),
  clipAudio: document.getElementById("clipAudio"),
  cityHint: document.getElementById("cityHint"),
  transcriptText: document.getElementById("transcriptText"),
  translationText: document.getElementById("translationText"),
  descriptionText: document.getElementById("descriptionText"),
  submitGuessBtn: document.getElementById("submitGuessBtn"),
  nextRoundBtn: document.getElementById("nextRoundBtn"),
  newGameBtn: document.getElementById("newGameBtn"),
  status: document.getElementById("status"),
};

function setStatus(text, isError = false) {
  el.status.textContent = text;
  el.status.style.color = isError ? "var(--bad)" : "var(--muted)";
}

function initMap() {
  state.map = L.map("map", {
    maxBounds: MAP_BOUNDS,
    maxBoundsViscosity: 1.0,
    zoomSnap: 0.25,
  });
  state.map.fitBounds(MAP_BOUNDS);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(state.map);

  state.map.on("click", (evt) => {
    if (!getCurrentRound()) return;

    state.guessLatLng = evt.latlng;
    if (state.guessMarker) {
      state.guessMarker.setLatLng(evt.latlng);
    } else {
      state.guessMarker = L.circleMarker(evt.latlng, {
        radius: 8,
        color: "#ff6464",
        fillColor: "#ff6464",
        fillOpacity: 0.95,
      }).addTo(state.map);
    }
    el.submitGuessBtn.disabled = false;
    setStatus("Guess selected. Submit when ready.");
  });
}

function getCurrentRound() {
  if (state.currentRoundIndex < 0 || state.currentRoundIndex >= state.rounds.length) {
    return null;
  }
  return state.rounds[state.currentRoundIndex];
}

function haversineKm(a, b) {
  const R = 6371;
  const toRad = (v) => (v * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);

  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

function scoreFromDistance(distanceKm) {
  return Math.round(MAX_POINTS * Math.exp(-distanceKm / SCORE_DECAY_KM));
}

function clearRoundMapOverlays() {
  if (state.guessMarker) {
    state.map.removeLayer(state.guessMarker);
    state.guessMarker = null;
  }
  if (state.answerMarker) {
    state.map.removeLayer(state.answerMarker);
    state.answerMarker = null;
  }
  if (state.answerLine) {
    state.map.removeLayer(state.answerLine);
    state.answerLine = null;
  }
  state.guessLatLng = null;
}

function updateStatsUI() {
  el.roundLabel.textContent = `${Math.max(0, state.currentRoundIndex + 1)} / ${state.rounds.length}`;
  el.totalScoreLabel.textContent = String(state.totalScore);
}

function updateClipUI(roundItem) {
  const clipSrc = resolveClipUrl(roundItem);
  el.clipAudio.src = clipSrc;
  el.cityHint.textContent = roundItem.city || roundItem.target_location || "Unknown";
  el.transcriptText.textContent = roundItem.transcript || "-";
  el.translationText.textContent = roundItem.translation_en || "-";
  el.descriptionText.textContent = roundItem.description || "-";
}

function normalizeSupabaseUrl(url) {
  return String(url || "").replace(/\/+$/, "");
}

function buildSupabasePublicObjectUrl(objectPath) {
  const base = normalizeSupabaseUrl(APP_CONFIG.supabaseUrl);
  const bucket = String(APP_CONFIG.bucket || "").trim();
  if (!base || !bucket) {
    throw new Error("Supabase mode requires supabaseUrl and bucket in webui/config.js");
  }
  const encodedPath = String(objectPath || "")
    .split("/")
    .map((p) => encodeURIComponent(p))
    .join("/");
  return `${base}/storage/v1/object/public/${encodeURIComponent(bucket)}/${encodedPath}`;
}

function resolveClipUrl(roundItem) {
  if (APP_CONFIG.mode === "supabase_public") {
    const objectPath = roundItem.storage_path || roundItem.clip_path;
    return buildSupabasePublicObjectUrl(objectPath);
  }
  return `/${roundItem.clip_path}`;
}

function startRound(roundIndex) {
  state.currentRoundIndex = roundIndex;
  clearRoundMapOverlays();
  state.map.fitBounds(MAP_BOUNDS);

  const roundItem = getCurrentRound();
  if (!roundItem) {
    setStatus(`Game finished. Final score: ${state.totalScore}`);
    el.submitGuessBtn.disabled = true;
    el.nextRoundBtn.disabled = true;
    el.clipAudio.removeAttribute("src");
    el.clipAudio.load();
    updateStatsUI();
    return;
  }

  updateClipUI(roundItem);
  updateStatsUI();

  el.distanceLabel.textContent = "-";
  el.roundScoreLabel.textContent = "-";
  el.submitGuessBtn.disabled = true;
  el.nextRoundBtn.disabled = true;

  setStatus("Listen, click the map, then submit your guess.");
}

function submitGuess() {
  const roundItem = getCurrentRound();
  if (!roundItem || !state.guessLatLng) return;

  const answer = L.latLng(roundItem.latitude, roundItem.longitude);
  const distanceKm = haversineKm(state.guessLatLng, answer);
  const roundScore = scoreFromDistance(distanceKm);
  state.totalScore += roundScore;

  state.answerMarker = L.circleMarker(answer, {
    radius: 8,
    color: "#55d187",
    fillColor: "#55d187",
    fillOpacity: 0.95,
  }).addTo(state.map);

  state.answerLine = L.polyline([state.guessLatLng, answer], {
    color: "#c5d2ff",
    weight: 2,
    opacity: 0.85,
    dashArray: "6 4",
  }).addTo(state.map);

  const group = L.featureGroup([state.guessMarker, state.answerMarker, state.answerLine]);
  state.map.fitBounds(group.getBounds().pad(0.3));

  el.distanceLabel.textContent = `${distanceKm.toFixed(1)} km`;
  el.roundScoreLabel.textContent = `${roundScore}`;
  updateStatsUI();

  const finalRound = state.currentRoundIndex === state.rounds.length - 1;
  if (finalRound) {
    setStatus(`Round complete. Game finished! Final score: ${state.totalScore}`);
  } else {
    setStatus("Round complete. Continue to the next clip.");
  }

  el.submitGuessBtn.disabled = true;
  el.nextRoundBtn.disabled = finalRound;
}

function shuffle(arr) {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function startNewGame() {
  state.totalScore = 0;
  const shuffled = shuffle(state.allItems);
  const roundsCount = Math.min(DEFAULT_ROUNDS, shuffled.length);
  state.rounds = shuffled.slice(0, roundsCount);
  startRound(0);
}

async function loadData() {
  let dataUrl = "./data/game_data.json";
  if (APP_CONFIG.mode === "supabase_public") {
    const indexPath = APP_CONFIG.indexObjectPath || "indexes/all_clips.json";
    dataUrl = buildSupabasePublicObjectUrl(indexPath);
  }

  const response = await fetch(dataUrl);
  if (!response.ok) {
    throw new Error(`Could not load game data (${response.status})`);
  }
  const data = await response.json();
  if (!data.items || !Array.isArray(data.items) || data.items.length === 0) {
    throw new Error("Game data has no playable clips.");
  }
  state.allItems = data.items;
}

function bindEvents() {
  el.submitGuessBtn.addEventListener("click", submitGuess);
  el.nextRoundBtn.addEventListener("click", () => startRound(state.currentRoundIndex + 1));
  el.newGameBtn.addEventListener("click", startNewGame);
}

async function bootstrap() {
  initMap();
  bindEvents();

  try {
    await loadData();
    startNewGame();
    setStatus(`Loaded ${state.allItems.length} clips. Ready to play.`);
  } catch (err) {
    setStatus(String(err?.message || err), true);
  }
}

bootstrap();

# Chinese Dialect Geo Guess (Local)

This local UI turns your generated clip files into a geo-guess game:

1. Listen to a dialect clip.
2. Click on a map where you think it comes from.
3. Submit guess and get score by distance.

## 1) Build game data (coordinates + clip index)

From repo root:

```bash
python3 webui/scripts/build_game_data.py
```

This script:

- scans `bili_gemini_test_out/clips_by_bvid/**/*.metadata.json`
- geocodes `target_location` (cached in `webui/data/location_cache.json`)
- writes playable clips to `webui/data/game_data.json`

Notes:

- First run needs internet (for geocoding via OpenStreetMap Nominatim).
- Later runs reuse cache and are much faster.

### Fast first-run (recommended)

If you want to start playing quickly:

```bash
python3 webui/scripts/build_game_data.py --max-clips 80 --max-new-locations 25 --pause 0.1
```

This builds a smaller playable set quickly. You can later run full build.

## 2) Run local web server

From repo root:

```bash
python3 -m http.server 8000
```

Then open:

- `http://localhost:8000/webui/`

## 3) Run from Supabase Storage only (no DB)

1) Upload clips + index:

```bash
python3 scripts/upload_storage_only_supabase.py --bucket dialect-game --public-bucket
```

2) Edit `webui/config.js`:

```js
window.DIALECT_GAME_CONFIG = {
  mode: "supabase_public",
  supabaseUrl: "https://YOUR_PROJECT_REF.supabase.co",
  bucket: "dialect-game",
  indexObjectPath: "indexes/all_clips.json",
};
```

3) Start local web server and open app:

```bash
python3 -m http.server 8001
```

- Open `http://localhost:8001/webui/`

## Scoring

- Distance is computed with Haversine formula in km.
- Score per round:

```text
round_score = round(5000 * exp(-distance_km / 1200))
```

10 rounds are sampled randomly each game.

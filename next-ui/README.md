# Next.js Reviewer UI

Simple reviewer UI to:

- load clip index from Supabase Storage (`indexes/all_clips.json`)
- play map-based geo-guess rounds (10 clips per game)
- play clip audio
- mark low-quality examples
- copy bad-example IDs

## Setup

```bash
cd next-ui
cp .env.example .env.local
npm install
npm run dev
```

Open `http://localhost:3000`.

## Env vars

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_BUCKET` (default `dialect-game`)
- `NEXT_PUBLIC_SUPABASE_INDEX_OBJECT_PATH` (default `indexes/all_clips.json`)

The app uses public Storage URLs, so anon key is not required.
